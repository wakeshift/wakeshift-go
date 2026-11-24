package wakeshift

// Requests a vendor's tool call waits on (a subscribe, an unsubscribe,
// ResolveSession, the result's addition, a rollback): each has a short
// budget, is never retried, and goes through the client's circuit breaker,
// so a slow or failing Wakeshift adds seconds to a tool call, never a
// minute, and after a run of failures adds nothing.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// CallTimeouts are the budgets of the requests a tool call waits on
// (WithCallTimeouts). A zero field is its default.
type CallTimeouts struct {
	// Subscribe bounds Subscribe, Unsubscribe and ResolveSession in a tool
	// call (default 5 seconds).
	Subscribe time.Duration
	// Block bounds the addition to the tool's result, and each rollback
	// of a failed call's subscriptions (default 1 second).
	Block time.Duration
}

// defaultCallTimeouts are CallTimeouts' defaults.
var defaultCallTimeouts = CallTimeouts{Subscribe: 5 * time.Second, Block: time.Second}

// breakerThreshold is how many in-call requests in a row fail before the
// breaker opens; breakerCooldown is how long it stays open. Tests change
// them.
var (
	breakerThreshold = 5
	breakerCooldown  = 30 * time.Second
)

// breaker counts in-call requests that failed in a row. Once open it skips
// them until the cool-down passes, then lets one through: the probe.
type breaker struct {
	mu        sync.Mutex
	failures  int
	openUntil time.Time // zero: closed
	probing   bool
}

// admit reports whether an in-call request may go now, and whether it's
// the probe.
func (b *breaker) admit(now time.Time) (ok, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.openUntil.IsZero() {
		return true, false
	}
	if now.Before(b.openUntil) || b.probing {
		return false, false
	}
	b.probing = true
	return true, true
}

// outcome is what an in-call request tells the breaker.
type outcome int

const (
	outcomeNone    outcome = iota // the caller canceled it
	outcomeSuccess                // Wakeshift answered
	outcomeFailure                // unreachable, a timeout or a 5xx
)

// record notes a request's outcome.
func (b *breaker) record(now time.Time, o outcome, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
	}
	// Once open, only the probe decides: a request admitted before it
	// opened and answered after neither closes it nor extends the cool-down.
	if !probe && !b.openUntil.IsZero() {
		return
	}
	switch o {
	case outcomeSuccess:
		b.failures, b.openUntil = 0, time.Time{}
	case outcomeFailure:
		b.failures++
		if probe || b.failures >= breakerThreshold {
			b.openUntil = now.Add(breakerCooldown)
		}
	}
}

// outcomeOf classifies an in-call request's error; ctx is the caller's.
func outcomeOf(ctx context.Context, err error) outcome {
	if err == nil {
		return outcomeSuccess
	}
	var ae *APIError
	if errors.As(err, &ae) {
		if ae.Status == 0 && (ae.Code == CodeUnreachable || ae.Code == CodeTimeout) || ae.Status >= 500 {
			return outcomeFailure
		}
		return outcomeSuccess
	}
	if ctx.Err() != nil {
		return outcomeNone
	}
	return outcomeSuccess // answered, but not as expected, or no request
}

// noRetryKey marks a request's context: do sends it once.
type noRetryKey struct{}

func noRetries(ctx context.Context) bool { return ctx.Value(noRetryKey{}) != nil }

// sharedContext is a tool call's context for work the call shares with the
// client (a manifest send): its values without the no-retry marker.
type sharedContext struct{ context.Context }

func (s sharedContext) Value(k any) any {
	if _, ok := k.(noRetryKey); ok {
		return nil
	}
	return s.Context.Value(k)
}

// subscribeBudget and blockBudget are WithCallTimeouts', or the defaults.
func (c *Client) subscribeBudget() time.Duration {
	return cmp.Or(c.calls.Subscribe, defaultCallTimeouts.Subscribe)
}

func (c *Client) blockBudget() time.Duration { return cmp.Or(c.calls.Block, defaultCallTimeouts.Block) }

// inCall is eventsCall for a request a tool call waits on, within budget,
// through the breaker.
func (c *Client) inCall(ctx context.Context, budget time.Duration, method, path string, body, out any) error {
	return c.inCallBy(ctx, time.Now().Add(budget), func(ctx context.Context) error {
		return c.eventsCall(ctx, method, path, body, out)
	})
}

// inCallBy runs f, one step a tool call waits on, through the breaker: a
// total deadline from ctx (any manifest send f waits on included), no
// retries, and our deadline passing is a timeout, never the caller's own
// context error.
func (c *Client) inCallBy(ctx context.Context, deadline time.Time, f func(context.Context) error) error {
	ok, probe := c.breaker.admit(c.now())
	if !ok {
		c.logLimited(ctx, "core_unavailable", fmt.Sprintf("Wakeshift failed %d times in a row inside tool calls, so tool calls skip it for %s",
			breakerThreshold, seconds(breakerCooldown)))
		return &APIError{Code: CodeUnreachable,
			Message: fmt.Sprintf("can't reach Wakeshift at %s (skipped: %d requests in a row failed inside tool calls, so they pause for %s). %s %s",
				c.core, breakerThreshold, seconds(breakerCooldown), fixes[CodeUnreachable], anchor(CodeUnreachable))}
	}
	sctx, cancel := context.WithDeadline(context.WithValue(ctx, noRetryKey{}, true), deadline)
	defer cancel()
	err := f(sctx)
	if err != nil && ctx.Err() == nil && sctx.Err() != nil && !answered(err) {
		err = c.timedOut(errors.New("no answer within the tool call's budget"))
	}
	c.breaker.record(c.now(), outcomeOf(ctx, err), probe)
	return err
}

// vouchersOfferedBy is vouchersOffered for a tool call: a manifest send it
// needs first is an in-call step, by deadline. A failed or skipped one is
// no.
func (c *Client) vouchersOfferedBy(ctx context.Context, deadline time.Time) bool {
	c.mu.Lock()
	done := !c.attached || c.sent && c.sentGen == c.gen
	c.mu.Unlock()
	if !done && c.inCallBy(ctx, deadline, c.ensureManifest) != nil {
		return false
	}
	return c.vouchersKnown()
}

// answered reports whether err is Wakeshift's answer.
func answered(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status != 0
}

// sendLock is a mutex whose lock gives up when ctx ends, so a tool call
// waiting on another's manifest send stays within its budget.
type sendLock struct {
	once sync.Once
	ch   chan struct{}
}

func (l *sendLock) lock(ctx context.Context) error {
	l.once.Do(func() { l.ch = make(chan struct{}, 1) })
	select {
	case l.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wakeshift: the request to Wakeshift ended: %w", ctx.Err())
	}
}

func (l *sendLock) unlock() { <-l.ch }

// seconds is d for a message: "30 seconds".
func seconds(d time.Duration) string {
	if d == time.Second {
		return "1 second"
	}
	if d%time.Second == 0 {
		return fmt.Sprintf("%d seconds", int(d/time.Second))
	}
	return d.String()
}
