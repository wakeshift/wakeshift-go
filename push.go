package wakeshift

// MCP Events push (SEP-3415 events/stream), served at the HTTP layer
// (Handler) as a relay. The client holds an
// events/stream POST open on the vendor's server; the SDK opens one
// upstream stream to Wakeshift for it (POST /api/apps/v1/events/stream,
// NDJSON) and writes each frame to the POST's response as an SSE message,
// stamped with the request's ID. Wakeshift keeps the subscription, its
// queue, cursors, heartbeats, consent and limits; the SDK keeps nothing that
// has to outlive the request:
//
//   - the client dropping the POST (r.Context()) ends the upstream, and
//     gets no result: over HTTP an abort can't be told from a dropped
//     connection, so the subscription lapses on its own;
//   - Wakeshift ending the stream (terminated, close) ends the client's,
//     with the final result, and so does an upstream silent for three
//     heartbeat intervals, Push.MaxStream passing, or the client's Close:
//     the client reconnects with its cursor;
//   - the upstream has no client-wide timeout (only one until its
//     response's headers arrive), no retries and no breaker: a stream that
//     fails to open answers the client with an error, and the client
//     retries;
//   - frames, cursors, arguments and vouchers are never logged.
//
// Go serves push only over HTTP: on stdio Go vendors keep poll.

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wakeshift/wakeshift-go/internal/mcpevents/event"
)

// PushOptions are AttachOptions.Push: MCP Events push (events/stream),
// served where Wakeshift offers it. The zero value serves it, with
// Wakeshift's heartbeat and no limit on a stream's length (on Vercel, AWS
// Lambda and Cloud Run, a limit under the host's request cap).
type PushOptions struct {
	// Off serves no push: events/list doesn't list it and events/stream
	// answers Unsupported. PushOff is the value that sets it.
	Off bool
	// MaxStream ends each stream gracefully after this long, under the
	// host's request duration cap: the client gets the final result and
	// reconnects with its cursor. Zero: none, except on a host the SDK
	// recognizes by its environment (VERCEL 280 seconds,
	// AWS_LAMBDA_FUNCTION_NAME 14 minutes, K_SERVICE 55 minutes).
	MaxStream time.Duration
	// MaxHeartbeat asks Wakeshift for heartbeats at least this often, under
	// a load balancer's idle timeout (Wakeshift grants between 10 and 45
	// seconds). Zero: the client's suggestion, else Wakeshift's 30 seconds.
	MaxHeartbeat time.Duration
	// Reauthorize is how often every open stream asks again whether its
	// subscriber may still have it: Identify runs again (a stream that had
	// a user at open ends when it now gives no user or another one), then
	// the event's Authorize, if it has one. A refusal ends the stream as a
	// revocation does (the client gets terminated, Forbidden, and stops).
	// 0 means the default, 5 minutes; negative values panic. It's kept
	// between 1 and 50 minutes. Authorize or Identify failing (an error, a
	// panic, a run over 10 seconds) keeps the stream, until its last
	// successful check is 15 minutes old (or three intervals, if longer):
	// then it ends with the final result, and the client reconnects with
	// its cursor.
	Reauthorize time.Duration
}

// The Reauthorize interval: its default, and the bounds it's kept in.
const (
	defaultReauthorize = 5 * time.Minute
	minReauthorize     = time.Minute
	maxReauthorize     = 50 * time.Minute
)

// maxUnchecked is how long an open stream goes on while Identify or
// Authorize fail (an error, not a refusal) at its rechecks: once its last
// successful check (or its open) is this old, a failed recheck ends it with
// the final result, and the client reconnects with its cursor (retrying
// while the checks fail: checkFailed). 15 minutes: three failed rechecks at the
// default interval.
const maxUnchecked = 15 * time.Minute

// maxUncheckedFor is the bound for a stream rechecked every interval: at
// least maxUnchecked, and at least three intervals, so a longer interval
// still allows three failed rechecks. Over about 17 minutes the core's
// 50-minute stream age ends the stream first.
func maxUncheckedFor(interval time.Duration) time.Duration {
	return max(maxUnchecked, 3*interval)
}

// PushOff turns push off: AttachOptions{Push: wakeshift.PushOff}, for a
// host that can't hold a response open.
var PushOff = PushOptions{Off: true}

// pushFeature is the core's feature: it serves the push upstream
// (WAKESHIFT_MCP_EVENTS_PUSH).
const pushFeature = "push"

// pushDelivery is the delivery mode events/list adds, last, where push is
// served.
const pushDelivery = "push"

// subscriptionIDMeta is the _meta key every push notification carries: the
// events/stream request's JSON-RPC ID, as sent.
const subscriptionIDMeta = "io.modelcontextprotocol/subscriptionId"

// pushClient reports whether push is what wakes a client in its open
// conversation (opencode), by the client table: its first matching
// entry while push isn't served is a without_push one
// (event.WokenByPush). Where push is served, it's decided by what it
// declares, and its call.subscribe sends events_ext.
func pushClient(client, userAgent string, meta map[string]any) bool {
	return event.WokenByPush(client, userAgent, meta)
}

// hostMaxStream is how long a stream lasts by default on a host the SDK
// recognizes by its environment: under its request duration cap.
var hostMaxStream = []struct {
	env  string
	d    time.Duration
	host string
}{
	{"VERCEL", 280 * time.Second, "Vercel"},
	{"AWS_LAMBDA_FUNCTION_NAME", 14 * time.Minute, "AWS Lambda"},
	{"K_SERVICE", 55 * time.Minute, "Cloud Run"},
}

// pushConfig is opts with MaxStream defaulted by the host it runs on
// (getenv), and the host it found ("" for none).
func pushConfig(opts PushOptions, getenv func(string) string) (PushOptions, string) {
	switch {
	case opts.Reauthorize == 0:
		opts.Reauthorize = defaultReauthorize
	case opts.Reauthorize > 0:
		opts.Reauthorize = min(max(opts.Reauthorize, minReauthorize), maxReauthorize)
	}
	if opts.Off || opts.MaxStream > 0 {
		return opts, ""
	}
	for _, h := range hostMaxStream {
		if getenv(h.env) != "" {
			opts.MaxStream = h.d
			return opts, h.host
		}
	}
	return opts, ""
}

// pushOptionsOf is the attachment's push settings, logging the host
// default it took (push_max_stream, debug).
func (c *Client) pushOptionsOf(opts PushOptions) PushOptions {
	for name, d := range map[string]time.Duration{"MaxStream": opts.MaxStream, "MaxHeartbeat": opts.MaxHeartbeat, "Reauthorize": opts.Reauthorize} {
		if d < 0 {
			c.configPanic("wakeshift: AttachOptions.Push." + name + " is a duration over 0, or 0 for the default")
		}
	}
	out, host := pushConfig(opts, os.Getenv)
	if host != "" {
		c.logOnce(context.Background(), "push_max_stream", host,
			fmt.Sprintf("this server runs on %s, so each events/stream ends after %s and its client reconnects", host, out.MaxStream))
	}
	return out
}

// pushOffered reports whether the core serves push: its manifest answer
// reports push. It sends the manifest first if it hasn't been; a failed
// send is no.
func (c *Client) pushOffered(ctx context.Context) bool {
	if c.ensureManifest(ctx) != nil {
		return false
	}
	return c.pushKnown()
}

// pushKnown is whether the core's last manifest answer reported push,
// without asking: a call's events_ext goes only to a core that reported it
// (an older one refuses fields it doesn't know).
func (c *Client) pushKnown() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.features {
		if f == pushFeature {
			return true
		}
	}
	return false
}

// pushServed reports whether events/list lists push (and events/stream
// serves it): attach's push isn't off, Wakeshift serves this server's
// events, and the core reports push. Go always streams over HTTP.
func (set *settings) pushServed(ctx context.Context) bool {
	return set.pushOn() && set.app.pushOffered(ctx)
}

// pushOn is pushServed without the core's part: attach's push isn't off
// and Wakeshift serves this server's events.
func (set *settings) pushOn() bool {
	return !set.push.Off && !set.eventsKept.Load() && len(set.app.events()) > 0
}

// servesPushFor is the "serves push" input of nativeClientRule for a
// request: asked only for a client push wakes, the others' answer
// being the same either way.
func (set *settings) servesPushFor(ctx context.Context, client, userAgent string, meta map[string]any) bool {
	return pushClient(client, userAgent, meta) && set.pushServed(ctx)
}

// pushStreamResult is the final result of an events/stream: Wakeshift (or
// the SDK) ended it, and the client reconnects with its cursor (or, after
// terminated, stops).
// It carries the request's ID, as sent, as its subscription ID, as every
// notification does.
func pushStreamResult(id json.RawMessage) map[string]any {
	return map[string]any{"resultType": event.ResultType, "_meta": map[string]any{subscriptionIDMeta: id}}
}

// defaultHeartbeat is the grant until the first active frame says
// Wakeshift's: the spec's default.
const defaultHeartbeat = 30 * time.Second

// streamOpenTimeout bounds the wait for the upstream's response headers;
// its body is read for as long as Wakeshift keeps it open.
var streamOpenTimeout = 20 * time.Second

// maxFrame is the longest NDJSON line read from the upstream: an
// occurrence is at most 256 KiB.
const maxFrame = 1 << 20

// streamClient is the client the upstream is read with: the app's
// transport (WithHTTPClient) or the default one, never a client-wide
// Timeout, which would cut the stream off.
func (c *Client) streamClient() *http.Client {
	if c.http != nil {
		return &http.Client{Transport: c.http.Transport, CheckRedirect: c.http.CheckRedirect, Jar: c.http.Jar}
	}
	return http.DefaultClient
}

// errStreamOpenTimeout is the upstream not answering in streamOpenTimeout.
var errStreamOpenTimeout = errors.New("wakeshift: Wakeshift didn't answer the stream in time")

// openStream sends the upstream's request: one attempt, a timeout only
// until the response's headers arrive. ctx ends the stream.
func (c *Client) openStream(ctx context.Context, body any) (*http.Response, error) {
	if err := c.ensureManifest(ctx); err != nil {
		return nil, err
	}
	tok, err := c.assertion()
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	timer := time.AfterFunc(streamOpenTimeout, func() { cancel(errStreamOpenTimeout) })
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.core+"/api/apps/v1/events/stream", bytes.NewReader(b))
	if err != nil {
		timer.Stop()
		cancel(nil)
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/x-ndjson")
	resp, err := c.streamClient().Do(req)
	if !timer.Stop() {
		if resp != nil {
			resp.Body.Close()
		}
		if err == nil {
			err = errStreamOpenTimeout
		}
	}
	if err != nil {
		cancel(nil)
		return nil, err
	}
	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: func() { cancel(nil) }}
	return resp, nil
}

// cancelBody ends its request's context once closed.
type cancelBody struct {
	io.ReadCloser
	cancel func()
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// maxRetryAfter caps the wait a 503's Retry-After asks for; pushJitter is
// the most random wait added to it.
var (
	maxRetryAfter = 10 * time.Second
	pushJitter    = time.Second
)

// retryAfter is a Retry-After header's delay in seconds, at most
// maxRetryAfter; 0 when it's missing or not a whole number of seconds.
func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || n <= 0 {
		return 0
	}
	return min(time.Duration(n)*time.Second, maxRetryAfter)
}

// pushCheckFailed is what an events/stream gets when Identify or Authorize
// failed (an error, a panic, a run over 10 seconds) as it opened, which
// reconnects do: a retryable error (Internal error, as when Wakeshift can't
// be reached), never Forbidden, which tells the client to stop. The client
// retries, and the stream opens once the app's check answers. A refusal is
// still Forbidden.
const pushCheckFailed = "this app couldn't check access to this event just now: try again shortly"

// checkFailed answers an events/stream whose Identify or Authorize failed
// as it opened (pushCheckFailed), logged as push_open_check_failed with the
// event's name only.
func (c *Client) checkFailed(ctx context.Context, w http.ResponseWriter, id json.RawMessage, event string) {
	c.logLimited(ctx, "push_open_check_failed", fmt.Sprintf("identify or authorize failed when an events/stream of %s opened, so the client was answered with a retryable error", event),
		slog.String("event", event))
	rpcError(w, id, rpcInternal, pushCheckFailed, nil)
}

// pushUpstreamFailed is the message for a refusal whose body Wakeshift
// didn't write (no JSON error message): the body itself is never passed on.
const pushUpstreamFailed = "push upstream failed"

// pushRun is one events/stream, every check passed: what the relay needs.
type pushRun struct {
	id      json.RawMessage
	t       *eventType
	call    wireCall
	args    map[string]any
	voucher string
	p       eventParams
	codes   event.Codes
	// req is the request as Identify and Authorize see it, asked again
	// every Reauthorize.
	req Request
}

// reauthorizeAfter is the Reauthorize timer (tests replace it).
var reauthorizeAfter = func(d time.Duration) <-chan time.Time { return time.After(d) }

// recheck is what reauthorize found.
type recheck int

const (
	recheckAllowed recheck = iota
	recheckRefused
	recheckFailed // Identify or Authorize failed: neither allowed nor refused
)

// reauthorize asks again whether the stream's subscriber may still have
// it: Identify, run fresh, for a stream that had a user at open (no user,
// another user, or RequireUser refusing ends it; a voucher's stream
// without a user skips it), then the event's Authorize. Identify or
// Authorize failing keeps the stream (logged, the error's type only),
// until maxUncheckedFor: only a refusal ends it at once.
func (set *settings) reauthorize(ctx context.Context, run pushRun) recheck {
	c := set.app
	user := run.call.User
	if user != "" && set.identify != nil {
		id, err := withLimit(ctx, identifyTimeout, func(ctx context.Context) (Identity, error) { return set.identify(ctx, run.req) })
		if err != nil {
			c.logLimited(ctx, "identify_error", fmt.Sprintf("identify failed (%s) while an events/stream was checked again, so the stream was kept", errorType(err)),
				slog.String("error_type", errorType(err)))
			return recheckFailed
		}
		if now := (resolved{id: id}).user(); now != user {
			return recheckRefused // no user now (RequireUser refuses that too), or another
		}
	}
	if run.t.authorize == nil {
		return recheckAllowed
	}
	args, _ := json.Marshal(run.args)
	sub := Subscriber{User: user, Client: run.call.Client, Native: true, Request: run.req}
	ok, err := withTimeLimit(ctx, func(ctx context.Context) (bool, error) { return run.t.authorize(ctx, sub, args) })
	if err != nil {
		c.logLimited(ctx, "authorize_failed", fmt.Sprintf("authorize for %s failed (%s) while an events/stream was checked again, so the stream was kept", run.t.name, errorType(err)),
			slog.String("event", run.t.name), slog.String("error_type", errorType(err)))
		return recheckFailed
	}
	if !ok {
		return recheckRefused
	}
	return recheckAllowed
}

// serveStream answers one events/stream over HTTP, after its Mcp-Name
// check: refused as events/poll refuses, and with Unsupported, value
// "push", where push isn't served; otherwise relayed from Wakeshift's
// upstream.
func (set *settings) serveStream(w http.ResponseWriter, r *http.Request, post *postState, id json.RawMessage, p eventParams, base wireCall, codes event.Codes) {
	a := set.app
	// Not served: Unsupported for any event, known or not.
	if !set.pushServed(r.Context()) {
		rpcError(w, id, codes.Unsupported, streamRefusal, map[string]string{"feature": "deliveryMode", "value": "push"})
		return
	}
	t := a.eventByName(p.Name)
	if t == nil {
		nameJSON, _ := json.Marshal(p.Name)
		rpcError(w, id, codes.NotFound, fill("native_unknown_event", "{event}", string(nameJSON)), map[string]string{"kind": "event"})
		return
	}
	// The voucher leaves the arguments before anything reads them. A
	// tools-only event streams only with the voucher one of the app's
	// tools handed out, while the core hands them out; an open event
	// ignores a voucher.
	var offered string
	p.Arguments, offered = takeVoucher(p.Arguments)
	voucher := ""
	if !t.open() && offered != "" && a.vouchersOffered(r.Context()) {
		voucher = offered
	}
	if !t.open() && voucher == "" {
		rpcError(w, id, codes.Forbidden, fill("native_tools_only", "{event}", t.name), nil)
		return
	}
	for _, v := range []*int64{p.MaxAgeMs, p.HeartbeatMs} {
		if v != nil && *v < 0 {
			rpcError(w, id, event.InvalidParams, "maxAgeMs and heartbeatMs are whole numbers of milliseconds, or null", nil)
			return
		}
	}
	p.Arguments = t.known(p.Arguments)
	ident := post.identity(Request{ClientName: base.Client, Meta: p.Meta})
	ident.logRequired(r.Context(), a, "")
	// Identify failing (an error, not a refusal) is retryable, never
	// Forbidden: the client reconnects once the app's check answers (#1212).
	if ident.refused != "" && !ident.anonymous {
		a.checkFailed(r.Context(), w, id, t.name)
		return
	}
	if ident.refused != "" {
		rpcError(w, id, codes.Forbidden, eventTools["native_no_user"], nil)
		return
	}
	call := ident.id.apply(base)
	if call.User == "" && voucher == "" {
		rpcError(w, id, codes.Forbidden, eventTools["native_no_user"], nil)
		return
	}
	// Each stream (each reconnect too) asks Authorize again; its failing is
	// retryable, as Identify's is.
	if t.authorize != nil {
		args, _ := json.Marshal(p.Arguments)
		sub := Subscriber{User: call.User, Client: call.Client, Native: true,
			Request: Request{Token: post.req.Token, Header: post.req.Header, ClientName: base.Client, Meta: p.Meta}}
		ok, err := withTimeLimit(post.ctx, func(ctx context.Context) (bool, error) { return t.authorize(ctx, sub, args) })
		if err != nil {
			a.logLimited(post.ctx, "authorize_failed", fmt.Sprintf("authorize for %s failed (%s) as an events/stream opened, so the client was answered with a retryable error", t.name, errorType(err)),
				slog.String("event", t.name), slog.String("error_type", errorType(err)))
			a.checkFailed(r.Context(), w, id, t.name)
			return
		}
		if !ok {
			rpcError(w, id, codes.Forbidden, fill("native_refused", "{event}", t.name), nil)
			return
		}
	}
	if voucher != "" {
		call.TransportSession = transportSession(r.Header.Get("Mcp-Session-Id"))
	}
	set.servePush(w, r, pushRun{id: id, t: t, call: call, args: p.Arguments, voucher: voucher, p: p, codes: codes,
		req: Request{Token: post.req.Token, Header: post.req.Header, ClientName: base.Client, Meta: p.Meta}})
}

// servePush relays one events/stream: opens the upstream and writes its
// frames to w until either end closes. A refusal before the stream opens
// is a JSON-RPC error, as events/poll answers one.
func (set *settings) servePush(w http.ResponseWriter, r *http.Request, run pushRun) {
	c := set.app
	ctx, stop := context.WithCancel(r.Context())
	defer stop()
	// The heartbeat asked for: the client's suggestion, at most the app's MaxHeartbeat.
	var asked *int64
	if run.p.HeartbeatMs != nil {
		v := *run.p.HeartbeatMs
		asked = &v
	}
	if m := set.push.MaxHeartbeat.Milliseconds(); m > 0 && (asked == nil || *asked > m) {
		asked = &m
	}
	body := map[string]any{"call": run.call, "name": run.t.name, "arguments": run.args, "cursor": run.p.Cursor,
		"max_age_ms": run.p.MaxAgeMs, "heartbeat_ms": asked}
	if run.voucher != "" {
		body["voucher"] = run.voucher
	}
	resp, err := c.openStream(ctx, body)
	if err != nil {
		if r.Context().Err() != nil {
			return // the client went away: nothing to answer
		}
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status != 0 {
			// The manifest's send was refused.
			code, msg, data := rpcErrorOf(err, run.codes, run.t.name, "push")
			rpcError(w, run.id, code, msg, data)
			return
		}
		c.logLimited(r.Context(), "push_upstream_failed", "an events/stream couldn't reach Wakeshift, so the client was answered with an error",
			slog.String("reason", "unreachable"))
		rpcError(w, run.id, rpcInternal, "Wakeshift couldn't be reached", nil)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		// The core is at its stream limit: the client is answered with a
		// retryable error (never a result with no active before it, which
		// SEP-3415 doesn't allow) after Retry-After (at most 10 s) and up to
		// a second of jitter, so clients that retry at once don't come back
		// together; a client that leaves meanwhile gets nothing.
		wait := retryAfter(resp.Header.Get("Retry-After")) + time.Duration(mrand.Int64N(int64(pushJitter)+1))
		resp.Body.Close()
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-t.C:
			rpcError(w, run.id, rpcInternal, streamBusy, nil)
		case <-r.Context().Done():
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		code, msg, data := pushRefusal(resp.StatusCode, raw, run.codes, run.t.name)
		if code == rpcInternal {
			c.logLimited(r.Context(), "push_upstream_failed", fmt.Sprintf("Wakeshift answered an events/stream with %d, so the client was answered with an error", resp.StatusCode),
				slog.String("reason", "status"))
		}
		rpcError(w, run.id, code, msg, data)
		return
	}
	set.relay(ctx, w, r, run, resp.Body, asked)
}

// streamBusy is events/stream's answer when the core is at its stream
// limit (a 503): -32603, in every dialect, for the client to try again.
const streamBusy = "Wakeshift can't open this stream just now: try again shortly"

// relay writes the upstream's frames to the client until either end
// closes it. SEP-3415 allows two openings: an error, or
// notifications/events/active first. The SSE response starts with the
// first notification; until active was relayed, an ending is an error (a
// terminated frame's own, else -32603), as JSON while nothing was written.
func (set *settings) relay(ctx context.Context, w http.ResponseWriter, r *http.Request, run pushRun, upstream io.Reader, asked *int64) {
	c := set.app
	rc := http.NewResponseController(w)
	// A stream outlives the server's WriteTimeout: cleared for this response.
	rc.SetWriteDeadline(time.Time{})
	started := false
	write := func(msg map[string]any) bool {
		b, err := json.Marshal(msg)
		if err != nil {
			return false
		}
		if !started {
			started = true
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
		}
		if _, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", b); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	meta := func(own map[string]any) map[string]any {
		m := make(map[string]any, len(own)+1)
		for k, v := range own {
			m[k] = v
		}
		m[subscriptionIDMeta] = run.id
		return m
	}
	notify := func(method string, params map[string]any) bool {
		return write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
	}
	// first: active wasn't relayed yet.
	first := true
	// fail answers the request with a JSON-RPC error: as JSON if nothing
	// was written, else as the stream's last message.
	fail := func(code int, msg string, data any) {
		if !started {
			rpcError(w, run.id, code, msg, data)
			return
		}
		e := map[string]any{"code": code, "message": msg}
		if data != nil {
			e["data"] = data
		}
		write(map[string]any{"jsonrpc": "2.0", "id": run.id, "error": e})
	}
	final := func() {
		if first {
			fail(rpcInternal, "Wakeshift couldn't be reached", nil)
			return
		}
		write(map[string]any{"jsonrpc": "2.0", "id": run.id, "result": pushStreamResult(run.id)})
	}

	type line struct {
		b   []byte
		err error // set on the last one: the upstream ended
	}
	lines := make(chan line)
	go func() {
		sc := bufio.NewScanner(upstream)
		sc.Buffer(make([]byte, 0, 64<<10), maxFrame)
		for sc.Scan() {
			b := bytes.TrimSpace(sc.Bytes())
			if len(b) == 0 {
				continue
			}
			select {
			case lines <- line{b: bytes.Clone(b)}:
			case <-ctx.Done():
				return
			}
		}
		err := sc.Err()
		if err == nil {
			err = io.EOF
		}
		select {
		case lines <- line{err: err}:
		case <-ctx.Done():
		}
	}()

	grant := defaultHeartbeat
	if asked != nil && *asked > 0 {
		grant = time.Duration(*asked) * time.Millisecond
	}
	silence := time.NewTimer(3 * grant)
	defer silence.Stop()
	var lifetime <-chan time.Time
	if set.push.MaxStream > 0 {
		t := time.NewTimer(set.push.MaxStream)
		defer t.Stop()
		lifetime = t.C
	}
	// Every Reauthorize, Identify and Authorize again, off the relay's
	// path: a refusal ends the stream as a revocation does. A failure keeps
	// it, until the last successful check (the open's, at first) is
	// maxUncheckedFor old: then it ends as MaxStream ends it.
	type rechecked struct {
		got recheck
		at  time.Time // when the check started
	}
	var checked chan rechecked
	checkedAt, unchecked := c.now(), maxUncheckedFor(set.push.Reauthorize)
	recheckAt := reauthorizeAfter(set.push.Reauthorize)
	c.life.init()
	for {
		select {
		case <-recheckAt:
			recheckAt, checked = nil, make(chan rechecked, 1)
			go func(done chan<- rechecked) {
				at := c.now()
				done <- rechecked{set.reauthorize(r.Context(), run), at}
			}(checked)
		case got := <-checked:
			checked = nil
			switch got.got {
			case recheckAllowed:
				checkedAt = got.at
			case recheckFailed:
				c.logLimited(r.Context(), "push_recheck_failed", fmt.Sprintf("identify or authorize failed when an open events/stream of %s was checked again, so the stream goes on", run.t.name),
					slog.String("event", run.t.name))
				if c.now().Sub(checkedAt) >= unchecked {
					c.logLimited(r.Context(), "push_recheck_stale", fmt.Sprintf("an open events/stream of %s went %d minutes without a successful check of identify and authorize, so it was ended for its client to reconnect", run.t.name, int(unchecked.Round(time.Minute).Minutes())),
						slog.String("event", run.t.name))
					final()
					return
				}
			}
			if got.got == recheckRefused {
				// Revoked: terminated (Forbidden, the open-time refusal's
				// words), the final result, and the upstream aborted
				// (stop, deferred). The subscription itself lapses. Before
				// active, that refusal is the request's error.
				if first {
					fail(run.codes.Forbidden, fill("native_refused", "{event}", run.t.name), map[string]any{"reason": "revoked"})
					return
				}
				if notify("notifications/events/terminated", map[string]any{"error": map[string]any{"code": run.codes.Forbidden,
					"message": fill("native_refused", "{event}", run.t.name), "data": map[string]any{"reason": "revoked"}}, "_meta": meta(nil)}) {
					final()
				}
				return
			}
			recheckAt = reauthorizeAfter(set.push.Reauthorize)
		case <-r.Context().Done():
			return // the client ended it: no result
		case <-c.life.drain.Done():
			final() // the client was closed: its client reconnects elsewhere
			return
		case <-lifetime:
			final()
			return
		case <-silence.C:
			c.logLimited(r.Context(), "push_upstream_failed", "Wakeshift's events/stream went silent for three heartbeat intervals, so the client's stream was ended for it to reconnect",
				slog.String("reason", "silent"))
			final()
			return
		case l := <-lines:
			if l.err != nil {
				if r.Context().Err() != nil {
					return
				}
				reason, what := "ended", "ended without closing"
				if l.err != io.EOF {
					reason, what = "broken", "broke off"
				}
				c.logLimited(r.Context(), "push_upstream_failed", "Wakeshift's events/stream "+what+", so the client's stream was ended for it to reconnect",
					slog.String("reason", reason))
				final()
				return
			}
			silence.Reset(3 * grant)
			var frame map[string]any
			if json.Unmarshal(l.b, &frame) != nil || frame == nil {
				continue
			}
			switch frame["type"] {
			case "active":
				if hb, ok := frame["heartbeat_ms"].(float64); ok && hb > 0 {
					grant = time.Duration(hb) * time.Millisecond
					silence.Reset(3 * grant)
				}
				if first && frame["created"] == true {
					set.pushCreated(r.Context(), run, frame)
				}
				first = false
				params := map[string]any{"cursor": frame["cursor"], "truncated": frame["truncated"] == true}
				if hb, ok := frame["heartbeat_ms"].(float64); ok {
					params["heartbeatMs"] = hb
				}
				params["_meta"] = meta(nil)
				if !notify("notifications/events/active", params) {
					return
				}
			case "event":
				o, _ := frame["occurrence"].(map[string]any)
				params := make(map[string]any, len(o)+1)
				for k, v := range o {
					params[k] = v
				}
				own, _ := o["_meta"].(map[string]any)
				params["_meta"] = meta(own)
				if !notify("notifications/events/event", params) {
					return
				}
			case "heartbeat":
				if !notify("notifications/events/heartbeat", map[string]any{"cursor": frame["cursor"], "_meta": meta(nil)}) {
					return
				}
			case "terminated":
				e, _ := frame["error"].(map[string]any)
				msg, _ := e["message"].(string)
				name, _ := e["name"].(string)
				out := map[string]any{"code": terminatedCode(name, run.codes), "message": msg}
				if d, ok := e["data"]; ok {
					out["data"] = d
				}
				if first {
					// Before active: the request's error, not a notification.
					fail(terminatedCode(name, run.codes), msg, out["data"])
					return
				}
				if notify("notifications/events/terminated", map[string]any{"error": out, "_meta": meta(nil)}) {
					final()
				}
				return
			case "close":
				final()
				return
			default:
				// A frame this SDK doesn't know: skipped, so a newer core can add one.
			}
		}
	}
}

// pushCreated runs OnSubscribe for a stream that created its subscription:
// a redeemed voucher's is the handing-out tool's, with the Context its call
// gave Subscribe if this process remembers it; any other is the native
// client's.
func (set *settings) pushCreated(ctx context.Context, run pushRun, frame map[string]any) {
	c := set.app
	args, _ := json.Marshal(run.args)
	via, _ := frame["via"].(string)
	if rest, ok := strings.CutPrefix(via, "tool:"); ok {
		tool, _ := frame["tool"].(string)
		if tool == "" {
			tool = rest
		}
		var value any
		if run.voucher != "" {
			value, _ = c.vouchers.take(run.voucher, c.now())
		}
		c.onSubscribed(ctx, run.t, args, subscribedInfo{user: run.call.User, via: viaTool, tool: tool, value: value})
		return
	}
	c.onSubscribed(ctx, run.t, args, subscribedInfo{user: run.call.User, via: viaNative})
}

// terminatedCode is a terminated frame's error name as the client's
// dialect's code.
func terminatedCode(name string, codes event.Codes) int {
	switch name {
	case "Forbidden":
		return codes.Forbidden
	case "NotFound":
		return codes.NotFound
	case "Unsupported":
		return codes.Unsupported
	case "ResourceExhausted":
		return codes.ResourceExhausted
	case "InvalidParams":
		return event.InvalidParams
	}
	return rpcInternal
}

// pushRefusal is the client's JSON-RPC error for Wakeshift refusing an
// events/stream before it opens (status and body), in the client's
// dialect's codes, as events/poll maps them, with the limits' data.
func pushRefusal(status int, body []byte, codes event.Codes, name string) (code int, msg string, data any) {
	e := errorOf(status, body)
	var extra struct {
		Error struct {
			Message any      `json:"message"`
			Limit   string   `json:"limit"`
			Max     *float64 `json:"max"`
		} `json:"error"`
	}
	json.Unmarshal(body, &extra)
	if m, _ := extra.Error.Message.(string); m == "" {
		// Not Wakeshift's own message (a proxy's page, a body without
		// one): a fixed text, never the body.
		e.Message = pushUpstreamFailed
	}
	switch e.Code {
	case "not_found", "unknown_event":
		nameJSON, _ := json.Marshal(name)
		return codes.NotFound, cmp.Or(e.Message, fill("native_unknown_event", "{event}", string(nameJSON))), map[string]string{"kind": "event"}
	case "tools_only":
		return codes.Forbidden, fill("native_tools_only", "{event}", name), nil
	case "subscription_limit", "stream_limit":
		limit := "subscriptions"
		if e.Code == "stream_limit" || extra.Error.Limit == "streams" {
			limit = "streams"
		}
		d := map[string]any{"limit": limit}
		if extra.Error.Max != nil {
			d["max"] = *extra.Error.Max
		}
		return codes.ResourceExhausted, e.Message, d
	case "unsupported":
		return codes.Unsupported, streamRefusal, map[string]string{"feature": "deliveryMode", "value": "push"}
	case "":
		if status >= 500 {
			return rpcInternal, "Wakeshift couldn't be reached", nil
		}
		return rpcInternal, pushUpstreamFailed, nil
	}
	return rpcErrorOf(e, codes, name, "push")
}
