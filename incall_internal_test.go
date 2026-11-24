package wakeshift

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// The breaker's states: while the probe is in flight others are skipped;
// a canceled probe frees the slot for the next request.
func TestBreakerProbe(t *testing.T) {
	var b breaker
	now := time.Now()
	for range breakerThreshold {
		if ok, _ := b.admit(now); !ok {
			t.Fatal("skipped while closed")
		}
		b.record(now, outcomeFailure, false)
	}
	if ok, _ := b.admit(now); ok {
		t.Fatal("admitted while open")
	}
	now = now.Add(breakerCooldown)
	ok, probe := b.admit(now)
	if !ok || !probe {
		t.Fatalf("after the cool-down: %v %v, want the probe", ok, probe)
	}
	if ok, _ := b.admit(now); ok {
		t.Fatal("admitted while the probe is in flight")
	}
	b.record(now, outcomeNone, true) // canceled
	ok, probe = b.admit(now)
	if !ok || !probe {
		t.Fatalf("after a canceled probe: %v %v, want the next probe", ok, probe)
	}
	b.record(now, outcomeSuccess, true)
	if ok, probe := b.admit(now); !ok || probe {
		t.Fatalf("after a successful probe: %v %v, want closed", ok, probe)
	}
	if b.failures != 0 {
		t.Fatalf("failures %d after closing", b.failures)
	}
}

// Once open, a request admitted before it opened and answered after
// neither closes it nor extends the cool-down: only the probe decides.
func TestBreakerIgnoresLateOutcomes(t *testing.T) {
	var b breaker
	now := time.Now()
	for range breakerThreshold {
		b.admit(now)
	}
	for range breakerThreshold {
		b.record(now, outcomeFailure, false)
	}
	until := b.openUntil
	b.record(now.Add(time.Second), outcomeSuccess, false)
	b.record(now.Add(2*time.Second), outcomeFailure, false)
	if ok, _ := b.admit(now.Add(3 * time.Second)); ok {
		t.Fatal("a late success closed the breaker")
	}
	if !b.openUntil.Equal(until) {
		t.Fatalf("a late failure moved the cool-down from %v to %v", until, b.openUntil)
	}
}

// What counts: unreachable, timeout and 5xx fail; any answer below 500
// (and bad_response on a 2xx) succeeds; the caller's cancellation is
// neither.
func TestBreakerOutcomes(t *testing.T) {
	bg := context.Background()
	canceled, cancel := context.WithCancel(bg)
	cancel()
	for _, c := range []struct {
		ctx  context.Context
		err  error
		want outcome
	}{
		{bg, nil, outcomeSuccess},
		{bg, &APIError{Code: CodeUnreachable}, outcomeFailure},
		{bg, &APIError{Code: CodeTimeout}, outcomeFailure},
		{bg, &APIError{Status: 500}, outcomeFailure},
		{bg, &APIError{Status: 503, Code: "unavailable"}, outcomeFailure},
		{bg, &APIError{Status: 400, Code: CodeBadRequest}, outcomeSuccess},
		{bg, &APIError{Status: 403}, outcomeSuccess},
		{bg, &APIError{Status: 429}, outcomeSuccess},
		{bg, &APIError{Status: 200, Code: CodeBadResponse}, outcomeSuccess},
		{canceled, errors.New("ended"), outcomeNone},
	} {
		if got := outcomeOf(c.ctx, c.err); got != c.want {
			t.Errorf("%v: %v, want %v", c.err, got, c.want)
		}
	}
}

// New fills in CallTimeouts' defaults field by field.
func TestCallTimeoutsDefaults(t *testing.T) {
	for _, c := range []struct{ in, want CallTimeouts }{
		{CallTimeouts{}, CallTimeouts{Subscribe: 5 * time.Second, Block: time.Second}},
		{CallTimeouts{Subscribe: 2 * time.Second}, CallTimeouts{Subscribe: 2 * time.Second, Block: time.Second}},
		{CallTimeouts{Block: 300 * time.Millisecond}, CallTimeouts{Subscribe: 5 * time.Second, Block: 300 * time.Millisecond}},
	} {
		a, err := New(WithKey(""), WithCallTimeouts(c.in))
		if err != nil {
			t.Fatal(err)
		}
		if a.calls != c.want {
			t.Errorf("%+v: %+v, want %+v", c.in, a.calls, c.want)
		}
	}
	a, _ := New(WithKey(""))
	if a.calls != defaultCallTimeouts {
		t.Errorf("no option: %+v", a.calls)
	}
}

// A tool call waiting on another's manifest send gives up at its deadline,
// as a timeout.
func TestInCallBoundsTheManifestWait(t *testing.T) {
	a := clientAt(t, "http://127.0.0.1:1")
	a.mu.Lock()
	a.attached = true
	a.mu.Unlock()
	if err := a.sendMu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.sendMu.unlock()
	start := time.Now()
	err := a.inCall(context.Background(), 100*time.Millisecond, "POST", "/api/apps/v1/event-subscriptions", map[string]any{}, nil)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s", took)
	}
	if !IsCode(err, CodeTimeout) {
		t.Fatalf("%v, want timeout", err)
	}
}

// A manifest send a tool call waits on is the client's shared work: the
// call stops waiting at its budget, but the send runs on and finishes, and
// none of the call's requests is sent late.
func TestInCallLeavesTheManifestSendToFinish(t *testing.T) {
	release := make(chan struct{})
	var merged, subscribes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/apps/v1/manifest/merge":
			<-release
			merged.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		case "/api/apps/v1/event-subscriptions":
			subscribes.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	a := clientAt(t, srv.URL)
	defineSample(t, a, "chat.message")
	a.mu.Lock()
	a.attached = true
	a.mu.Unlock()
	err := a.inCall(context.Background(), 100*time.Millisecond, "POST", "/api/apps/v1/event-subscriptions", map[string]any{}, nil)
	if !IsCode(err, CodeTimeout) {
		t.Fatalf("%v, want timeout", err)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		a.mu.Lock()
		sent := a.sent
		a.mu.Unlock()
		if sent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the manifest send didn't finish after the call gave up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := merged.Load(); n != 1 {
		t.Fatalf("%d merges, want 1", n)
	}
	if n := subscribes.Load(); n != 0 {
		t.Fatalf("%d subscribes sent after the call gave up, want 0", n)
	}
}
