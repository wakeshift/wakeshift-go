package wakeshift_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wakeshift/wakeshift-go"
)

// Requests a tool call waits on have a short budget, are never retried, and
// stop for a while after five failures in a row (the in-call breaker), so a
// slow or failing Wakeshift never holds a vendor's tool call for long.

// clock is a test clock for SetClock, safe across the server's goroutines.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock(c *wakeshift.Client) *clock {
	k := &clock{now: time.Now()}
	wakeshift.SetClock(c, k.get)
	return k
}

func (k *clock) get() time.Time { k.mu.Lock(); defer k.mu.Unlock(); return k.now }

func (k *clock) add(d time.Duration) { k.mu.Lock(); k.now = k.now.Add(d); k.mu.Unlock() }

// watchTool adds "watch", which subscribes to table.news, keeps
// Subscribe's error in errs and goes on without wakes.
func watchTool(w *wrapped) *[]error {
	var mu sync.Mutex
	errs := &[]error{}
	wakeshift.AddTool(w.at, &mcp.Tool{Name: "watch"}, func(ctx context.Context, call *wakeshift.ToolCall, in tableIn) (*mcp.CallToolResult, any, error) {
		_, err := w.shared.Subscribe(ctx, call, tableArgs{Table: in.Table})
		mu.Lock()
		*errs = append(*errs, err)
		mu.Unlock()
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "Watching."}}}, nil, nil
	}, wakeshift.Subscribes(w.shared))
	return errs
}

func watched(t *testing.T, w *wrapped, table string) {
	t.Helper()
	res := w.callTool(t, "ada", "", "watch", map[string]any{"table": table}, nil)
	if res.IsError || res.RPCError != nil || len(res.Content) == 0 || res.texts()[0] != "Watching." {
		t.Fatalf("watch: %+v", res)
	}
}

// A core that never answers holds a tool call for the budget, not the
// HTTP client's 20 seconds: Subscribe, Unsubscribe and ResolveSession
// fail with timeout and the tool's result goes out as it gave it.
func TestInCallBudgetBoundsAHangingCore(t *testing.T) {
	for _, c := range []struct{ name, path string }{
		{"subscribe", "/event-subscriptions"},
		{"unsubscribe", "/event-subscriptions/unsubscribe"},
		{"resolve session", "/calls/session"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newWrapped(t, wakeshift.AttachOptions{}, wakeshift.WithCallTimeouts(wakeshift.CallTimeouts{Subscribe: 200 * time.Millisecond, Block: 200 * time.Millisecond}))
			w.core.broken = map[string]string{c.path: "hang"}
			var got error
			wakeshift.AddTool(w.at, &mcp.Tool{Name: "op"}, func(ctx context.Context, call *wakeshift.ToolCall, in tableIn) (*mcp.CallToolResult, any, error) {
				switch c.path {
				case "/event-subscriptions":
					_, got = w.shared.Subscribe(ctx, call, tableArgs{Table: in.Table})
				case "/event-subscriptions/unsubscribe":
					_, got = w.shared.Unsubscribe(ctx, call, tableArgs{Table: in.Table})
				default:
					_, got = call.ResolveSession(ctx)
				}
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "Done."}}}, nil, nil
			}, wakeshift.Subscribes(w.shared))
			start := time.Now()
			res := w.callTool(t, "ada", "", "op", map[string]any{"table": "oak", "session_key": "wsc_s1"}, nil)
			if took := time.Since(start); took > 3*time.Second {
				t.Fatalf("the call took %s", took)
			}
			if res.IsError || len(res.texts()) == 0 || res.texts()[0] != "Done." {
				t.Fatalf("result: %+v", res)
			}
			var e *wakeshift.APIError
			if !errors.As(got, &e) || e.Code != wakeshift.CodeTimeout || e.Status != 0 || !strings.Contains(e.Message, "didn't answer in time") {
				t.Fatalf("%s: %v, want timeout", c.name, got)
			}
		})
	}
}

// A hanging addition: the tool's result goes out unchanged within the
// Block budget, and what the call subscribed is rolled back.
func TestInCallBudgetBoundsAHangingAddition(t *testing.T) {
	w := newWrapped(t, wakeshift.AttachOptions{}, wakeshift.WithCallTimeouts(wakeshift.CallTimeouts{Block: 200 * time.Millisecond}))
	w.core.broken = map[string]string{"/calls/addition": "hang"}
	errs := watchTool(w)
	start := time.Now()
	watched(t, w, "oak")
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the call took %s", took)
	}
	if (*errs)[0] != nil {
		t.Fatalf("subscribe: %v", (*errs)[0])
	}
	ends := w.core.bodies("/event-subscriptions/unsubscribe")
	if len(ends) != 1 || ends[0]["reason"] != "rolled_back" {
		t.Fatalf("rollback: %v", ends)
	}
}

// A cold first native call: it makes no subscribe request, so its addition
// is the first to wait for the manifest. That wait gets the Subscribe
// budget, not Block, so the line still goes out; a manifest send that
// hangs costs at most Subscribe + Block.
func TestInCallColdNativeCallWaitsForTheManifestWithinSubscribe(t *testing.T) {
	native := wakeshift.AttachOptions{
		IsNativeClient: func(context.Context, wakeshift.Request) bool { return true },
		Identify: func(context.Context, wakeshift.Request) (wakeshift.Identity, error) {
			return wakeshift.Identity{User: "ada"}, nil
		},
	}
	for _, c := range []struct {
		name      string
		slow      func(f *wakeCore)
		within    time.Duration
		additions int
	}{
		{"a slow manifest", func(f *wakeCore) { f.manifestDelay = 300 * time.Millisecond }, 2 * time.Second, 1},
		{"a hanging manifest", func(f *wakeCore) { f.broken = map[string]string{"/manifest/merge": "hang"} }, time.Second, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			subscribe := 2 * time.Second
			if c.additions == 0 {
				subscribe = 200 * time.Millisecond
			}
			// A short client timeout, so the hanging send the call left
			// running in the background ends with the test.
			w := newWrapped(t, native, wakeshift.WithCallTimeouts(wakeshift.CallTimeouts{Subscribe: subscribe, Block: 100 * time.Millisecond}),
				wakeshift.WithHTTPClient(&http.Client{Timeout: 500 * time.Millisecond}), wakeshift.WithMaxRetries(0))
			c.slow(w.core)
			var wakes bool
			wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
				_, err := w.shared.Subscribe(ctx, call, tableArgs{Table: "oak"})
				wakes = call.Wakes()
				return err
			})
			start := time.Now()
			res := w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil)
			if took := time.Since(start); took > c.within {
				t.Fatalf("the call took %s", took)
			}
			if res.IsError || res.RPCError != nil || !wakes {
				t.Fatalf("result %+v, wakes %v", res, wakes)
			}
			adds := w.core.bodies("/calls/addition")
			if len(adds) != c.additions || c.additions == 1 && adds[0]["native"] == nil {
				t.Fatalf("additions: %v", adds)
			}
		})
	}
}

// An in-call request is sent once, whatever WithMaxRetries says: neither a
// failed connection nor the load balancer's refusal is retried.
func TestInCallNeverRetries(t *testing.T) {
	defer wakeshift.SetRetryPauses(time.Millisecond)()
	for _, mode := range []string{"unreachable", "refused"} {
		t.Run(mode, func(t *testing.T) {
			w := newWrapped(t, wakeshift.AttachOptions{}, wakeshift.WithMaxRetries(2))
			w.core.broken = map[string]string{"/event-subscriptions": mode}
			errs := watchTool(w)
			watched(t, w, "oak")
			if n := len(w.core.bodies("/event-subscriptions")); n != 1 {
				t.Fatalf("sent %d times, want once", n)
			}
			if code := map[string]wakeshift.Code{"unreachable": wakeshift.CodeUnreachable, "refused": ""}[mode]; code != "" && !wakeshift.IsCode((*errs)[0], code) {
				t.Fatalf("subscribe: %v", (*errs)[0])
			}
			if (*errs)[0] == nil {
				t.Fatal("subscribe succeeded")
			}
		})
	}
}

// Five failures in a row open the breaker: in-call requests are skipped
// (unreachable at once, nothing sent) for the cool-down; then one probe
// goes; its success closes the breaker, its failure reopens it.
func TestInCallBreaker(t *testing.T) {
	var logs logLines
	w := newWrapped(t, wakeshift.AttachOptions{}, wakeshift.WithLogger(logs.logger()))
	k := newClock(w.app)
	errs := watchTool(w)
	sent := func() int { return len(w.core.bodies("/event-subscriptions")) }
	fail := func(on bool) {
		w.core.mu.Lock()
		w.core.fail = map[string]bool{"/event-subscriptions": on}
		w.core.mu.Unlock()
	}
	fail(true)
	for i := range 5 {
		watched(t, w, fmt.Sprintf("t%d", i))
	}
	if sent() != 5 {
		t.Fatalf("%d sent, want 5", sent())
	}
	// Open: skipped.
	watched(t, w, "t5")
	watched(t, w, "t6")
	if sent() != 5 {
		t.Fatalf("%d sent while open, want 5", sent())
	}
	var e *wakeshift.APIError
	if err := (*errs)[6]; !errors.As(err, &e) || e.Code != wakeshift.CodeUnreachable || e.Status != 0 ||
		!strings.Contains(err.Error(), "wakeshift: can't reach Wakeshift at ") ||
		!strings.Contains(e.Message, "(skipped: 5 requests in a row failed inside tool calls, so they pause for 30 seconds). Check that WAKESHIFT_CORE, or the core option, is the address of the console your key came from. https://wakeshift.ai/docs/errors#unreachable") {
		t.Fatalf("skipped: %v", err)
	}
	// Still open just before the cool-down ends.
	k.add(29 * time.Second)
	watched(t, w, "t7")
	if sent() != 5 {
		t.Fatalf("%d sent before the cool-down ended", sent())
	}
	// A failed probe opens it again.
	k.add(2 * time.Second)
	watched(t, w, "t8")
	watched(t, w, "t9")
	if sent() != 6 {
		t.Fatalf("%d sent, want the probe only", sent())
	}
	// A successful probe closes it.
	k.add(31 * time.Second)
	fail(false)
	watched(t, w, "t10")
	watched(t, w, "t11")
	if sent() != 8 || (*errs)[10] != nil || (*errs)[11] != nil {
		t.Fatalf("%d sent, errors %v", sent(), (*errs)[10:])
	}
	// Closed again: the count starts over.
	fail(true)
	for i := range 4 {
		watched(t, w, fmt.Sprintf("u%d", i))
	}
	fail(false)
	watched(t, w, "u4")
	if sent() != 13 || (*errs)[len(*errs)-1] != nil {
		t.Fatalf("%d sent, want 13", sent())
	}
	var lines []map[string]any
	for _, l := range logs.lines() {
		if l["code"] == "core_unavailable" {
			lines = append(lines, l)
		}
	}
	if len(lines) != 1 || lines[0]["level"] != "WARN" || lines[0]["count"] != float64(1) ||
		!strings.Contains(lines[0]["msg"].(string), "wakeshift: Wakeshift failed 5 times in a row inside tool calls, so tool calls skip it for 30 seconds. Nothing to change;") {
		t.Fatalf("core_unavailable: %v", lines)
	}
}

// Wakeshift's 4xx answers are answers: they never trip the breaker.
func TestInCallBreakerIgnores4xx(t *testing.T) {
	for _, status := range []string{"400", "403"} {
		t.Run(status, func(t *testing.T) {
			w := newWrapped(t, wakeshift.AttachOptions{})
			w.core.broken = map[string]string{"/event-subscriptions": status}
			errs := watchTool(w)
			for i := range 7 {
				watched(t, w, fmt.Sprintf("t%d", i))
			}
			if n := len(w.core.bodies("/event-subscriptions")); n != 7 {
				t.Fatalf("%d sent, want 7", n)
			}
			if wakeshift.IsCode((*errs)[6], wakeshift.CodeUnreachable) {
				t.Fatalf("skipped: %v", (*errs)[6])
			}
		})
	}
}

// While the breaker is open, an emit (outside tool calls) still goes, and
// is retried per WithMaxRetries.
func TestInCallBreakerLeavesEmitsAlone(t *testing.T) {
	defer wakeshift.SetRetryPauses(time.Millisecond)()
	w := newWrapped(t, wakeshift.AttachOptions{}, wakeshift.WithMaxRetries(2))
	w.core.fail = map[string]bool{"/event-subscriptions": true}
	errs := watchTool(w)
	for i := range 6 {
		watched(t, w, fmt.Sprintf("t%d", i))
	}
	if !wakeshift.IsCode((*errs)[5], wakeshift.CodeUnreachable) {
		t.Fatalf("the breaker isn't open: %v", (*errs)[5])
	}
	if _, err := w.shared.Emit(context.Background(), tableArgs{Table: "t0"}, turnMsg{Text: "hi"}); err != nil {
		t.Fatalf("emit while open: %v", err)
	}
	w.core.mu.Lock()
	w.core.broken = map[string]string{"/events/emit": "refused"}
	w.core.mu.Unlock()
	if _, err := w.shared.Emit(context.Background(), tableArgs{Table: "t0"}, turnMsg{Text: "hi"}); err == nil {
		t.Fatal("a refused emit succeeded")
	}
	if n := len(w.core.bodies("/events/emit")); n != 1+3 {
		t.Fatalf("%d emits sent, want 1 and then 3 (2 retries)", n)
	}
}

// WithCallTimeouts: a negative budget is a config error.
func TestWithCallTimeoutsValidation(t *testing.T) {
	for _, ct := range []wakeshift.CallTimeouts{{Subscribe: -time.Second}, {Block: -1}} {
		if _, err := wakeshift.New(wakeshift.WithKey(""), wakeshift.WithCallTimeouts(ct)); !wakeshift.IsCode(err, wakeshift.CodeConfig) ||
			!strings.Contains(err.Error(), "WithCallTimeouts") {
			t.Errorf("%+v: %v, want config", ct, err)
		}
	}
	if _, err := wakeshift.New(wakeshift.WithKey(""), wakeshift.WithCallTimeouts(wakeshift.CallTimeouts{})); err != nil {
		t.Errorf("zero: %v", err)
	}
}
