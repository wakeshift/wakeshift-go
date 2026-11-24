package wakeshift_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wakeshift/wakeshift-go"
	"github.com/wakeshift/wakeshift-go/wakeshifttest"
)

// call.Wakes() is true only after a Subscribe of the call made or renewed a
// subscription that's still live, and the core said the conversation can
// be woken; false otherwise, so a tool keeps its own waiting.

// wakesTool adds "wakes", which runs f in its handler and keeps the call.
func wakesTool(w *wrapped, f func(ctx context.Context, call *wakeshift.ToolCall) error) **wakeshift.ToolCall {
	var kept *wakeshift.ToolCall
	wakeshift.AddTool(w.at, &mcp.Tool{Name: "wakes"}, func(ctx context.Context, call *wakeshift.ToolCall, in tableIn) (*mcp.CallToolResult, any, error) {
		kept = call
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil, f(ctx, call)
	}, wakeshift.Subscribes(w.turn, w.shared))
	return &kept
}

func TestToolCallWakes(t *testing.T) {
	oak, elm := tableArgs{Table: "oak"}, tableArgs{Table: "elm"}
	cases := []struct {
		name  string
		wakes any // the core's answer (nil: left out)
		fail  bool
		do    func(ctx context.Context, w *wrapped, call *wakeshift.ToolCall) error
		want  bool
	}{
		{name: "wakes true", wakes: true, want: true},
		{name: "wakes false", wakes: false},
		{name: "an older core leaves it out"},
		{name: "the subscribe fails", wakes: true, fail: true},
		{name: "renewed", wakes: true, want: true, do: func(ctx context.Context, w *wrapped, call *wakeshift.ToolCall) error {
			_, err := w.shared.Subscribe(ctx, call, oak)
			return err
		}},
		{name: "unsubscribed again", wakes: true, do: func(ctx context.Context, w *wrapped, call *wakeshift.ToolCall) error {
			ended, err := w.shared.Unsubscribe(ctx, call, oak)
			if err == nil && !ended {
				err = errors.New("nothing ended")
			}
			return err
		}},
		{name: "another subscription still holds", wakes: true, want: true, do: func(ctx context.Context, w *wrapped, call *wakeshift.ToolCall) error {
			if _, err := w.shared.Subscribe(ctx, call, elm); err != nil {
				return err
			}
			_, err := w.shared.Unsubscribe(ctx, call, oak)
			return err
		}},
		{name: "the latest answer counts", wakes: true, do: func(ctx context.Context, w *wrapped, call *wakeshift.ToolCall) error {
			w.core.mu.Lock()
			w.core.wakes = false
			w.core.mu.Unlock()
			_, err := w.shared.Subscribe(ctx, call, elm)
			return err
		}},
		{name: "a later subscribe of the call fails", wakes: true, do: func(ctx context.Context, w *wrapped, call *wakeshift.ToolCall) error {
			if !call.Wakes() {
				return errors.New("no Wakes after the first Subscribe")
			}
			w.core.mu.Lock()
			w.core.fail = map[string]bool{"/event-subscriptions": true}
			w.core.mu.Unlock()
			if _, err := w.shared.Subscribe(ctx, call, elm); err == nil {
				return errors.New("the second Subscribe didn't fail")
			}
			return nil
		}},
		{name: "resubscribed after unsubscribing", wakes: true, want: true, do: func(ctx context.Context, w *wrapped, call *wakeshift.ToolCall) error {
			if _, err := w.shared.Unsubscribe(ctx, call, oak); err != nil {
				return err
			}
			if call.Wakes() {
				return errors.New("Wakes after the unsubscribe")
			}
			_, err := w.shared.Subscribe(ctx, call, oak)
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWrapped(t, wakeshift.AttachOptions{})
			w.core.wakes = c.wakes
			if c.fail {
				w.core.fail = map[string]bool{"/event-subscriptions": true}
			}
			var before, after bool
			var subErr error
			kept := wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
				before = call.Wakes()
				_, subErr = w.shared.Subscribe(ctx, call, oak)
				if subErr == nil && c.do != nil {
					if err := c.do(ctx, w, call); err != nil {
						return err
					}
				}
				after = call.Wakes()
				return nil
			})
			if res := w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil); res.IsError || res.RPCError != nil {
				t.Fatalf("wakes: %+v", res)
			}
			if before {
				t.Error("Wakes before any Subscribe")
			}
			if (subErr != nil) != c.fail {
				t.Fatalf("Subscribe: %v", subErr)
			}
			if after != c.want {
				t.Errorf("Wakes: %v, want %v", after, c.want)
			}
			if got := (*kept).Wakes(); got != c.want {
				t.Errorf("Wakes once the call returned: %v, want %v", got, c.want)
			}
		})
	}
}

// While the user's notify choice is pending (Claude Code's first call asks
// it), the core answers wakes false though the subscription holds, so
// Wakes is false; a later call, once the user's pick runs, gets true.
func TestToolCallWakesPendingThenConfirmed(t *testing.T) {
	w := newWrapped(t, wakeshift.AttachOptions{})
	var got []bool
	wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
		sub, err := w.shared.Subscribe(ctx, call, tableArgs{Table: "oak"})
		if err != nil {
			return err
		}
		if sub.State != wakeshift.SubscriptionCreated && sub.State != wakeshift.SubscriptionRenewed {
			return fmt.Errorf("state %v", sub.State)
		}
		got = append(got, call.Wakes())
		return nil
	})
	for _, wakes := range []bool{false, true} {
		w.core.mu.Lock()
		w.core.wakes = wakes
		w.core.mu.Unlock()
		if res := w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil); res.IsError || res.RPCError != nil {
			t.Fatalf("wakes: %+v", res)
		}
	}
	if fmt.Sprint(got) != "[false true]" {
		t.Fatalf("Wakes per call: %v, want [false true]", got)
	}
}

// A call that fails rolls its subscriptions back: Wakes is false after.
func TestToolCallWakesRolledBack(t *testing.T) {
	w := newWrapped(t, wakeshift.AttachOptions{})
	w.core.wakes = true
	var during bool
	kept := wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
		if _, err := w.shared.Subscribe(ctx, call, tableArgs{Table: "oak"}); err != nil {
			return err
		}
		during = call.Wakes()
		return errors.New("the vendor's own failure")
	})
	w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil)
	if !during || (*kept).Wakes() {
		t.Fatalf("Wakes: %v during, %v after the rollback", during, (*kept).Wakes())
	}
}

// While the in-call breaker is open, Subscribe fails without asking the
// core, and Wakes stays false.
func TestToolCallWakesBreakerOpen(t *testing.T) {
	w := newWrapped(t, wakeshift.AttachOptions{})
	newClock(w.app)
	w.core.wakes = true
	w.core.fail = map[string]bool{"/event-subscriptions": true}
	var errs []error
	var wakes []bool
	wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
		_, err := w.shared.Subscribe(ctx, call, tableArgs{Table: "oak"})
		errs, wakes = append(errs, err), append(wakes, call.Wakes())
		return nil
	})
	for range 5 {
		w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil)
	}
	w.core.mu.Lock()
	w.core.fail = nil
	w.core.mu.Unlock()
	w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil)
	if len(errs) != 6 || !wakeshift.IsCode(errs[5], wakeshift.CodeUnreachable) {
		t.Fatalf("the breaker isn't open: %v", errs)
	}
	for i, wk := range wakes {
		if wk {
			t.Errorf("call %d: Wakes", i)
		}
	}
}

// A native caller (a ChatGPT Work chat on the web, MCP Events) is woken
// once the call offers it a line, whatever the core would say of a bridged
// one; not when its Subscribe comes back unavailable, or once the call
// unsubscribed it again.
func TestToolCallWakesNative(t *testing.T) {
	cases := []struct {
		name  string
		user  string
		wakes any
		unsub bool
		state wakeshift.SubscriptionState
		want  bool
	}{
		{name: "offered a line", user: "ada", state: wakeshift.SubscriptionNative, want: true},
		{name: "offered a line, the core says false for bridged", user: "ada", wakes: false, state: wakeshift.SubscriptionNative, want: true},
		{name: "unavailable without a user", state: wakeshift.SubscriptionUnavailable},
		{name: "unsubscribed again", user: "ada", unsub: true, state: wakeshift.SubscriptionNative},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWrapped(t, wakeshift.AttachOptions{
				IsNativeClient: func(context.Context, wakeshift.Request) bool { return true },
				Identify: func(context.Context, wakeshift.Request) (wakeshift.Identity, error) {
					return wakeshift.Identity{User: c.user}, nil
				},
			})
			w.core.wakes = c.wakes
			var state wakeshift.SubscriptionState
			var before, after bool
			wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
				before = call.Wakes()
				sub, err := w.shared.Subscribe(ctx, call, tableArgs{Table: "oak"})
				if err != nil {
					return err
				}
				state = sub.State
				if c.unsub {
					if _, err := w.shared.Unsubscribe(ctx, call, tableArgs{Table: "oak"}); err != nil {
						return err
					}
				}
				after = call.Wakes()
				return nil
			})
			if res := w.callTool(t, "ada", "some-client/1", "wakes", map[string]any{"table": "oak"}, nil); res.IsError || res.RPCError != nil {
				t.Fatalf("wakes: %+v", res)
			}
			if state != c.state || before || after != c.want {
				t.Fatalf("%s, Wakes %v before, %v after; want %s, %v", state, before, after, c.state, c.want)
			}
			if got := w.core.bodies("/event-subscriptions"); len(got) != 0 {
				t.Errorf("the core was asked to subscribe a native caller: %v", got)
			}
			// A line unsubscribed again in the call isn't offered in the result.
			for _, b := range w.core.bodies("/calls/addition") {
				if lines, _ := b["native"].([]any); c.unsub && len(lines) != 0 {
					t.Errorf("the addition still offers %v", lines)
				}
			}
		})
	}
}

// A disabled client's call and a nil call never wake.
func TestToolCallWakesDisabled(t *testing.T) {
	var nilCall *wakeshift.ToolCall
	if nilCall.Wakes() {
		t.Error("a nil call wakes")
	}
	d := newDisabled(t, "http://127.0.0.1:1")
	d.inCall(t, func(ctx context.Context, call *wakeshift.ToolCall) {
		if sub, err := d.news.Subscribe(ctx, call, tableArgs{Table: "oak"}); err != nil || sub.State != wakeshift.SubscriptionDisabled {
			t.Errorf("%+v %v", sub, err)
		}
		if call.Wakes() {
			t.Error("a disabled client's call wakes")
		}
	})
}

// Wakes is safe to read while the call's Subscribe runs.
func TestToolCallWakesConcurrent(t *testing.T) {
	w := newWrapped(t, wakeshift.AttachOptions{})
	w.core.wakes = true
	wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
		var wg sync.WaitGroup
		stop := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					call.Wakes()
				}
			}
		}()
		var err error
		for i := range 3 {
			if _, err = w.shared.Subscribe(ctx, call, tableArgs{Table: fmt.Sprint("t", i)}); err != nil {
				break
			}
		}
		close(stop)
		wg.Wait()
		return err
	})
	if res := w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil); res.IsError || res.RPCError != nil {
		t.Fatalf("wakes: %+v", res)
	}
}

// The stand-in call (wakeshifttest.NewToolCall): Wakes after a Subscribe
// that holds (a Native call's too), never with NoWakes, after a failed
// Subscribe or after the Unsubscribe of what it subscribed.
func TestStandInToolCallWakes(t *testing.T) {
	ctx := context.Background()
	wake := wakeshifttest.NewClient()
	news, err := wakeshift.DefineEvent(wake.Client, "table.news", wakeshift.EventDef[tableArgs, turnMsg]{Description: "News", Public: true})
	if err != nil {
		t.Fatal(err)
	}
	oak := tableArgs{Table: "oak"}

	call, _ := wakeshifttest.NewToolCall(wakeshifttest.ToolCallOptions{User: "ada"})
	if call.Wakes() {
		t.Error("Wakes before a Subscribe")
	}
	if _, err := news.Subscribe(ctx, call, oak); err != nil || !call.Wakes() {
		t.Fatalf("after Subscribe: %v, Wakes %v", err, call.Wakes())
	}
	if _, err := news.Unsubscribe(ctx, call, oak); err != nil || call.Wakes() {
		t.Fatalf("after Unsubscribe: %v, Wakes %v", err, call.Wakes())
	}

	call, _ = wakeshifttest.NewToolCall(wakeshifttest.ToolCallOptions{User: "ada", NoWakes: true})
	if _, err := news.Subscribe(ctx, call, oak); err != nil || call.Wakes() {
		t.Fatalf("NoWakes: %v, Wakes %v", err, call.Wakes())
	}

	call, _ = wakeshifttest.NewToolCall(wakeshifttest.ToolCallOptions{User: "ada", Native: true})
	if call.Wakes() {
		t.Error("Native: Wakes before a Subscribe")
	}
	if sub, err := news.Subscribe(ctx, call, oak); err != nil || sub.State != wakeshift.SubscriptionNative || !call.Wakes() {
		t.Fatalf("Native: %+v %v, Wakes %v", sub, err, call.Wakes())
	}
	if _, err := news.Unsubscribe(ctx, call, oak); err != nil || call.Wakes() {
		t.Fatalf("Native, after Unsubscribe: %v, Wakes %v", err, call.Wakes())
	}

	call, _ = wakeshifttest.NewToolCall(wakeshifttest.ToolCallOptions{User: "ada", Native: true, NoWakes: true})
	if _, err := news.Subscribe(ctx, call, oak); err != nil || call.Wakes() {
		t.Fatalf("Native with NoWakes: %v, Wakes %v", err, call.Wakes())
	}

	call, rec := wakeshifttest.NewToolCall(wakeshifttest.ToolCallOptions{User: "ada"})
	rec.FailNext("subscribe", wakeshifttest.FailUnreachable)
	if _, err := news.Subscribe(ctx, call, oak); err == nil || call.Wakes() {
		t.Fatalf("a failed Subscribe: %v, Wakes %v", err, call.Wakes())
	}
}

// The test client's fake answers wakes true, or false with NoWakes, so a
// tool served by it takes either branch.
func TestTestClientWakes(t *testing.T) {
	for _, noWakes := range []bool{false, true} {
		t.Run(fmt.Sprint("NoWakes ", noWakes), func(t *testing.T) {
			var opts []wakeshifttest.Option
			if noWakes {
				opts = append(opts, wakeshifttest.NoWakes())
			}
			wake := wakeshifttest.NewClient(opts...)
			t.Cleanup(func() { wake.Close() })
			news, err := wakeshift.DefineEvent(wake.Client, "table.news", wakeshift.EventDef[tableArgs, turnMsg]{Description: "News", Public: true})
			if err != nil {
				t.Fatal(err)
			}
			srv := mcp.NewServer(&mcp.Implementation{Name: "t", Version: "1"}, nil)
			at := wake.Attach(srv, wakeshift.AttachOptions{})
			var wakes bool
			wakeshift.AddTool(at, &mcp.Tool{Name: "watch"}, func(ctx context.Context, call *wakeshift.ToolCall, in tableIn) (*mcp.CallToolResult, any, error) {
				if _, err := news.Subscribe(ctx, call, tableArgs{Table: in.Table}); err != nil {
					return nil, nil, err
				}
				wakes = call.Wakes()
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil, nil
			}, wakeshift.Subscribes(news))
			st, ct := mcp.NewInMemoryTransports()
			if _, err := srv.Connect(context.Background(), st, nil); err != nil {
				t.Fatal(err)
			}
			cs, err := mcp.NewClient(&mcp.Implementation{Name: "claude-code", Version: "2.1"}, nil).Connect(context.Background(), ct, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer cs.Close()
			res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "watch", Arguments: map[string]any{"table": "oak", "session_key": "wsc_x"}})
			if err != nil || res.IsError {
				t.Fatalf("watch: %v %+v", err, res)
			}
			if wakes == noWakes {
				t.Errorf("Wakes %v with NoWakes %v", wakes, noWakes)
			}
		})
	}
}
