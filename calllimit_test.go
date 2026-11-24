package wakeshift_test

import (
	"context"
	"testing"
	"time"

	"github.com/wakeshift/wakeshift-go"
	"github.com/wakeshift/wakeshift-go/wakeshifttest"
)

// call.CallLimit() and call.ShouldSkipWait: a tool asked for a long
// explicit wait skips it only when Wakeshift wakes the conversation and
// the program would cut the call off before the wait ends.

func TestToolCallCallLimit(t *testing.T) {
	cases := []struct {
		name      string
		wakes     any // the core's answers (nil: left out)
		callLimit any
		fail      bool
		limit     time.Duration
		skip      bool
	}{
		{name: "Claude Code's 60 s, woken", wakes: true, callLimit: 60, limit: time.Minute, skip: true},
		{name: "Codex's 300 s, woken: a 300 s wait doesn't fit", wakes: true, callLimit: 300, limit: 5 * time.Minute, skip: true},
		{name: "a longer limit than the wait", wakes: true, callLimit: 600, limit: 10 * time.Minute},
		{name: "the user's notify choice pending (wakes false)", wakes: false, callLimit: 60, limit: time.Minute},
		{name: "a limit that isn't known (left out)", wakes: true},
		{name: "a core that answers something else", wakes: true, callLimit: "60"},
		{name: "a negative limit", wakes: true, callLimit: -5},
		{name: "the subscribe fails", wakes: true, callLimit: 60, fail: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWrapped(t, wakeshift.AttachOptions{})
			w.core.wakes, w.core.callLimit = c.wakes, c.callLimit
			if c.fail {
				w.core.fail = map[string]bool{"/event-subscriptions": true}
			}
			var before, limit time.Duration
			var skip bool
			wakesTool(w, func(ctx context.Context, call *wakeshift.ToolCall) error {
				before = call.CallLimit()
				_, _ = w.shared.Subscribe(ctx, call, tableArgs{Table: "oak"})
				limit, skip = call.CallLimit(), call.ShouldSkipWait(300*time.Second)
				return nil
			})
			if res := w.callTool(t, "ada", "", "wakes", map[string]any{"table": "oak"}, nil); res.IsError || res.RPCError != nil {
				t.Fatalf("wakes: %+v", res)
			}
			if before != 0 {
				t.Errorf("CallLimit before a Subscribe: %v", before)
			}
			if limit != c.limit || skip != c.skip {
				t.Errorf("CallLimit %v, ShouldSkipWait %v; want %v, %v", limit, skip, c.limit, c.skip)
			}
		})
	}
}

// skip-wait.json, on a stand-in call made with NoWakes and CallLimit,
// after one Subscribe.
func TestSkipWaitFixture(t *testing.T) {
	var f struct {
		Cases []struct {
			Name      string   `json:"name"`
			Wakes     bool     `json:"wakes"`
			CallLimit *float64 `json:"call_limit"`
			Seconds   *float64 `json:"seconds"`
			Skip      bool     `json:"skip"`
		} `json:"cases"`
	}
	readFixture(t, "skip-wait.json", &f)
	if len(f.Cases) == 0 {
		t.Fatal("no cases")
	}
	secs := func(v *float64) time.Duration {
		if v == nil {
			return 0
		}
		return time.Duration(*v * float64(time.Second))
	}
	ctx := context.Background()
	wake := wakeshifttest.NewClient()
	news, err := wakeshift.DefineEvent(wake.Client, "table.news", wakeshift.EventDef[tableArgs, turnMsg]{Description: "News", Public: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range f.Cases {
		t.Run(c.Name, func(t *testing.T) {
			call, _ := wakeshifttest.NewToolCall(wakeshifttest.ToolCallOptions{User: "ada", NoWakes: !c.Wakes, CallLimit: secs(c.CallLimit)})
			if call.CallLimit() != 0 {
				t.Errorf("CallLimit before a Subscribe: %v", call.CallLimit())
			}
			if _, err := news.Subscribe(ctx, call, tableArgs{Table: "oak"}); err != nil {
				t.Fatal(err)
			}
			if got := call.CallLimit(); got != secs(c.CallLimit) {
				t.Errorf("CallLimit %v, want %v", got, secs(c.CallLimit))
			}
			if got := call.ShouldSkipWait(secs(c.Seconds)); got != c.Skip {
				t.Errorf("ShouldSkipWait(%v): %v, want %v", secs(c.Seconds), got, c.Skip)
			}
		})
	}
}

// A Native stand-in call and a nil call never have a limit, so they never
// skip a wait.
func TestCallLimitNever(t *testing.T) {
	ctx := context.Background()
	wake := wakeshifttest.NewClient()
	news, err := wakeshift.DefineEvent(wake.Client, "table.news", wakeshift.EventDef[tableArgs, turnMsg]{Description: "News", Public: true})
	if err != nil {
		t.Fatal(err)
	}
	call, _ := wakeshifttest.NewToolCall(wakeshifttest.ToolCallOptions{User: "ada", Native: true, CallLimit: time.Minute})
	if _, err := news.Subscribe(ctx, call, tableArgs{Table: "oak"}); err != nil || !call.Wakes() {
		t.Fatalf("Native: %v, Wakes %v", err, call.Wakes())
	}
	if call.CallLimit() != 0 || call.ShouldSkipWait(5*time.Minute) {
		t.Errorf("Native: CallLimit %v, ShouldSkipWait true", call.CallLimit())
	}
	var none *wakeshift.ToolCall
	if none.CallLimit() != 0 || none.ShouldSkipWait(5*time.Minute) {
		t.Error("a nil call has a limit")
	}
}
