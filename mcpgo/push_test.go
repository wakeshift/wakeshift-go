package mcpgo_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/wakeshift/wakeshift-go"
	"github.com/wakeshift/wakeshift-go/mcpgo"
	"github.com/wakeshift/wakeshift-go/wakeshifttest"
)

type chatArgs struct {
	Channel string `json:"channel"`
}

type chatMsg struct {
	Text string `json:"text"`
}

// MCP Events push on an mcp-go server, against wakeshifttest's fake
// Wakeshift: events/list lists push, an events/stream gets active, then an
// event for each matching emit, each with the request's ID as its
// subscription ID, and the final result when the client is closed.
func TestPushOnMCPGo(t *testing.T) {
	wake := wakeshifttest.NewClient()
	chat, err := wakeshift.DefineEvent(wake.Client, "chat.message", wakeshift.EventDef[chatArgs, chatMsg]{Description: "A new message", Public: true,
		Text: func(m chatMsg, _ chatArgs) string { return m.Text }})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewMCPServer("t", "1.0.0")
	at := mcpgo.Attach(wake.Client, srv, wakeshift.AttachOptions{Identify: func(_ context.Context, r wakeshift.Request) (wakeshift.Identity, error) {
		return wakeshift.Identity{User: r.Header.Get("X-User")}, nil
	}})
	hs := httptest.NewServer(at.Handler(server.NewStreamableHTTPServer(srv)))
	t.Cleanup(hs.Close)
	post := func(ctx context.Context, id, method string, params any) *http.Response {
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "method": method, "params": params})
		req, _ := http.NewRequestWithContext(ctx, "POST", hs.URL, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("User-Agent", "some-draft-client/1")
		req.Header.Set("X-User", "u1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	resp := post(context.Background(), "1", "events/list", map[string]any{})
	var list struct {
		Result struct {
			Events []struct {
				Delivery []string `json:"delivery"`
			} `json:"events"`
		} `json:"result"`
	}
	json.NewDecoder(resp.Body).Decode(&list)
	resp.Body.Close()
	if len(list.Result.Events) != 1 || !reflect.DeepEqual(list.Result.Events[0].Delivery, []string{"webhook", "poll", "push"}) {
		t.Fatalf("events/list: %+v", list.Result)
	}

	resp = post(context.Background(), `"s-1"`, "events/stream", map[string]any{"name": "chat.message", "arguments": map[string]any{"channel": "eng"}, "cursor": nil})
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type %q", ct)
	}
	msgs := make(chan map[string]any, 16)
	go func() {
		defer close(msgs)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if d, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				var m map[string]any
				json.Unmarshal([]byte(d), &m)
				msgs <- m
			}
		}
	}()
	next := func() map[string]any {
		select {
		case m := <-msgs:
			return m
		case <-time.After(5 * time.Second):
			t.Fatal("nothing came")
			return nil
		}
	}
	active := next()
	if active["method"] != "notifications/events/active" {
		t.Fatalf("first: %v", active)
	}
	if subs := wake.Subscriptions(); len(subs) != 1 || subs[0].Via != "native" || subs[0].User != "u1" {
		t.Errorf("subscriptions: %+v", subs)
	}
	if _, err := chat.Emit(context.Background(), chatArgs{Channel: "eng"}, chatMsg{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	ev := next()
	params, _ := ev["params"].(map[string]any)
	meta, _ := params["_meta"].(map[string]any)
	if ev["method"] != "notifications/events/event" || params["name"] != "chat.message" || meta["io.modelcontextprotocol/subscriptionId"] != "s-1" ||
		meta["ai.wakeshift/text"] != "hi" || !reflect.DeepEqual(params["data"], map[string]any{"text": "hi"}) {
		t.Errorf("event: %v", ev)
	}
	wake.Close()
	final := next()
	res, _ := final["result"].(map[string]any)
	fmeta, _ := res["_meta"].(map[string]any)
	if final["id"] != "s-1" || res["resultType"] != "complete" || fmeta["io.modelcontextprotocol/subscriptionId"] != "s-1" {
		t.Errorf("final: %v", final)
	}
}

// opencode on the legacy protocol declares MCP Events only in its
// session's initialize: where push is served, its tools/list leaves out
// subscribe_events, as a 2026-07-28 request that declares it gets; another
// client's session, or one that doesn't declare it, keeps it.
func TestLegacyInitializeDeclarationOnMCPGo(t *testing.T) {
	for _, c := range []struct {
		name, client string
		declares     bool
		hidden       bool
	}{
		{"opencode, declared in initialize", "opencode", true, true},
		{"opencode, not declared", "opencode", false, false},
		{"Claude Code, declared (bridged whatever it declares)", "claude-code", true, false},
	} {
		wake := wakeshifttest.NewClient()
		if _, err := wakeshift.DefineEvent(wake.Client, "chat.message", wakeshift.EventDef[chatArgs, chatMsg]{Description: "A new message", Public: true}); err != nil {
			t.Fatal(err)
		}
		srv := server.NewMCPServer("t", "1.0.0")
		at := mcpgo.Attach(wake.Client, srv, wakeshift.AttachOptions{Identify: func(_ context.Context, r wakeshift.Request) (wakeshift.Identity, error) {
			return wakeshift.Identity{User: "u1"}, nil
		}})
		hs := httptest.NewServer(at.Handler(server.NewStreamableHTTPServer(srv)))
		sid := ""
		post := func(id, method string, params any) *http.Response {
			m := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
			if id != "" {
				m["id"] = json.RawMessage(id)
			}
			b, _ := json.Marshal(m)
			req, _ := http.NewRequest("POST", hs.URL, bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			req.Header.Set("User-Agent", "Bun/1.3.14")
			if sid != "" {
				req.Header.Set("Mcp-Session-Id", sid)
				req.Header.Set("MCP-Protocol-Version", "2025-11-25")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return resp
		}
		caps := map[string]any{}
		if c.declares {
			caps["extensions"] = map[string]any{"io.modelcontextprotocol/events": map[string]any{}}
		}
		resp := post("0", "initialize", map[string]any{"protocolVersion": "2025-11-25", "capabilities": caps, "clientInfo": map[string]any{"name": c.client, "version": "1"}})
		sid = resp.Header.Get("Mcp-Session-Id")
		resp.Body.Close()
		if sid == "" {
			t.Fatalf("%s: no session", c.name)
		}
		post("", "notifications/initialized", map[string]any{}).Body.Close()
		resp = post("1", "tools/list", map[string]any{})
		var body []byte
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				if d, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
					body = []byte(d)
				}
			}
		} else {
			body, _ = io.ReadAll(resp.Body)
		}
		resp.Body.Close()
		var list struct {
			Result struct {
				Tools []struct {
					Name string `json:"name"`
				} `json:"tools"`
			} `json:"result"`
		}
		json.Unmarshal(body, &list)
		var names []string
		for _, tl := range list.Result.Tools {
			names = append(names, tl.Name)
		}
		if len(names) == 0 {
			t.Fatalf("%s: tools/list: %s", c.name, body)
		}
		if hidden := !slices.Contains(names, "subscribe_events"); hidden != c.hidden {
			t.Errorf("%s: tools %v", c.name, names)
		}
		hs.Close()
		wake.Close()
	}
}
