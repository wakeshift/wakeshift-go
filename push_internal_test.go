package wakeshift

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wakeshift/wakeshift-go/internal/mcpevents/event"
)

// pushFixture is mcp-events-wire.json's push section (the TypeScript,
// Python and Go SDKs run it).
type pushFixture struct {
	Delivery          []string `json:"delivery"`
	DeliveryToolsOnly []string `json:"delivery_tools_only"`
	Streams           []struct {
		Name          string           `json:"name"`
		ID            json.RawMessage  `json:"id"`
		User          *string          `json:"user"`
		Params        map[string]any   `json:"params"`
		CoreRequest   map[string]any   `json:"core_request"`
		CallUser      *string          `json:"call_user"`
		Frames        []map[string]any `json:"frames"`
		Notifications []map[string]any `json:"notifications"`
		Result        map[string]any   `json:"result"`
	} `json:"streams"`
	TerminatedCodes []struct {
		Name string         `json:"name"`
		Code map[string]int `json:"code"`
	} `json:"terminated_codes"`
	NotServed struct {
		Code    map[string]int `json:"code"`
		Message string         `json:"message"`
		Data    any            `json:"data"`
	} `json:"not_served"`
	Refused []struct {
		Name       string          `json:"name"`
		User       *string         `json:"user"`
		Header     string          `json:"header"`
		Params     map[string]any  `json:"params"`
		ID         json.RawMessage `json:"id"`
		MinMs      int64           `json:"min_ms"`
		CoreStatus int             `json:"core_status"`
		CoreError  map[string]any  `json:"core_error"`
		// RetryAfter and CoreHeaders: a 503's Retry-After, in whichever
		// form the fixture gives it.
		RetryAfter  any               `json:"retry_after"`
		CoreHeaders map[string]string `json:"core_headers"`
		Error       *struct {
			Code    map[string]int `json:"code"`
			Message string         `json:"message"`
			Data    any            `json:"data"`
		} `json:"error"`
		Result map[string]any `json:"result"`
	} `json:"refused"`
	Reauthorize *struct {
		DefaultMs int64 `json:"default_ms"`
		MinMs     int64 `json:"min_ms"`
		MaxMs     int64 `json:"max_ms"`
		Clamp     []struct {
			ReauthorizeMs int64 `json:"reauthorize_ms"`
			WantMs        int64 `json:"want_ms"`
		} `json:"clamp"`
		Invalid      []any           `json:"invalid"`
		StreamID     json.RawMessage `json:"stream_id"`
		User         string          `json:"user"`
		Params       map[string]any  `json:"params"`
		Code         map[string]int  `json:"code"`
		Notification map[string]any  `json:"notification"`
		Result       map[string]any  `json:"result"`
	} `json:"reauthorize"`
}

// readPushFixture is mcp-events-wire.json's push section: every SDK runs
// it, so a missing section is a failure.
func readPushFixture(t *testing.T) pushFixture {
	t.Helper()
	var whole struct {
		Push *pushFixture `json:"push"`
	}
	readSharedFixture(t, "mcp-events-wire.json", &whole)
	if whole.Push == nil {
		t.Fatal("mcp-events-wire.json has no push section")
	}
	if whole.Push.Reauthorize == nil {
		t.Fatal("mcp-events-wire.json has no push.reauthorize")
	}
	return *whole.Push
}

// pushServer is the fixture's app (chat.message public, crawl.finished
// tools-only, deploy.finished allowing only u1) behind Handler, with the
// user from X-User, against a core that reports features and answers
// /events/stream with stream.
type pushServer struct {
	url  string
	core *httptest.Server
	mu   sync.Mutex
	reqs []voucherReq
	subs []map[string]any // what OnSubscribe got
	logs *bytes.Buffer
	app  *Client
	at   *Attachment
}

type pushOpts struct {
	features   []string
	push       PushOptions
	stream     http.HandlerFunc // /events/stream
	replies    map[string]voucherReply
	server     func(h http.Handler) *httptest.Server
	tools      func(ps *pushServer, at *Attachment, chat *Event[wireArgs, wireMsg], crawl *Event[crawlArgs, crawlMsg])
	stateful   bool
	jsonResult bool
	// authorize and identify, if set, replace deploy.finished's Authorize
	// and the X-User Identify; requireUser is AttachOptions.RequireUser.
	authorize   func(s Subscriber) (bool, error)
	identify    func(r Request) (Identity, error)
	requireUser bool
}

func newPushServer(t *testing.T, o pushOpts) *pushServer {
	t.Helper()
	if o.features == nil {
		o.features = []string{"subscribe_vouchers", "push"}
	}
	ps := &pushServer{logs: &bytes.Buffer{}}
	ps.core = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/apps/v1")
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		json.Unmarshal(raw, &body)
		ps.mu.Lock()
		if !strings.HasPrefix(path, "/manifest") && path != "/app" {
			ps.reqs = append(ps.reqs, voucherReq{path, body})
		}
		ps.mu.Unlock()
		if path == "/events/stream" && o.stream != nil {
			r.Body = io.NopCloser(bytes.NewReader(raw))
			o.stream(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if rep, ok := o.replies[path]; ok {
			w.WriteHeader(rep.status)
			json.NewEncoder(w).Encode(rep.body)
			return
		}
		switch path {
		case "/manifest/merge", "/manifest":
			json.NewEncoder(w).Encode(map[string]any{"features": o.features})
		case "/app":
			io.WriteString(w, `{"id":"app-test","name":"Sample"}`)
		case "/calls/addition":
			io.WriteString(w, `{"text":"Woken."}`)
		case "/event-subscriptions":
			io.WriteString(w, `{"event_subscription": {"id": "s1", "user": "u1", "created": true}, "session": "wsc_s1", "pending": "pa_1"}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	t.Cleanup(ps.core.Close)
	var logMu sync.Mutex
	ps.app = clientAt(t, ps.core.URL, WithLogger(slog.New(slog.NewJSONHandler(writerFunc(func(p []byte) (int, error) {
		logMu.Lock()
		defer logMu.Unlock()
		return ps.logs.Write(p)
	}), &slog.HandlerOptions{Level: slog.LevelDebug}))))
	onSub := func(args any, user, via, tool string) {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		ps.subs = append(ps.subs, map[string]any{"args": args, "user": user, "via": via, "tool": tool})
	}
	chat, err := DefineEvent(ps.app, "chat.message", EventDef[wireArgs, wireMsg]{Description: "A new message in a channel", Public: true,
		OnSubscribe: func(_ context.Context, s Subscribed[wireArgs]) error {
			onSub(s.Args.Channel, s.User, s.Via, s.Tool)
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	crawl, err := DefineEvent(ps.app, "crawl.finished", EventDef[crawlArgs, crawlMsg]{Description: "A crawl finished",
		OnSubscribe: func(_ context.Context, s Subscribed[crawlArgs]) error {
			onSub(s.Args.Job, s.User, s.Via, s.Tool)
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DefineEvent(ps.app, "deploy.finished", EventDef[deployArgs, deployMsg]{Description: "A deploy finished",
		Authorize: func(_ context.Context, s Subscriber, a deployArgs) (bool, error) {
			if o.authorize != nil {
				return o.authorize(s)
			}
			return s.User == "u1", nil
		}}); err != nil {
		t.Fatal(err)
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "push", Version: "1"}, nil)
	ps.at = ps.app.Attach(srv, AttachOptions{Push: o.push, RequireUser: o.requireUser, Identify: func(_ context.Context, r Request) (Identity, error) {
		if o.identify != nil {
			return o.identify(r)
		}
		return Identity{User: r.Header.Get("X-User")}, nil
	}})
	if o.tools != nil {
		o.tools(ps, ps.at, chat, crawl)
	}
	h := ps.at.Handler(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{Stateless: !o.stateful, JSONResponse: true}))
	var hs *httptest.Server
	if o.server != nil {
		hs = o.server(h)
	} else {
		hs = httptest.NewServer(h)
	}
	t.Cleanup(hs.Close)
	ps.url = hs.URL
	return ps
}

func (ps *pushServer) last(path string) map[string]any {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for i := len(ps.reqs) - 1; i >= 0; i-- {
		if ps.reqs[i].path == path {
			return ps.reqs[i].body
		}
	}
	return nil
}

func (ps *pushServer) count(path string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	n := 0
	for _, r := range ps.reqs {
		if r.path == path {
			n++
		}
	}
	return n
}

// ndjson serves frames as Wakeshift's upstream does: one per line, flushed.
func ndjson(frames []map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(200)
		for _, f := range frames {
			b, _ := json.Marshal(f)
			w.Write(append(b, '\n'))
			w.(http.Flusher).Flush()
		}
	}
}

// pushResponse is what the client got for one request: its JSON-RPC
// messages in order (SSE data lines, or one JSON body), and the HTTP status.
type pushResponse struct {
	status   int
	sse      bool
	messages []map[string]any
}

func (p pushResponse) notifications() []map[string]any {
	var out []map[string]any
	for _, m := range p.messages {
		if _, ok := m["method"]; ok {
			out = append(out, map[string]any{"method": m["method"], "params": m["params"]})
		}
	}
	return out
}

func (p pushResponse) final() (result, rpcErr map[string]any, id any) {
	for _, m := range p.messages {
		if _, ok := m["method"]; ok {
			continue
		}
		result, _ = m["result"].(map[string]any)
		rpcErr, _ = m["error"].(map[string]any)
		return result, rpcErr, m["id"]
	}
	return nil, nil, nil
}

// post sends one JSON-RPC request with id (as raw JSON) as user ("" for
// none) and reads the whole response.
func (ps *pushServer) post(t *testing.T, ctx context.Context, user, ua string, id json.RawMessage, method string, params any, header ...string) pushResponse {
	t.Helper()
	resp := ps.open(t, ctx, user, ua, id, method, params, header...)
	defer resp.Body.Close()
	return readPush(t, resp)
}

func (ps *pushServer) open(t *testing.T, ctx context.Context, user, ua string, id json.RawMessage, method string, params any, header ...string) *http.Response {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	req, _ := http.NewRequestWithContext(ctx, "POST", ps.url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("User-Agent", ua)
	if user != "" {
		req.Header.Set("X-User", user)
	}
	// A 2026-07-28 request (its _meta names the version) carries the
	// transport's headers, as its client sends them (transportRefusal).
	var p struct {
		Meta map[string]any `json:"_meta"`
	}
	if pb, err := json.Marshal(params); err == nil && json.Unmarshal(pb, &p) == nil {
		if v, ok := p.Meta["io.modelcontextprotocol/protocolVersion"].(string); ok {
			req.Header.Set("MCP-Protocol-Version", v)
			req.Header.Set("Mcp-Method", method)
		}
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func readPush(t *testing.T, resp *http.Response) pushResponse {
	t.Helper()
	out := pushResponse{status: resp.StatusCode, sse: strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")}
	if !out.sse {
		var m map[string]any
		raw, _ := io.ReadAll(resp.Body)
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("%d %s", resp.StatusCode, raw)
		}
		out.messages = []map[string]any{m}
		return out
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if d, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			var m map[string]any
			if err := json.Unmarshal([]byte(d), &m); err != nil {
				t.Fatalf("an SSE data line that isn't JSON: %q", d)
			}
			out.messages = append(out.messages, m)
		}
	}
	return out
}

// wantFinal is the final result the client should get for request id: the
// fixture's when it carries _meta, else its resultType with the
// subscription ID the SDK adds.
func wantFinal(fixture map[string]any, id any) any {
	if _, has := fixture["_meta"]; has {
		return plain(fixture)
	}
	return plain(map[string]any{"resultType": fixture["resultType"], "_meta": map[string]any{subscriptionIDMeta: id}})
}

// isFinal reports whether res is the final result for request id (raw JSON).
func isFinal(res map[string]any, id string) bool {
	var want any
	json.Unmarshal([]byte(id), &want)
	return reflect.DeepEqual(plain(res), wantFinal(map[string]any{"resultType": "complete"}, want))
}

// plain is v as plain JSON values.
func plain(v any) any {
	b, _ := json.Marshal(v)
	var out any
	json.Unmarshal(b, &out)
	return out
}

var pushUAs = map[string]string{"draft": "some-draft-client/1", "chatgpt": "openai-mcp/1.0.0"}

// mcp-events-wire.json push.streams: what each client gets for its
// events/stream, the subscription ID echoed as sent (string or number),
// and what Wakeshift is asked; on stateless and stateful go-sdk servers.
func TestPushStreamsWireGolden(t *testing.T) {
	f := readPushFixture(t)
	for _, stateful := range []bool{false, true} {
		for _, c := range f.Streams {
			ps := newPushServer(t, pushOpts{stream: ndjson(c.Frames), stateful: stateful})
			user := ""
			if c.User != nil {
				user = *c.User
			}
			got := ps.post(t, context.Background(), user, pushUAs["draft"], c.ID, "events/stream", c.Params)
			if !got.sse {
				t.Fatalf("%s: not a stream: %v", c.Name, got.messages)
			}
			if n := got.notifications(); !reflect.DeepEqual(plain(n), plain(c.Notifications)) {
				t.Errorf("%s: notifications\n got %v\nwant %v", c.Name, plain(n), plain(c.Notifications))
			}
			res, rpcErr, id := got.final()
			if rpcErr != nil || !reflect.DeepEqual(plain(res), wantFinal(c.Result, plain(c.ID))) || !reflect.DeepEqual(plain(id), plain(c.ID)) {
				t.Errorf("%s: final %v %v id %v, want %v", c.Name, res, rpcErr, id, c.Result)
			}
			asked := ps.last("/events/stream")
			call, _ := asked["call"].(map[string]any)
			delete(asked, "call")
			if !reflect.DeepEqual(plain(asked), plain(c.CoreRequest)) {
				t.Errorf("%s: asked Wakeshift\n got %v\nwant %v", c.Name, asked, c.CoreRequest)
			}
			if !reflect.DeepEqual(plain(call["user"]), plain(c.CallUser)) {
				t.Errorf("%s: call.user %v, want %v", c.Name, call["user"], c.CallUser)
			}
			if strings.Contains(ps.logs.String(), "wsv_") || strings.Contains(ps.logs.String(), "c2.") {
				t.Errorf("%s: a voucher or a cursor was logged:\n%s", c.Name, ps.logs)
			}
		}
	}
}

// SEP-3415 opens a stream one of two ways: an error, or
// notifications/events/active first. An upstream that ends before active
// is a retryable error (-32603); a terminated frame before active is the
// request's error itself; both as plain JSON, no SSE, no notification.
func TestPushEndsBeforeActiveAreErrors(t *testing.T) {
	for _, c := range []struct {
		name   string
		frames []map[string]any
		want   map[string]any
	}{
		{"close before active", []map[string]any{{"type": "close"}},
			map[string]any{"code": float64(rpcInternal), "message": "Wakeshift couldn't be reached"}},
		{"the upstream ends before active", nil,
			map[string]any{"code": float64(rpcInternal), "message": "Wakeshift couldn't be reached"}},
		{"terminated before active", []map[string]any{{"type": "terminated", "error": map[string]any{"name": "Forbidden", "message": "gone", "data": map[string]any{"reason": "revoked"}}}},
			map[string]any{"code": float64(event.CodesFor(event.DialectSpec).Forbidden), "message": "gone", "data": map[string]any{"reason": "revoked"}}},
	} {
		ps := newPushServer(t, pushOpts{stream: ndjson(c.frames)})
		got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
		res, rpcErr, _ := got.final()
		if got.sse || res != nil || len(got.notifications()) != 0 || !reflect.DeepEqual(plain(rpcErr), c.want) {
			t.Errorf("%s: sse %v, %v, want %v", c.name, got.sse, got.messages, c.want)
		}
	}
	// Once active was relayed, an ending is the final result, as before.
	ps := newPushServer(t, pushOpts{stream: ndjson([]map[string]any{
		{"type": "active", "cursor": "c2.a.0.m", "truncated": false, "heartbeat_ms": 30000}, {"type": "close"}})})
	got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
	if res, rpcErr, _ := got.final(); !got.sse || !isFinal(res, "1") || rpcErr != nil || len(got.notifications()) != 1 {
		t.Errorf("active then close: %v", got.messages)
	}
}

// push.terminated_codes: a terminated frame's error name is the client's
// dialect's code.
func TestPushTerminatedCodesGolden(t *testing.T) {
	f := readPushFixture(t)
	for _, c := range f.TerminatedCodes {
		for dialect, want := range c.Code {
			ps := newPushServer(t, pushOpts{stream: ndjson([]map[string]any{
				{"type": "active", "cursor": "c2.a.0.m", "truncated": false, "heartbeat_ms": 30000},
				{"type": "terminated", "error": map[string]any{"name": c.Name, "message": "gone"}},
			})})
			got := ps.post(t, context.Background(), "u1", pushUAs[dialect], json.RawMessage(`1`), "events/stream",
				map[string]any{"name": "chat.message", "arguments": map[string]any{"channel": "eng"}, "cursor": nil})
			n := got.notifications()
			if len(n) != 2 || n[1]["method"] != "notifications/events/terminated" {
				t.Fatalf("%s %s: %v", c.Name, dialect, n)
			}
			e := n[1]["params"].(map[string]any)["error"].(map[string]any)
			if e["code"] != float64(want) || e["message"] != "gone" {
				t.Errorf("%s %s: %v, want %d", c.Name, dialect, e, want)
			}
			if _, has := e["data"]; has {
				t.Errorf("%s %s: data %v where the frame had none", c.Name, dialect, e["data"])
			}
			if res, _, _ := got.final(); !isFinal(res, "1") {
				t.Errorf("%s %s: no final result", c.Name, dialect)
			}
		}
	}
}

// push.refused: refusals before the stream opens, the SDK's own (Wakeshift
// isn't asked) and Wakeshift's, in both dialects.
func TestPushRefusedWireGolden(t *testing.T) {
	f := readPushFixture(t)
	// No jitter here: a 503's waits only as long as its Retry-After.
	defer func(j time.Duration) { pushJitter = j }(pushJitter)
	pushJitter = 0
	for _, c := range f.Refused {
		for _, dialect := range []string{"draft", "chatgpt"} {
			o := pushOpts{stream: ndjson(nil)}
			if c.CoreStatus != 0 {
				status, coreErr, headers := c.CoreStatus, c.CoreError, c.CoreHeaders
				if c.RetryAfter != nil {
					headers = map[string]string{"Retry-After": strings.Trim(string(must(json.Marshal(c.RetryAfter))), `"`)}
				}
				o.stream = func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					for k, v := range headers {
						w.Header().Set(k, v)
					}
					w.WriteHeader(status)
					json.NewEncoder(w).Encode(map[string]any{"error": coreErr})
				}
			}
			ps := newPushServer(t, o)
			user := ""
			if c.User != nil {
				user = *c.User
			}
			var header []string
			if c.Header != "" {
				header = []string{"Mcp-Name", c.Header}
			}
			id := c.ID
			if len(id) == 0 {
				id = json.RawMessage(`5`)
			}
			start := time.Now()
			got := ps.post(t, context.Background(), user, pushUAs[dialect], id, "events/stream", c.Params, header...)
			if took := time.Since(start); took < time.Duration(c.MinMs)*time.Millisecond {
				t.Errorf("%s %s: answered after %s, want at least %d ms", c.Name, dialect, took, c.MinMs)
			}
			asked := ps.count("/events/stream")
			if c.CoreStatus == 0 && asked != 0 {
				t.Errorf("%s %s: Wakeshift was asked", c.Name, dialect)
			}
			if c.CoreStatus != 0 && asked != 1 {
				t.Errorf("%s %s: Wakeshift was asked %d times", c.Name, dialect, asked)
			}
			if len(got.notifications()) != 0 {
				t.Errorf("%s %s: notifications %v", c.Name, dialect, got.notifications())
			}
			res, rpcErr, _ := got.final()
			if c.Result != nil {
				var want any
				json.Unmarshal(id, &want)
				if rpcErr != nil || !reflect.DeepEqual(plain(res), wantFinal(c.Result, want)) {
					t.Errorf("%s %s: %v %v, want %v", c.Name, dialect, res, rpcErr, c.Result)
				}
				continue
			}
			want := map[string]any{"code": c.Error.Code[dialect], "message": c.Error.Message}
			if c.Error.Data != nil {
				want["data"] = c.Error.Data
			}
			if !reflect.DeepEqual(plain(rpcErr), plain(want)) {
				t.Errorf("%s %s: error\n got %v\nwant %v", c.Name, dialect, rpcErr, want)
			}
			if c.Header != "" && got.status != http.StatusBadRequest {
				t.Errorf("%s %s: HTTP %d, want 400", c.Name, dialect, got.status)
			}
		}
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// A 503 (the core at its stream limit) answers the client with a
// retryable error (-32603, never a result with no active before it) only
// after Retry-After (capped) and jitter; a client that leaves during the
// wait gets nothing, at once.
func TestPush503WaitsRetryAfter(t *testing.T) {
	defer func(j time.Duration) { pushJitter = j }(pushJitter)
	pushJitter = 0
	ps := newPushServer(t, pushOpts{stream: func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"code":"unavailable","message":"too many streams"}}`)
	}})
	start := time.Now()
	got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
	if res, rpcErr, _ := got.final(); res != nil || rpcErr["code"] != float64(rpcInternal) || rpcErr["message"] != streamBusy || len(got.notifications()) != 0 {
		t.Errorf("503: %v", got.messages)
	}
	if d := time.Since(start); d < time.Second || d > 3*time.Second {
		t.Errorf("answered after %s, want Retry-After's 1 s", d)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "events/stream", "params": streamParams})
	req, _ := http.NewRequestWithContext(ctx, "POST", ps.url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("X-User", "u1")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if len(raw) > 0 {
			t.Errorf("an aborted client got %s", raw)
		}
	}
	if d := time.Since(start); d > 900*time.Millisecond {
		t.Errorf("the abort took %s", d)
	}
	for h, want := range map[string]time.Duration{"": 0, "x": 0, "-3": 0, "2": 2 * time.Second, "60": 10 * time.Second} {
		if got := retryAfter(h); got != want {
			t.Errorf("Retry-After %q: %s, want %s", h, got, want)
		}
	}
}

// A refusal whose body Wakeshift didn't write (a proxy's page, a body
// without a message) never reaches the client: a fixed text instead.
func TestPushRefusalBodyNotForwarded(t *testing.T) {
	codes := event.CodesFor(event.DialectDraft)
	for _, c := range []struct {
		status int
		body   string
	}{
		{400, "<html>secret-proxy-page</html>"},
		{403, `{"error":{"code":"forbidden"}}`},
		{429, `secret-plain-text`},
		{502, `<html>secret</html>`},
	} {
		_, msg, data := pushRefusal(c.status, []byte(c.body), codes, "chat.message")
		if strings.Contains(msg, "secret") || strings.Contains(msg, "<html>") || strings.Contains(msg, "{") {
			t.Errorf("%d %s: message %q", c.status, c.body, msg)
		}
		if b, _ := json.Marshal(data); strings.Contains(string(b), "secret") {
			t.Errorf("%d %s: data %s", c.status, c.body, b)
		}
	}
	if _, msg, _ := pushRefusal(403, []byte(`{"error":{"code":"forbidden"}}`), codes, "chat.message"); !strings.Contains(msg, pushUpstreamFailed) {
		t.Errorf("a coded body without a message: %q", msg)
	}
}

// Push is listed (and served) only where the core reports it and push
// isn't off: delivery and not_served.
func TestPushListedOnlyWhereServed(t *testing.T) {
	f := readPushFixture(t)
	native := map[string]any{"_meta": modernMeta("some-draft-client", true)}
	for _, c := range []struct {
		name   string
		o      pushOpts
		served bool
	}{
		{"served", pushOpts{}, true},
		{"the core doesn't report push", pushOpts{features: []string{"subscribe_vouchers"}}, false},
		{"PushOff", pushOpts{push: PushOff}, false},
	} {
		ps := newPushServer(t, c.o)
		got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/list", native)
		res, _, _ := got.final()
		deliveries := map[string]any{}
		for _, e := range res["events"].([]any) {
			ev := e.(map[string]any)
			deliveries[ev["name"].(string)] = ev["delivery"]
		}
		wantOpen, wantTools := any(plain(event.Delivery)), any([]any{"webhook"})
		if c.served {
			wantOpen, wantTools = plain(f.Delivery), plain(f.DeliveryToolsOnly)
		}
		if !reflect.DeepEqual(deliveries["chat.message"], wantOpen) || !reflect.DeepEqual(deliveries["crawl.finished"], wantTools) {
			t.Errorf("%s: delivery %v", c.name, deliveries)
		}
		for _, dialect := range []string{"draft", "chatgpt"} {
			got := ps.post(t, context.Background(), "u1", pushUAs[dialect], json.RawMessage(`2`), "events/stream",
				map[string]any{"name": "chat.message", "arguments": map[string]any{"channel": "eng"}, "cursor": nil})
			_, rpcErr, _ := got.final()
			if c.served {
				// Relayed: this fake core's upstream ends before active,
				// which is answered as -32603 (never Unsupported).
				if rpcErr == nil || rpcErr["code"] != float64(rpcInternal) || ps.count("/events/stream") == 0 {
					t.Errorf("%s %s: not relayed: %v", c.name, dialect, got.messages)
				}
				continue
			}
			want := map[string]any{"code": f.NotServed.Code[dialect], "message": f.NotServed.Message, "data": f.NotServed.Data}
			if !reflect.DeepEqual(plain(rpcErr), plain(want)) {
				t.Errorf("%s %s: %v, want %v", c.name, dialect, rpcErr, want)
			}
		}
		if !c.served && ps.count("/events/stream") != 0 {
			t.Errorf("%s: Wakeshift was asked for a stream", c.name)
		}
	}
}

// streamParams is a chat.message stream from null.
var streamParams = map[string]any{"name": "chat.message", "arguments": map[string]any{"channel": "eng"}, "cursor": nil}

// The subscription ID is the request's ID as sent, a string or a number,
// on every notification.
func TestPushSubscriptionIDEcho(t *testing.T) {
	frames := []map[string]any{
		{"type": "active", "cursor": "c2.a.0.m", "truncated": false, "heartbeat_ms": 30000},
		{"type": "event", "occurrence": map[string]any{"eventId": "e1", "name": "chat.message", "timestamp": "2026-11-12T00:00:00Z", "data": map[string]any{}, "cursor": "c2.a.1.m"}},
		{"type": "heartbeat", "cursor": "c2.a.1.m"},
		{"type": "close"},
	}
	for _, id := range []string{`"sub-1"`, `42`, `0`} {
		ps := newPushServer(t, pushOpts{stream: ndjson(frames)})
		got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(id), "events/stream", streamParams)
		var want any
		json.Unmarshal([]byte(id), &want)
		n := got.notifications()
		if len(n) != 3 {
			t.Fatalf("%s: %v", id, n)
		}
		for _, m := range n {
			meta := m["params"].(map[string]any)["_meta"].(map[string]any)
			if !reflect.DeepEqual(meta[subscriptionIDMeta], want) {
				t.Errorf("%s: %s's subscription ID %v", id, m["method"], meta[subscriptionIDMeta])
			}
		}
		if res, _, gotID := got.final(); !reflect.DeepEqual(gotID, want) || !isFinal(res, id) {
			t.Errorf("%s: final result %v, id %v", id, res, gotID)
		}
	}
}

// heldStream is an upstream that writes active, then holds the stream
// open until its request ends, reporting that on ended.
func heldStream(hb int, ended chan<- struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(200)
		b, _ := json.Marshal(map[string]any{"type": "active", "cursor": "c2.a.0.m", "truncated": false, "heartbeat_ms": hb})
		w.Write(append(b, '\n'))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		if ended != nil {
			ended <- struct{}{}
		}
	}
}

// The client dropping its request aborts the upstream, and nothing more
// is written for it.
func TestPushClientDisconnectAbortsUpstream(t *testing.T) {
	ended := make(chan struct{}, 1)
	ps := newPushServer(t, pushOpts{stream: heldStream(30000, ended)})
	ctx, cancel := context.WithCancel(context.Background())
	resp := ps.open(t, ctx, "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line, "notifications/events/active") {
			break
		}
	}
	cancel()
	resp.Body.Close()
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the upstream wasn't aborted")
	}
	if ps.count("/events/stream/end") != 0 {
		t.Error("an HTTP abort ended the subscription")
	}
}

// An upstream silent for 3 x the heartbeat grant ends the client's stream
// with the final result (logged); MaxStream ends it too, and so does the
// client's Close.
func TestPushStreamEnds(t *testing.T) {
	for _, c := range []struct {
		name string
		hb   int
		o    PushOptions
		end  func(ps *pushServer)
		log  string
	}{
		{name: "silent", hb: 20, log: `"reason":"silent"`},
		{name: "max stream", hb: 30000, o: PushOptions{MaxStream: 100 * time.Millisecond}},
		{name: "close", hb: 30000, end: func(ps *pushServer) {
			time.Sleep(100 * time.Millisecond)
			ps.app.Close()
		}},
	} {
		ps := newPushServer(t, pushOpts{stream: heldStream(c.hb, nil), push: c.o})
		if c.end != nil {
			go c.end(ps)
		}
		start := time.Now()
		got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
		if res, _, _ := got.final(); !isFinal(res, "1") || len(got.notifications()) != 1 {
			t.Errorf("%s: %v", c.name, got.messages)
		}
		if time.Since(start) > 5*time.Second {
			t.Errorf("%s: took %s", c.name, time.Since(start))
		}
		if c.log != "" && !strings.Contains(ps.logs.String(), c.log) {
			t.Errorf("%s: not logged:\n%s", c.name, ps.logs)
		}
	}
}

// A stream outlives the server's WriteTimeout: Handler clears it for the
// stream's response.
func TestPushClearsWriteTimeout(t *testing.T) {
	ps := newPushServer(t, pushOpts{
		stream: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(200)
			for _, f := range []string{`{"type":"active","cursor":"c2.a.0.m","truncated":false,"heartbeat_ms":30000}`, `{"type":"heartbeat","cursor":"c2.a.0.m"}`, `{"type":"close"}`} {
				w.Write([]byte(f + "\n"))
				w.(http.Flusher).Flush()
				time.Sleep(150 * time.Millisecond)
			}
		},
		server: func(h http.Handler) *httptest.Server {
			s := httptest.NewUnstartedServer(h)
			s.Config.WriteTimeout = 100 * time.Millisecond
			s.Start()
			return s
		},
	})
	got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
	if res, _, _ := got.final(); !isFinal(res, "1") || len(got.notifications()) != 2 {
		t.Errorf("cut off: %v", got.messages)
	}
}

// The heartbeat asked for: the smaller of the client's suggestion and
// MaxHeartbeat, or MaxHeartbeat alone; the upstream isn't cut by the app's
// client Timeout.
func TestPushMaxHeartbeat(t *testing.T) {
	for _, c := range []struct {
		suggest any
		max     time.Duration
		want    any
	}{
		{nil, 0, nil},
		{float64(40000), 0, float64(40000)},
		{float64(40000), 15 * time.Second, float64(15000)},
		{float64(12000), 15 * time.Second, float64(12000)},
		{nil, 15 * time.Second, float64(15000)},
	} {
		ps := newPushServer(t, pushOpts{stream: ndjson([]map[string]any{{"type": "close"}}), push: PushOptions{MaxHeartbeat: c.max}})
		p := map[string]any{"name": "chat.message", "arguments": map[string]any{"channel": "eng"}, "cursor": nil}
		if c.suggest != nil {
			p["heartbeatMs"] = c.suggest
		}
		ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", p)
		if got := ps.last("/events/stream")["heartbeat_ms"]; !reflect.DeepEqual(got, c.want) {
			t.Errorf("%v, max %s: asked %v, want %v", c.suggest, c.max, got, c.want)
		}
	}
}

// A stream whose upstream reads longer than the app's client Timeout
// isn't cut off by it (the upstream's client has none).
func TestPushUpstreamOutlivesClientTimeout(t *testing.T) {
	ps := &pushServer{}
	var app *Client
	core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, "/api/apps/v1") {
		case "/events/stream":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(200)
			w.Write([]byte(`{"type":"active","cursor":"c2.a.0.m","truncated":false,"heartbeat_ms":30000}` + "\n"))
			w.(http.Flusher).Flush()
			time.Sleep(300 * time.Millisecond)
			w.Write([]byte(`{"type":"close"}` + "\n"))
		default:
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"features":["push"]}`)
		}
	})
	app, _ = testApp(t, core, WithHTTPClient(&http.Client{Timeout: 100 * time.Millisecond, Transport: &http.Transport{}}))
	if _, err := DefineEvent(app, "chat.message", EventDef[wireArgs, wireMsg]{Description: "A new message", Public: true}); err != nil {
		t.Fatal(err)
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "push", Version: "1"}, nil)
	at := app.Attach(srv, AttachOptions{Identify: func(context.Context, Request) (Identity, error) { return Identity{User: "u1"}, nil }})
	hs := httptest.NewServer(at.Handler(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, &mcp.StreamableHTTPOptions{Stateless: true})))
	t.Cleanup(hs.Close)
	ps.url = hs.URL
	got := ps.post(t, context.Background(), "", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
	if res, _, _ := got.final(); !isFinal(res, "1") || len(got.notifications()) != 1 {
		t.Errorf("cut off: %v", got.messages)
	}
}

// A stream that created its subscription runs OnSubscribe: via native, or
// a redeemed voucher's as the tool's; created, via and tool never reach
// the client.
func TestPushCreatedRunsOnSubscribe(t *testing.T) {
	ps := newPushServer(t, pushOpts{stream: ndjson([]map[string]any{
		{"type": "active", "cursor": "c2.a.0.m", "truncated": false, "heartbeat_ms": 30000, "created": true},
		{"type": "close"},
	})})
	got := ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
	ps2 := newPushServer(t, pushOpts{stream: ndjson([]map[string]any{
		{"type": "active", "cursor": "c2.b.0.m", "truncated": false, "heartbeat_ms": 30000, "created": true, "via": "tool:start_crawl", "tool": "start_crawl"},
		{"type": "close"},
	})})
	ps2.post(t, context.Background(), "", pushUAs["draft"], json.RawMessage(`1`), "events/stream",
		map[string]any{"name": "crawl.finished", "arguments": map[string]any{"job": "j1", "wakeshift_voucher": "wsv_fakefakefakefakefakefakefakefake"}, "cursor": nil})
	time.Sleep(100 * time.Millisecond)
	params := got.notifications()[0]["params"].(map[string]any)
	for _, k := range []string{"created", "via", "tool"} {
		if _, has := params[k]; has {
			t.Errorf("active carries %s", k)
		}
	}
	ps.mu.Lock()
	ps2.mu.Lock()
	defer ps.mu.Unlock()
	defer ps2.mu.Unlock()
	if want := []map[string]any{{"args": "eng", "user": "u1", "via": "native", "tool": ""}}; !reflect.DeepEqual(ps.subs, want) {
		t.Errorf("native: %v", ps.subs)
	}
	if want := []map[string]any{{"args": "j1", "user": "", "via": "tool", "tool": "start_crawl"}}; !reflect.DeepEqual(ps2.subs, want) {
		t.Errorf("voucher: %v", ps2.subs)
	}
}

// Concurrency: open streams don't block tools/call or other requests.
func TestPushStreamsDontBlock(t *testing.T) {
	ps := newPushServer(t, pushOpts{stream: heldStream(30000, nil)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 50 {
		resp := ps.open(t, ctx, "u1", pushUAs["draft"], json.RawMessage(`1`), "events/stream", streamParams)
		defer resp.Body.Close()
	}
	done := make(chan pushResponse, 1)
	go func() {
		done <- ps.post(t, context.Background(), "u1", pushUAs["draft"], json.RawMessage(`9`), "tools/list", map[string]any{})
	}()
	select {
	case got := <-done:
		if res, _, _ := got.final(); res == nil {
			t.Errorf("tools/list: %v", got.messages)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("tools/list blocked behind open streams")
	}
}

// opencode: where push is served and it declares MCP Events, it's
// native (no subscribe_events), and its call.Subscribe asks the core with
// events_ext; native_line gives the native line, and without it the
// bridged block stays (the mixed state). Where push isn't served it stays
// bridged: subscribe_events listed, no events_ext.
func TestPushOpencodeD3(t *testing.T) {
	opencodeMeta := modernMeta("opencode", true)
	for _, c := range []struct {
		name       string
		features   []string
		nativeLine bool
		native     bool
	}{
		{"push served, native_line", []string{"subscribe_vouchers", "push"}, true, true},
		{"push served, downgraded (mixed state)", []string{"subscribe_vouchers", "push"}, false, true},
		{"push not served", []string{"subscribe_vouchers"}, false, false},
		{"PushOff, the core reports push", []string{"subscribe_vouchers", "push"}, false, false},
	} {
		reply := map[string]any{"event_subscription": map[string]any{"id": "s1", "created": true}, "session": "wsc_s1", "pending": "pa_1"}
		if c.nativeLine {
			reply["native_line"] = true
		}
		var push PushOptions
		if strings.HasPrefix(c.name, "PushOff") {
			push = PushOff
			reply["native_line"] = true // even so, no native line: push isn't served here
		}
		ps := newPushServer(t, pushOpts{features: c.features, push: push, replies: map[string]voucherReply{"/event-subscriptions": {200, reply}},
			tools: func(ps *pushServer, at *Attachment, chat *Event[wireArgs, wireMsg], crawl *Event[crawlArgs, crawlMsg]) {
				AddTool(at, &mcp.Tool{Name: "start_crawl"}, func(ctx context.Context, call *ToolCall, in crawlArgs) (*mcp.CallToolResult, any, error) {
					if _, err := crawl.Subscribe(ctx, call, in, Context("ctx-1")); err != nil {
						return nil, nil, err
					}
					return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "started"}}}, nil, nil
				}, Subscribes(crawl))
			}})
		list := ps.post(t, context.Background(), "u1", "opencode/1.0", json.RawMessage(`1`), "tools/list", map[string]any{"_meta": opencodeMeta},
			"MCP-Protocol-Version", "2026-07-28", "Mcp-Method", "tools/list")
		res, _, _ := list.final()
		if res == nil {
			t.Fatalf("%s: tools/list: %d %v", c.name, list.status, list.messages)
		}
		var names []string
		for _, tl := range res["tools"].([]any) {
			names = append(names, tl.(map[string]any)["name"].(string))
		}
		if slices.Contains(names, "subscribe_events") == c.native {
			t.Errorf("%s: tools %v", c.name, names)
		}
		ps.post(t, context.Background(), "u1", "opencode/1.0", json.RawMessage(`2`), "tools/call",
			map[string]any{"name": "start_crawl", "arguments": map[string]any{"job": "j1"}, "_meta": opencodeMeta},
			"MCP-Protocol-Version", "2026-07-28", "Mcp-Method", "tools/call", "Mcp-Name", "start_crawl")
		sub := ps.last("/event-subscriptions")
		if sub == nil {
			t.Fatalf("%s: no bridged subscribe", c.name)
		}
		call := sub["call"].(map[string]any)
		if (call["events_ext"] == true) != c.native {
			t.Errorf("%s: events_ext %v", c.name, call["events_ext"])
		}
		add := ps.last("/calls/addition")
		native, _ := add["native"].([]any)
		if _, has := call["transport_session"]; has && !c.native {
			t.Errorf("%s: transport_session sent", c.name)
		}
		if (len(native) > 0) != c.nativeLine {
			t.Errorf("%s: native lines %v", c.name, native)
		}
		if c.nativeLine && native[0].(map[string]any)["name"] != "crawl.finished" {
			t.Errorf("%s: native line %v", c.name, native[0])
		}
	}
}

// opencode on the legacy protocol (its default config) declares MCP Events
// only in initialize, never in a request's _meta: where push is served,
// its session's tools/list leaves out subscribe_events as the 2026-07-28
// one does, and its call.Subscribe asks with events_ext. Without the
// declaration, or where push isn't served, subscribe_events stays.
func TestPushOpencodeLegacyInitialize(t *testing.T) {
	for _, c := range []struct {
		name     string
		features []string
		declares bool
		native   bool
	}{
		{"declared in initialize, push served", []string{"subscribe_vouchers", "push"}, true, true},
		{"not declared", []string{"subscribe_vouchers", "push"}, false, false},
		{"declared, push not served", []string{"subscribe_vouchers"}, true, false},
	} {
		ps := newPushServer(t, pushOpts{features: c.features, stateful: true,
			tools: func(ps *pushServer, at *Attachment, chat *Event[wireArgs, wireMsg], crawl *Event[crawlArgs, crawlMsg]) {
				AddTool(at, &mcp.Tool{Name: "start_crawl"}, func(ctx context.Context, call *ToolCall, in crawlArgs) (*mcp.CallToolResult, any, error) {
					if _, err := crawl.Subscribe(ctx, call, in); err != nil {
						return nil, nil, err
					}
					return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "started"}}}, nil, nil
				}, Subscribes(crawl))
			}})
		caps := map[string]any{}
		if c.declares {
			caps["extensions"] = map[string]any{"io.modelcontextprotocol/events": map[string]any{}}
		}
		resp := ps.open(t, context.Background(), "u1", "opencode/1.0", json.RawMessage(`0`), "initialize",
			map[string]any{"protocolVersion": "2025-11-25", "capabilities": caps, "clientInfo": map[string]any{"name": "opencode", "version": "2.2.7"}})
		sid := resp.Header.Get("Mcp-Session-Id")
		readPush(t, resp)
		resp.Body.Close()
		if sid == "" {
			t.Fatalf("%s: no session", c.name)
		}
		hdr := []string{"Mcp-Session-Id", sid, "MCP-Protocol-Version", "2025-11-25"}
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
		req, _ := http.NewRequest("POST", ps.url, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Mcp-Session-Id", sid)
		if r, err := http.DefaultClient.Do(req); err == nil {
			r.Body.Close()
		}
		list := ps.post(t, context.Background(), "u1", "opencode/1.0", json.RawMessage(`1`), "tools/list", map[string]any{}, hdr...)
		res, _, _ := list.final()
		if res == nil {
			t.Fatalf("%s: tools/list: %d %v", c.name, list.status, list.messages)
		}
		var names []string
		for _, tl := range res["tools"].([]any) {
			names = append(names, tl.(map[string]any)["name"].(string))
		}
		if slices.Contains(names, "subscribe_events") == c.native {
			t.Errorf("%s: tools %v", c.name, names)
		}
		ps.post(t, context.Background(), "u1", "opencode/1.0", json.RawMessage(`2`), "tools/call",
			map[string]any{"name": "start_crawl", "arguments": map[string]any{"job": "j1"}}, hdr...)
		sub := ps.last("/event-subscriptions")
		if sub == nil {
			t.Fatalf("%s: no bridged subscribe", c.name)
		}
		if call := sub["call"].(map[string]any); (call["events_ext"] == true) != c.native {
			t.Errorf("%s: events_ext %v", c.name, call["events_ext"])
		}
	}
}

// An open stream asks again every Reauthorize: Authorize refusing (or
// Identify no longer giving the stream's user) ends it as a revocation
// does, terminated (Forbidden, the open-time words) then the final result,
// with the upstream aborted; allowing or failing keeps it.
func TestPushReauthorize(t *testing.T) {
	tick := make(chan time.Time)
	asked := make(chan time.Duration, 16)
	defer func(f func(time.Duration) <-chan time.Time) { reauthorizeAfter = f }(reauthorizeAfter)
	reauthorizeAfter = func(d time.Duration) <-chan time.Time {
		asked <- d
		return tick
	}
	type answer struct {
		ok   bool
		err  error
		user string
	}
	for _, c := range []struct {
		name    string
		answers []answer // per recheck, in order; the last one decides
		ends    bool
		event   string
		require bool
	}{
		{name: "authorize refuses", answers: []answer{{ok: false, user: "u1"}}, ends: true, event: "deploy.finished"},
		{name: "authorize allows, then refuses", answers: []answer{{ok: true, user: "u1"}, {ok: false, user: "u1"}}, ends: true, event: "deploy.finished"},
		{name: "authorize fails: kept, then refuses", answers: []answer{{err: errors.New("boom"), user: "u1"}, {ok: false, user: "u1"}}, ends: true, event: "deploy.finished"},
		{name: "identify gives another user", answers: []answer{{ok: true, user: "u2"}}, ends: true, event: "deploy.finished"},
		{name: "identify gives no user (RequireUser)", answers: []answer{{ok: true, user: ""}}, ends: true, event: "chat.message", require: true},
		{name: "no Authorize, no RequireUser: identify switches the user", answers: []answer{{ok: true, user: "u2"}}, ends: true, event: "chat.message"},
		{name: "no Authorize, no RequireUser: the same user", answers: []answer{{ok: true, user: "u1"}}, ends: false, event: "chat.message"},
		{name: "allowed", answers: []answer{{ok: true, user: "u1"}}, ends: false, event: "deploy.finished"},
	} {
		var mu sync.Mutex
		n := 0
		cur := func() answer {
			mu.Lock()
			defer mu.Unlock()
			if n == 0 {
				return answer{ok: true} // the open
			}
			return c.answers[min(n-1, len(c.answers)-1)]
		}
		ended := make(chan struct{}, 1)
		ps := newPushServer(t, pushOpts{stream: heldStream(30000, ended), requireUser: c.require,
			authorize: func(s Subscriber) (bool, error) {
				a := cur()
				if s.User != "u1" {
					return false, nil
				}
				return a.ok, a.err
			},
			identify: func(r Request) (Identity, error) {
				mu.Lock()
				defer mu.Unlock()
				if n == 0 {
					return Identity{User: "u1"}, nil // the open
				}
				return Identity{User: c.answers[min(n-1, len(c.answers)-1)].user}, nil
			}})
		args := map[string]any{"env": "prod"}
		if c.event == "chat.message" {
			args = map[string]any{"channel": "eng"}
		}
		ctx, cancel := context.WithCancel(context.Background())
		resp := ps.open(t, ctx, "u1", pushUAs["draft"], json.RawMessage(`7`), "events/stream", map[string]any{"name": c.event, "arguments": args, "cursor": nil})
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
		if m := <-msgs; m["method"] != "notifications/events/active" {
			t.Fatalf("%s: first %v", c.name, m)
		}
		if d := <-asked; d != defaultReauthorize {
			t.Errorf("%s: rechecks every %s", c.name, d)
		}
		for i := range c.answers {
			mu.Lock()
			n = i + 1
			mu.Unlock()
			tick <- time.Now()
			if i < len(c.answers)-1 || !c.ends {
				// Kept: the next recheck is armed.
				select {
				case <-asked:
				case m := <-msgs:
					t.Fatalf("%s: recheck %d ended it: %v", c.name, i, m)
				case <-time.After(5 * time.Second):
					t.Fatalf("%s: recheck %d armed no next one", c.name, i)
				}
			}
		}
		if c.ends {
			term := <-msgs
			params, _ := term["params"].(map[string]any)
			e, _ := params["error"].(map[string]any)
			meta, _ := params["_meta"].(map[string]any)
			if term["method"] != "notifications/events/terminated" || e["code"] != float64(-32024) ||
				e["message"] != fill("native_refused", "{event}", c.event) || meta[subscriptionIDMeta] != float64(7) ||
				!reflect.DeepEqual(e["data"], map[string]any{"reason": "revoked"}) {
				t.Errorf("%s: %v", c.name, term)
			}
			if final := <-msgs; final["id"] != float64(7) || !isFinal(final["result"].(map[string]any), "7") {
				t.Errorf("%s: final %v", c.name, final)
			}
			select {
			case <-ended:
			case <-time.After(5 * time.Second):
				t.Errorf("%s: the upstream wasn't aborted", c.name)
			}
			if ps.count("/events/stream/end") != 0 {
				t.Errorf("%s: called /events/stream/end", c.name)
			}
		}
		cancel()
		resp.Body.Close()
		for range msgs {
		}
	}
}

// mcp-events-wire.json push.reauthorize: the interval (default, clamp; Go's
// invalid values are the negative durations: 0 is the zero value, the
// default, and fractional or string values can't be expressed), and a
// stream whose Authorize refuses at the first check, in both dialects.
func TestPushReauthorizeGolden(t *testing.T) {
	g := readPushFixture(t).Reauthorize
	ms := func(n int64) time.Duration { return time.Duration(n) * time.Millisecond }
	if defaultReauthorize != ms(g.DefaultMs) || minReauthorize != ms(g.MinMs) || maxReauthorize != ms(g.MaxMs) {
		t.Errorf("default %s, bounds [%s, %s]", defaultReauthorize, minReauthorize, maxReauthorize)
	}
	for _, c := range g.Clamp {
		if got, _ := pushConfig(PushOptions{Reauthorize: ms(c.ReauthorizeMs)}, func(string) string { return "" }); got.Reauthorize != ms(c.WantMs) {
			t.Errorf("clamp %d: %s", c.ReauthorizeMs, got.Reauthorize)
		}
	}
	for _, v := range g.Invalid {
		n, ok := v.(float64)
		if !ok || n != float64(int64(n)) || n == 0 {
			// Skipped: 0 is Go's zero value, which means the default (5
			// minutes), not a mistake; 1.5 and "5m" can't be expressed as a
			// time.Duration of whole milliseconds.
			continue
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("invalid %v accepted", v)
				}
			}()
			clientAt(t, "https://core.test").pushOptionsOf(PushOptions{Reauthorize: ms(int64(n))})
		}()
	}
	tick := make(chan time.Time)
	asked := make(chan time.Duration, 4)
	defer func(f func(time.Duration) <-chan time.Time) { reauthorizeAfter = f }(reauthorizeAfter)
	reauthorizeAfter = func(d time.Duration) <-chan time.Time {
		asked <- d
		return tick
	}
	for dialect, code := range g.Code {
		var mu sync.Mutex
		checks := 0
		ended := make(chan struct{}, 1)
		ps := newPushServer(t, pushOpts{stream: heldStream(30000, ended), authorize: func(s Subscriber) (bool, error) {
			mu.Lock()
			defer mu.Unlock()
			checks++
			return s.User == g.User && checks == 1, nil // allowed at open, refused at the first check
		}})
		ctx, cancel := context.WithCancel(context.Background())
		resp := ps.open(t, ctx, g.User, pushUAs[dialect], g.StreamID, "events/stream", g.Params)
		br := bufio.NewScanner(resp.Body)
		var msgs []map[string]any
		read := func() map[string]any {
			for br.Scan() {
				if d, ok := strings.CutPrefix(br.Text(), "data: "); ok {
					var m map[string]any
					json.Unmarshal([]byte(d), &m)
					msgs = append(msgs, m)
					return m
				}
			}
			t.Fatalf("%s: the stream ended early: %v", dialect, msgs)
			return nil
		}
		read() // active
		if d := <-asked; d != ms(g.DefaultMs) {
			t.Errorf("%s: checks every %s", dialect, d)
		}
		tick <- time.Now()
		want := plain(g.Notification).(map[string]any)
		want["params"].(map[string]any)["error"].(map[string]any)["code"] = float64(code)
		if got := read(); !reflect.DeepEqual(plain(map[string]any{"method": got["method"], "params": got["params"]}), want) {
			t.Errorf("%s: notification\n got %v\nwant %v", dialect, got, want)
		}
		if got := read(); !reflect.DeepEqual(plain(got["result"]), wantFinal(g.Result, plain(g.StreamID))) || !reflect.DeepEqual(plain(got["id"]), plain(g.StreamID)) {
			t.Errorf("%s: result %v", dialect, got)
		}
		select {
		case <-ended:
		case <-time.After(5 * time.Second):
			t.Errorf("%s: the upstream wasn't aborted", dialect)
		}
		if ps.count("/events/stream/end") != 0 {
			t.Errorf("%s: called /events/stream/end", dialect)
		}
		cancel()
		resp.Body.Close()
	}
}

// pushConfig: MaxStream defaults by the host it runs on, unless set or off.
func TestPushConfig(t *testing.T) {
	env := func(k string) func(string) string {
		return func(n string) string {
			if n == k {
				return "1"
			}
			return ""
		}
	}
	for _, c := range []struct {
		opts PushOptions
		env  string
		want time.Duration
	}{
		{PushOptions{}, "", 0},
		{PushOptions{}, "VERCEL", 280 * time.Second},
		{PushOptions{}, "AWS_LAMBDA_FUNCTION_NAME", 14 * time.Minute},
		{PushOptions{}, "K_SERVICE", 55 * time.Minute},
		{PushOptions{MaxStream: time.Minute}, "VERCEL", time.Minute},
		{PushOff, "VERCEL", 0},
	} {
		if got, _ := pushConfig(c.opts, env(c.env)); got.MaxStream != c.want {
			t.Errorf("%+v on %q: %s, want %s", c.opts, c.env, got.MaxStream, c.want)
		}
	}
	for _, c := range []struct{ in, want time.Duration }{
		{0, 5 * time.Minute}, {time.Second, time.Minute}, {10 * time.Minute, 10 * time.Minute}, {2 * time.Hour, 50 * time.Minute},
	} {
		if got, _ := pushConfig(PushOptions{Reauthorize: c.in}, env("")); got.Reauthorize != c.want {
			t.Errorf("Reauthorize %s: %s, want %s", c.in, got.Reauthorize, c.want)
		}
	}
	for _, o := range []PushOptions{{Reauthorize: -time.Second}, {MaxStream: -1}, {MaxHeartbeat: -1}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%+v: not refused", o)
				}
			}()
			clientAt(t, "https://core.test").pushOptionsOf(o)
		}()
	}
	if !pushClient("OpenCode", "", nil) || !pushClient("", "opencode/1.2", nil) || pushClient("claude-code", "", nil) {
		t.Error("pushClient")
	}
	// The rule: opencode is bridged unless push is served, then decided by what it declares.
	meta := modernMeta("opencode", true)
	if nativeClientRule("opencode", "", meta, nil, false) || !nativeClientRule("opencode", "", meta, nil, true) ||
		nativeClientRule("opencode", "", modernMeta("opencode", false), nil, true) || nativeClientRule("claude-code", "", modernMeta("claude-code", true), nil, true) {
		t.Error("nativeClientRule with servesPush")
	}
}

// maxUncheckedFor: 15 minutes, or three Reauthorize intervals if longer.
func TestMaxUncheckedFor(t *testing.T) {
	for in, want := range map[time.Duration]time.Duration{
		defaultReauthorize: 15 * time.Minute, // three failed rechecks
		minReauthorize:     15 * time.Minute,
		10 * time.Minute:   30 * time.Minute,
	} {
		if got := maxUncheckedFor(in); got != want {
			t.Errorf("maxUncheckedFor(%s) = %s, want %s", in, got, want)
		}
	}
}

// Identify or Authorize failing (an error, not a refusal) at every recheck
// keeps the stream until its last successful check (the open's, at first)
// is maxUncheckedFor old: then it ends with the final result and no
// terminated, the upstream aborted and the subscription kept, for the
// client to reconnect with its cursor. A successful recheck resets the
// bound; a refusal still ends the stream as a revocation. Each failed
// recheck and the end log the event's name only: never the arguments, the
// cursor or the error's message.
func TestPushRecheckBound(t *testing.T) {
	tick := make(chan time.Time)
	asked := make(chan time.Duration, 16)
	defer func(f func(time.Duration) <-chan time.Time) { reauthorizeAfter = f }(reauthorizeAfter)
	reauthorizeAfter = func(d time.Duration) <-chan time.Time {
		asked <- d
		return tick
	}
	canary := errors.New("secret-canary")
	for _, c := range []struct {
		name    string
		every   time.Duration
		answers []string // per recheck: ok, refuse, authorize_fails, identify_fails
		end     string   // "unchecked", "revoked"
	}{
		{name: "authorize fails", answers: []string{"authorize_fails", "authorize_fails", "authorize_fails"}, end: "unchecked"},
		{name: "identify fails", answers: []string{"identify_fails", "identify_fails", "identify_fails"}, end: "unchecked"},
		{name: "a success resets the bound", answers: []string{"authorize_fails", "authorize_fails", "ok", "identify_fails", "identify_fails", "identify_fails"}, end: "unchecked"},
		{name: "failing, then refused", answers: []string{"authorize_fails", "refuse"}, end: "revoked"},
		{name: "a longer interval: three failed rechecks", every: 10 * time.Minute, answers: []string{"authorize_fails", "authorize_fails", "authorize_fails"}, end: "unchecked"},
	} {
		var mu sync.Mutex
		n := 0
		cur := func() string {
			mu.Lock()
			defer mu.Unlock()
			if n == 0 {
				return "ok" // the open
			}
			return c.answers[n-1]
		}
		ended := make(chan struct{}, 1)
		ps := newPushServer(t, pushOpts{stream: heldStream(30000, ended), push: PushOptions{Reauthorize: c.every},
			authorize: func(s Subscriber) (bool, error) {
				switch cur() {
				case "authorize_fails":
					return false, canary
				case "refuse":
					return false, nil
				}
				return true, nil
			},
			identify: func(r Request) (Identity, error) {
				if cur() == "identify_fails" {
					return Identity{}, canary
				}
				return Identity{User: "u1"}, nil
			}})
		var clockMu sync.Mutex
		now := time.Now()
		ps.app.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
		every := c.every
		if every == 0 {
			every = defaultReauthorize
		}
		ctx, cancel := context.WithCancel(context.Background())
		resp := ps.open(t, ctx, "u1", pushUAs["draft"], json.RawMessage(`7`), "events/stream",
			map[string]any{"name": "deploy.finished", "arguments": map[string]any{"env": "env-canary"}, "cursor": "c2.cursor-canary.0.m"})
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
		if m := <-msgs; m["method"] != "notifications/events/active" {
			t.Fatalf("%s: first %v", c.name, m)
		}
		if d := <-asked; d != every {
			t.Errorf("%s: rechecks every %s", c.name, d)
		}
		for i := range c.answers {
			mu.Lock()
			n = i + 1
			mu.Unlock()
			clockMu.Lock()
			now = now.Add(every)
			clockMu.Unlock()
			tick <- now
			if i < len(c.answers)-1 {
				// Kept: the next recheck is armed.
				select {
				case <-asked:
				case m := <-msgs:
					t.Fatalf("%s: recheck %d ended it: %v", c.name, i, m)
				case <-time.After(5 * time.Second):
					t.Fatalf("%s: recheck %d armed no next one", c.name, i)
				}
			}
		}
		last := <-msgs
		if c.end == "revoked" {
			params, _ := last["params"].(map[string]any)
			e, _ := params["error"].(map[string]any)
			if last["method"] != "notifications/events/terminated" || !reflect.DeepEqual(e["data"], map[string]any{"reason": "revoked"}) {
				t.Errorf("%s: %v", c.name, last)
			}
			last = <-msgs
		}
		if last["id"] != float64(7) || !isFinal(last["result"].(map[string]any), "7") {
			t.Errorf("%s: want the final result (no terminated), got %v", c.name, last)
		}
		select {
		case <-ended:
		case <-time.After(5 * time.Second):
			t.Errorf("%s: the upstream wasn't aborted", c.name)
		}
		if ps.count("/events/stream/end") != 0 {
			t.Errorf("%s: called /events/stream/end", c.name)
		}
		cancel()
		resp.Body.Close()
		for range msgs {
		}
		logs := ps.logs.String()
		if strings.Contains(logs, "canary") {
			t.Errorf("%s: a log carries an argument, the cursor or the error's message:\n%s", c.name, logs)
		}
		var failed, stale int
		for _, l := range strings.Split(strings.TrimSpace(logs), "\n") {
			var m map[string]any
			json.Unmarshal([]byte(l), &m)
			code, _ := m["code"].(string)
			if !strings.HasPrefix(code, "push_recheck") {
				continue
			}
			keys := slices.Sorted(maps.Keys(m))
			if !slices.Equal(keys, []string{"code", "count", "event", "level", "msg", "time"}) || m["event"] != "deploy.finished" || m["level"] != "WARN" {
				t.Errorf("%s: %v", c.name, m)
			}
			if code == "push_recheck_failed" {
				failed++
			} else {
				stale++
			}
		}
		wantFailed, wantStale := 0, 0
		for _, a := range c.answers {
			if strings.HasSuffix(a, "_fails") {
				wantFailed++
			}
		}
		if c.end == "unchecked" {
			wantStale = 1
		}
		if failed != wantFailed || stale != wantStale {
			t.Errorf("%s: logged %d push_recheck_failed and %d push_recheck_stale, want %d and %d:\n%s", c.name, failed, stale, wantFailed, wantStale, logs)
		}
	}
}

// A stream's open (as every reconnect is) whose Identify or Authorize
// failed answers a retryable Internal error (-32603), never Forbidden, so
// the client retries once the app's check answers, logging
// push_open_check_failed with the event's name only; a refusal is still
// Forbidden. No upstream opens either way.
func TestPushOpenCheckFailed(t *testing.T) {
	canary := errors.New("secret-canary")
	forbidden := map[string]float64{"draft": -32024, "chatgpt": -32012}
	for dialect, ua := range pushUAs {
		for _, how := range []string{"identify_fails", "authorize_fails", "authorize_refuses", "identify_no_user"} {
			name := dialect + " " + how
			ps := newPushServer(t, pushOpts{
				stream: func(w http.ResponseWriter, r *http.Request) { t.Errorf("%s: an upstream opened", name) },
				authorize: func(s Subscriber) (bool, error) {
					if how == "authorize_fails" {
						return false, canary
					}
					return how != "authorize_refuses", nil
				},
				identify: func(r Request) (Identity, error) {
					switch how {
					case "identify_fails":
						return Identity{}, canary
					case "identify_no_user":
						return Identity{}, nil
					}
					return Identity{User: "u1"}, nil
				}})
			got := ps.post(t, context.Background(), "u1", ua, json.RawMessage(`7`), "events/stream",
				map[string]any{"name": "deploy.finished", "arguments": map[string]any{"env": "env-canary"}, "cursor": "c2.cursor-canary.0.m"})
			_, rpcErr, _ := got.final()
			logs := ps.logs.String()
			if strings.HasSuffix(how, "_fails") {
				if rpcErr["code"] != float64(rpcInternal) || rpcErr["message"] != pushCheckFailed {
					t.Errorf("%s: %v", name, rpcErr)
				}
				var lines int
				for _, l := range strings.Split(strings.TrimSpace(logs), "\n") {
					var m map[string]any
					json.Unmarshal([]byte(l), &m)
					if m["code"] != "push_open_check_failed" {
						continue
					}
					lines++
					if keys := slices.Sorted(maps.Keys(m)); !slices.Equal(keys, []string{"code", "count", "event", "level", "msg", "time"}) || m["event"] != "deploy.finished" || m["level"] != "WARN" {
						t.Errorf("%s: %v", name, m)
					}
				}
				if lines != 1 {
					t.Errorf("%s: %d push_open_check_failed lines:\n%s", name, lines, logs)
				}
			} else {
				if rpcErr["code"] != forbidden[dialect] {
					t.Errorf("%s: want Forbidden, got %v", name, rpcErr)
				}
				if strings.Contains(logs, "push_open_check_failed") {
					t.Errorf("%s: logged push_open_check_failed", name)
				}
			}
			if strings.Contains(logs, "canary") {
				t.Errorf("%s: a log carries an argument, the cursor or the error's message:\n%s", name, logs)
			}
		}
	}
}
