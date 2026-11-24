package wakeshift

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wakeshift/wakeshift-go/internal/mcpevents/event"
)

// mcp-events-interop.json (with the TypeScript and Python SDKs): where the
// SDKs used to answer MCP Events requests differently.
type interopGolden struct {
	Revision      string `json:"revision"`
	MCPNameDecode struct {
		Cases []struct {
			Header string  `json:"header"`
			Value  *string `json:"value"`
		} `json:"cases"`
	} `json:"mcp_name_decode"`
	MCPName struct {
		Cases []struct {
			Name     string  `json:"name"`
			Method   string  `json:"method"`
			Client   string  `json:"client"`
			Declares bool    `json:"declares"`
			Header   *string `json:"header"`
			Second   *string `json:"second_header"`
			Want     string  `json:"want"`
		} `json:"cases"`
	} `json:"mcp_name"`
	MCPParamSchema struct {
		Cases []struct {
			Name   string `json:"name"`
			Schema any    `json:"schema"`
			Valid  bool   `json:"valid"`
		} `json:"cases"`
	} `json:"mcp_param_schema"`
	MCPParam struct {
		Schema json.RawMessage `json:"schema"`
		Cases  []struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
			Headers   [][2]string     `json:"headers"`
			Want      string          `json:"want"`
		} `json:"cases"`
	} `json:"mcp_param"`
	MCPParamHTTP struct {
		Event       string          `json:"event"`
		InputSchema json.RawMessage `json:"input_schema"`
		Cases       []struct {
			Name      string            `json:"name"`
			Method    string            `json:"method"`
			Event     string            `json:"event"`
			Arguments map[string]any    `json:"arguments"`
			Headers   map[string]string `json:"headers"`
			NoName    bool              `json:"no_name"`
			Want      string            `json:"want"`
		} `json:"cases"`
	} `json:"mcp_param_http"`
	Stream struct {
		Params map[string]any `json:"params"`
		Error  struct {
			Code    map[string]float64 `json:"code"`
			Message string             `json:"message"`
			Data    map[string]any     `json:"data"`
		} `json:"error"`
	} `json:"stream"`
	TransportHeaders struct {
		Cases []struct {
			Name        string            `json:"name"`
			Method      string            `json:"method"`
			Headers     map[string]string `json:"headers"`
			MetaVersion *string           `json:"meta_version"`
			Legacy      bool              `json:"legacy"`
			Want        string            `json:"want"`
		} `json:"cases"`
	} `json:"transport_headers"`
	NoEvents struct {
		Methods []string `json:"methods"`
		Code    float64  `json:"code"`
	} `json:"no_events"`
	NativeClient struct {
		Cases []struct {
			Name              string         `json:"name"`
			Client            string         `json:"client"`
			UserAgent         string         `json:"user_agent"`
			Meta              map[string]any `json:"meta"`
			SessionExtensions map[string]any `json:"session_extensions"`
			ServesPush        bool           `json:"serves_push"`
			Native            bool           `json:"native"`
			Dialect           string         `json:"dialect"`
			Entry             string         `json:"entry"`
		} `json:"cases"`
	} `json:"native_client"`
}

func readInterop(t *testing.T) interopGolden {
	t.Helper()
	var g interopGolden
	readSharedFixture(t, "mcp-events-interop.json", &g)
	return g
}

// interopServer is chat.message (public) behind Handler, user u1, over a
// stateless MCP handler; asked counts the requests that reached Wakeshift's
// events routes.
type interopServer struct {
	url   string
	mu    sync.Mutex
	asked int
}

func newInteropServer(t *testing.T, withEvent bool, more ...func(*Client)) *interopServer {
	t.Helper()
	s := &interopServer{}
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/apps/v1")
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(path, "/events/") {
			s.mu.Lock()
			s.asked++
			s.mu.Unlock()
		}
		switch path {
		case "/events/poll":
			io.WriteString(w, `{"events": [], "cursor": "c1.0", "truncated": false, "has_more": false, "next_poll_ms": 10000, "created": false}`)
		case "/events/subscribe":
			io.WriteString(w, `{"id": "sub_1", "refresh_before": "2026-11-11T13:00:00Z", "cursor": "c1.0", "truncated": false, "created": false}`)
		case "/events/unsubscribe":
			io.WriteString(w, `{"ended": true}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	t.Cleanup(core.Close)
	app := clientAt(t, core.URL)
	if withEvent {
		if _, err := DefineEvent(app, "chat.message", EventDef[wireArgs, wireMsg]{Description: "A new message in a channel", Public: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range more {
		f(app)
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "chat", Version: "1"}, nil)
	at := app.Attach(srv, AttachOptions{Identify: func(context.Context, Request) (Identity, error) { return Identity{User: "u1"}, nil }})
	hs := httptest.NewServer(at.Handler(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})))
	t.Cleanup(hs.Close)
	s.url = hs.URL
	return s
}

func (s *interopServer) askedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.asked
}

// post sends one JSON-RPC request with headers; status and the JSON-RPC
// error (nil for a result).
func (s *interopServer) post(t *testing.T, method string, params any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, _ := http.NewRequest("POST", s.url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		if name, ok := strings.CutSuffix(k, "+"); ok {
			continue
		} else {
			req.Header.Set(name, v)
		}
	}
	// A key ending in "+" adds a second field of that header.
	for k, v := range headers {
		if name, ok := strings.CutSuffix(k, "+"); ok {
			req.Header.Add(name, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Error map[string]any `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s: %d %s", method, resp.StatusCode, raw)
	}
	return resp.StatusCode, out.Error
}

func modernMeta(client string, declares bool) map[string]any {
	caps := map[string]any{}
	if declares {
		caps["extensions"] = map[string]any{event.ExtensionKey: map[string]any{}}
	}
	return map[string]any{
		"io.modelcontextprotocol/protocolVersion":    "2026-07-28",
		"io.modelcontextprotocol/clientInfo":         map[string]any{"name": client, "version": "1"},
		"io.modelcontextprotocol/clientCapabilities": caps,
	}
}

func TestInteropGoldenRevision(t *testing.T) {
	if g := readInterop(t); g.Revision != event.Revision {
		t.Fatalf("revision %q, want %q", g.Revision, event.Revision)
	}
}

func TestInteropGoldenMcpNameDecode(t *testing.T) {
	for _, c := range readInterop(t).MCPNameDecode.Cases {
		got, ok := event.HeaderValue(c.Header)
		switch {
		case c.Value == nil && ok:
			t.Errorf("%q: decoded to %q, want refused", c.Header, got)
		case c.Value != nil && (!ok || got != *c.Value):
			t.Errorf("%q: %q %v, want %q", c.Header, got, ok, *c.Value)
		}
	}
}

func TestInteropGoldenMcpName(t *testing.T) {
	s := newInteropServer(t, true)
	for _, c := range readInterop(t).MCPName.Cases {
		params := map[string]any{"name": "chat.message", "arguments": map[string]any{"channel": "eng"}, "_meta": modernMeta(c.Client, c.Declares)}
		switch c.Method {
		case "events/list":
			params = map[string]any{"_meta": modernMeta(c.Client, c.Declares)}
		case "events/subscribe":
			params["delivery"] = map[string]any{"mode": "webhook", "url": "https://agent.example/hooks/1", "secret": "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA=="}
		}
		h := map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": c.Method, "User-Agent": c.Client + "/1"}
		if c.Header != nil {
			h["Mcp-Name"] = *c.Header
		}
		if c.Second != nil {
			h["Mcp-Name+"] = *c.Second
		}
		before := s.askedCount()
		status, e := s.post(t, c.Method, params, h)
		refused := status == http.StatusBadRequest && e != nil && e["code"] == float64(event.HeaderMismatch)
		if refused != (c.Want == "refused") {
			t.Errorf("%s: %d %v, want %s", c.Name, status, e, c.Want)
		}
		if refused && s.askedCount() != before {
			t.Errorf("%s: refused, but asked Wakeshift", c.Name)
		}
	}
}

func TestInteropGoldenStream(t *testing.T) {
	g := readInterop(t).Stream
	s := newInteropServer(t, true)
	for dialect, client := range map[string]string{"draft": "some-draft-client", "chatgpt": "openai-mcp"} {
		for _, name := range []string{"chat.message", "no.such.event"} {
			params := map[string]any{}
			for k, v := range g.Params {
				params[k] = v
			}
			params["name"] = name
			params["_meta"] = modernMeta(client, false)
			// The transport's headers, which are checked first (transport_headers).
			_, e := s.post(t, "events/stream", params, map[string]string{"User-Agent": client + "/1",
				"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": "events/stream"})
			data, _ := e["data"].(map[string]any)
			if e == nil || e["code"] != g.Error.Code[dialect] || e["message"] != g.Error.Message || data["feature"] != g.Error.Data["feature"] || data["value"] != g.Error.Data["value"] {
				t.Errorf("%s %s: %v, want %v %v", dialect, name, e, g.Error.Code[dialect], g.Error.Data)
			}
		}
	}
	if s.askedCount() != 0 {
		t.Error("events/stream asked Wakeshift")
	}
}

// transport_headers: MCP-Protocol-Version and Mcp-Method on events/*,
// checked before anything else, legacy requests included (Handler serves
// them without a session).
func TestInteropGoldenTransportHeaders(t *testing.T) {
	s := newInteropServer(t, true)
	for _, c := range readInterop(t).TransportHeaders.Cases {
		meta := modernMeta("tanstack-ai", true)
		if c.MetaVersion == nil {
			delete(meta, "io.modelcontextprotocol/protocolVersion")
		} else {
			meta["io.modelcontextprotocol/protocolVersion"] = *c.MetaVersion
		}
		params := map[string]any{"name": "chat.message", "arguments": map[string]any{"channel": "eng"}, "_meta": meta}
		switch c.Method {
		case "events/list":
			params = map[string]any{"_meta": meta}
		case "events/subscribe":
			params["delivery"] = map[string]any{"mode": "webhook", "url": "https://agent.example/hooks/1", "secret": "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA=="}
		}
		h := map[string]string{"User-Agent": "tanstack-ai/1"}
		if c.Method != "events/list" {
			h["Mcp-Name"] = "chat.message"
		}
		for k, v := range c.Headers {
			h[k] = v
		}
		before := s.askedCount()
		status, e := s.post(t, c.Method, params, h)
		refused := status == http.StatusBadRequest && e != nil && e["code"] == float64(event.HeaderMismatch)
		if refused != (c.Want == "refused") {
			t.Errorf("%s: %d %v, want %s", c.Name, status, e, c.Want)
		}
		if refused && s.askedCount() != before {
			t.Errorf("%s: refused, but asked Wakeshift", c.Name)
		}
	}
}

func TestInteropGoldenNoEvents(t *testing.T) {
	g := readInterop(t).NoEvents
	s := newInteropServer(t, false)
	for _, m := range g.Methods {
		_, e := s.post(t, m, map[string]any{"name": "chat.message", "arguments": map[string]any{}, "_meta": modernMeta("tanstack-ai", true)},
			map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": m, "Mcp-Name": "chat.message"})
		if e == nil || e["code"] != g.Code {
			t.Errorf("%s: %v, want %v", m, e, g.Code)
		}
	}
}

// The client table's cases: the SDK's rule, with the case's serves_push,
// and the table's whole verdict (native, dialect, matched entry) for each,
// push or not.
func TestInteropGoldenNativeClient(t *testing.T) {
	for _, c := range readInterop(t).NativeClient.Cases {
		if got := nativeClientRule(c.Client, c.UserAgent, c.Meta, c.SessionExtensions, c.ServesPush); got != c.Native {
			t.Errorf("%s: %v, want %v", c.Name, got, c.Native)
		}
		v := event.EvaluateClient(event.ClientInput{Client: c.Client, UserAgent: c.UserAgent, Meta: c.Meta,
			SessionExtensions: c.SessionExtensions, ServesPush: c.ServesPush})
		if v.Native != c.Native || v.Dialect != c.Dialect || v.Entry != c.Entry {
			t.Errorf("%s: %+v, want native %v dialect %s entry %q", c.Name, v, c.Native, c.Dialect, c.Entry)
		}
	}
}

// The client table this SDK compiles in is the shared one, which the
// TypeScript and Python SDKs ship as generated constants.
func TestInteropGoldenClientTable(t *testing.T) {
	want, err := os.ReadFile(filepath.Join(goldenDir, "mcp-events-clients.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(event.ClientsJSON(), want) {
		t.Error("the compiled-in client table differs from mcp-events-clients.json")
	}
}

// paramDecode is golden JSON decoded as the request path decodes
// params.arguments (numbers exact).
func paramDecode(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestInteropGoldenMcpParamSchema(t *testing.T) {
	for _, c := range readInterop(t).MCPParamSchema.Cases {
		_, err := paramHeadersOf(c.Schema)
		if (err == nil) != c.Valid {
			t.Errorf("%s: %v, want valid %v", c.Name, err, c.Valid)
		}
	}
	// Through DefineEvent: an ArgsSchema whose annotation is on a number.
	app := clientAt(t, "https://core.test")
	_, err := DefineEvent(app, "bad.number", EventDef[map[string]any, wireMsg]{Description: "d", Topic: "t",
		ArgsSchema: json.RawMessage(`{"type":"object","properties":{"ratio":{"type":"number","x-mcp-header":"Ratio"}}}`)})
	var ue *UsageError
	if !errors.As(err, &ue) || ue.Code != CodeConfig || !strings.Contains(ue.Error(), "x-mcp-header on ratio") {
		t.Fatalf("DefineEvent: %v, want a config error naming x-mcp-header on ratio", err)
	}
	ev, err := DefineEvent(app, "good.event", EventDef[map[string]any, wireMsg]{Description: "d", Topic: "t",
		ArgsSchema: json.RawMessage(`{"type":"object","properties":{"team":{"type":"object","properties":{"id":{"type":"string","x-mcp-header":"Team-Id"}}}}}`)})
	if err != nil {
		t.Fatal(err)
	}
	if got := ev.t.paramHeaders; len(got) != 1 || strings.Join(got[0].Path, ".") != "team.id" || got[0].Name != "Team-Id" || got[0].Type != "string" {
		t.Errorf("paramHeaders %+v", got)
	}
}

func TestInteropGoldenMcpParam(t *testing.T) {
	g := readInterop(t).MCPParam
	annotated, err := paramHeadersOf(paramDecode(t, g.Schema))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range g.Cases {
		h := http.Header{}
		for _, kv := range c.Headers {
			h.Add(kv[0], kv[1])
		}
		msg := paramHeaderRefusal(annotated, paramDecode(t, c.Arguments), h)
		got := "passed"
		if msg != "" {
			got = "refused"
		}
		if got != c.Want {
			t.Errorf("%s: %s (%s), want %s", c.Name, got, msg, c.Want)
		}
	}
}

func TestInteropGoldenMcpParamHTTP(t *testing.T) {
	g := readInterop(t).MCPParamHTTP
	s := newInteropServer(t, true, func(app *Client) {
		if _, err := DefineEvent(app, g.Event, EventDef[map[string]any, map[string]any]{Description: "A deploy finished", Public: true,
			ArgsSchema:    g.InputSchema,
			PayloadSchema: json.RawMessage(`{"type":"object","properties":{"attempt":{"type":"integer"},"region":{"type":"string"}},"required":["attempt","region"]}`)}); err != nil {
			t.Fatal(err)
		}
	})
	// events/list publishes the annotations as defined.
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "events/list", "params": map[string]any{"_meta": modernMeta("tanstack-ai", true)}})
	req, _ := http.NewRequest("POST", s.url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2026-07-28")
	req.Header.Set("Mcp-Method", "events/list")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Result struct {
			Events []struct {
				Name        string          `json:"name"`
				InputSchema json.RawMessage `json:"inputSchema"`
			} `json:"events"`
		} `json:"result"`
	}
	json.NewDecoder(resp.Body).Decode(&listed)
	resp.Body.Close()
	found := false
	for _, e := range listed.Result.Events {
		if e.Name == g.Event {
			found = true
			var got, want any
			json.Unmarshal(e.InputSchema, &got)
			json.Unmarshal(g.InputSchema, &want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("inputSchema %s, want %s", e.InputSchema, g.InputSchema)
			}
		}
	}
	if !found {
		t.Fatalf("events/list doesn't list %s", g.Event)
	}
	for _, c := range g.Cases {
		name := cmp.Or(c.Event, g.Event)
		params := map[string]any{"name": name, "arguments": c.Arguments, "_meta": modernMeta("tanstack-ai", true)}
		if c.Method == "events/subscribe" {
			params["delivery"] = map[string]any{"mode": "webhook", "url": "https://agent.example/hooks/1", "secret": "whsec_dGVzdC1zZWNyZXQtdGVzdC1zZWNyZXQtdGVzdA=="}
		}
		h := map[string]string{"MCP-Protocol-Version": "2026-07-28", "Mcp-Method": c.Method, "User-Agent": "tanstack-ai/1"}
		if !c.NoName {
			h["Mcp-Name"] = name
		}
		for k, v := range c.Headers {
			h[k] = v
		}
		before := s.askedCount()
		status, e := s.post(t, c.Method, params, h)
		refused := status == http.StatusBadRequest && e != nil && e["code"] == float64(event.HeaderMismatch)
		if refused != (c.Want == "refused") {
			t.Errorf("%s: %d %v, want %s", c.Name, status, e, c.Want)
		}
		if refused && s.askedCount() != before {
			t.Errorf("%s: refused, but asked Wakeshift", c.Name)
		}
	}
}
