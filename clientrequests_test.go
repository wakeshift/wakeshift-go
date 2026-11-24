package wakeshift_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wakeshift/wakeshift-go"
)

// Client requests end to end (clientrequests.go): a go-sdk client that declares elicitation answers the
// form the core describes, against the fake core, which adds the request
// only when the call offers elicit.form.

const elicitToken = "wse_00000000-0000-4000-8000-000000000000_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// elicitRequest is client-requests.json's first validate request (the
// core's Codex form), changed by patch.
func elicitRequest(t *testing.T, patch func(r map[string]any)) json.RawMessage {
	t.Helper()
	var f struct {
		Validate []struct {
			Request json.RawMessage `json:"request"`
		} `json:"validate"`
	}
	readFixture(t, "client-requests.json", &f)
	if patch == nil {
		return f.Validate[0].Request
	}
	var r map[string]any
	if err := json.Unmarshal(f.Validate[0].Request, &r); err != nil {
		t.Fatal(err)
	}
	patch(r)
	b, _ := json.Marshal(r)
	return b
}

type elicitRun struct {
	res      *mcp.CallToolResult
	err      error
	asked    []*mcp.ElicitParams
	addition map[string]any
	core     *wakeCore
	logs     *logLines
}

func (r elicitRun) texts() []string {
	var out []string
	if r.res == nil {
		return nil
	}
	for _, c := range r.res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			out = append(out, tc.Text)
		}
	}
	return out
}

func (r elicitRun) reports() []map[string]any { return r.core.bodies("/requests/outcome") }

// elicitSetup is how a run connects the client: over memory (stdio's
// shape), or streamable HTTP with or without sessions.
type elicitSetup struct {
	opts     wakeshift.AttachOptions
	addition map[string]any
	http     string // "": in memory; "stateful" or "stateless"
	protocol string // the client's ("": 2025-11-25, Codex's)
	callCtx  func(context.Context) (context.Context, context.CancelFunc)
}

// runElicit calls a tool that subscribes, once, from a client that answers
// elicitation with answer (nil: one that doesn't declare elicitation).
func runElicit(t *testing.T, answer func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error), request json.RawMessage, setup elicitSetup) elicitRun {
	t.Helper()
	core := &wakeCore{appName: "Shipyard", request: request, addition: setup.addition}
	if core.addition == nil {
		core.addition = map[string]any{"text": "Today's text.", "trailer": "", "meta": map[string]any{"wakeshift/connector": "beacon-1"}}
	}
	cs := httptest.NewServer(http.HandlerFunc(core.handler))
	t.Cleanup(cs.Close)
	logs := &logLines{}
	app := testClient(t, cs.URL, wakeshift.WithLogger(logs.logger()))
	build, err := wakeshift.DefineEvent(app, "build.finished", wakeshift.EventDef[tableArgs, turnMsg]{Description: "A build finished.", Personal: true})
	if err != nil {
		t.Fatal(err)
	}
	opts := setup.opts
	opts.Identify = func(context.Context, wakeshift.Request) (wakeshift.Identity, error) {
		return wakeshift.Identity{User: "ada"}, nil
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "shipyard", Version: "1"}, nil)
	at := app.Attach(srv, opts)
	wakeshift.AddTool(at, &mcp.Tool{Name: "watch_build"}, func(ctx context.Context, call *wakeshift.ToolCall, in tableIn) (*mcp.CallToolResult, any, error) {
		if _, err := build.Subscribe(ctx, call, tableArgs{Table: in.Table}); err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "Watching."}}}, nil, nil
	}, wakeshift.Subscribes(build))

	var mu sync.Mutex
	var asked []*mcp.ElicitParams
	copts := &mcp.ClientOptions{}
	if answer != nil {
		copts.ElicitationHandler = func(ctx context.Context, req *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			mu.Lock()
			asked = append(asked, req.Params)
			mu.Unlock()
			return answer(ctx, req)
		}
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "codex-mcp-client", Version: "0.170.0"}, copts)
	var ct mcp.Transport
	switch setup.http {
	case "":
		st, c := mcp.NewInMemoryTransports()
		if _, err := srv.Connect(context.Background(), st, nil); err != nil {
			t.Fatal(err)
		}
		ct = c
	default:
		h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, &mcp.StreamableHTTPOptions{Stateless: setup.http == "stateless"})
		hs := httptest.NewServer(at.Handler(h))
		t.Cleanup(hs.Close)
		ct = &mcp.StreamableClientTransport{Endpoint: hs.URL}
	}
	protocol := setup.protocol
	if protocol == "" {
		protocol = "2025-11-25"
	}
	session, err := client.Connect(context.Background(), ct, &mcp.ClientSessionOptions{ProtocolVersion: protocol})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	ctx, cancel := context.Background(), context.CancelFunc(func() {})
	if setup.callCtx != nil {
		ctx, cancel = setup.callCtx(ctx)
	}
	defer cancel()
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "watch_build", Arguments: map[string]any{"table": "oak"}})
	run := elicitRun{res: res, err: err, core: core, logs: logs}
	if adds := core.bodies("/calls/addition"); len(adds) > 0 {
		run.addition = adds[0]
	}
	mu.Lock()
	run.asked = asked
	mu.Unlock()
	return run
}

func accept(choice string) func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
	return func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"choice": choice}}, nil
	}
}

func offered(run elicitRun) []any {
	call, _ := run.addition["call"].(map[string]any)
	kinds, _ := call["client_requests"].([]any)
	return kinds
}

func wantReports(t *testing.T, run elicitRun, outcome string) {
	t.Helper()
	want := []map[string]any{{"token": elicitToken, "outcome": outcome}}
	if got := run.reports(); !reflect.DeepEqual(got, want) {
		t.Fatalf("reports %v, want %v", got, want)
	}
}

// Accept: the form in the client's dialog, with the SDK's first line and
// the core's schema as it sent it; the answer's text before the tool's own
// content, the beacon kept, and the outcome reported.
func TestClientRequestAccept(t *testing.T) {
	for _, transport := range []string{"", "stateful"} {
		t.Run("transport "+transport, func(t *testing.T) {
			form := elicitRequest(t, nil)
			run := runElicit(t, accept("setup"), form, elicitSetup{http: transport})
			if run.err != nil {
				t.Fatal(run.err)
			}
			if k := offered(run); len(k) != 1 || k[0] != "elicit.form" {
				t.Fatalf("client_requests %v", run.addition["call"])
			}
			if len(run.asked) != 1 {
				t.Fatalf("asked %d times", len(run.asked))
			}
			if m := run.asked[0].Message; !strings.HasPrefix(m, "Wakeshift, for Shipyard:\nShipyard sends its notifications") {
				t.Fatalf("message %q", m)
			}
			var want struct {
				Schema any `json:"schema"`
			}
			json.Unmarshal(form, &want)
			got, _ := json.Marshal(run.asked[0].RequestedSchema)
			var gotSchema any
			json.Unmarshal(got, &gotSchema)
			if !reflect.DeepEqual(gotSchema, want.Schema) {
				t.Fatalf("schema %s", got)
			}
			if want := []string{"Set up.", "Watching."}; !eqStrings(run.texts(), want) {
				t.Fatalf("content %q", run.texts())
			}
			if run.res.Meta["wakeshift/connector"] != "beacon-1" {
				t.Fatalf("meta %v", run.res.Meta)
			}
			wantReports(t, run, "choice=setup")
		})
	}
}

// Decline: the core's text for decline, after the tool's content, and
// decline reported.
func TestClientRequestDecline(t *testing.T) {
	run := runElicit(t, func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		return &mcp.ElicitResult{Action: "decline"}, nil
	}, elicitRequest(t, nil), elicitSetup{})
	if run.err != nil || len(run.asked) != 1 {
		t.Fatalf("%v, asked %d", run.err, len(run.asked))
	}
	if want := []string{"Watching.", "Today's text."}; !eqStrings(run.texts(), want) {
		t.Fatalf("content %q", run.texts())
	}
	wantReports(t, run, "decline")
}

// A request past the bounds is never shown: the core's text stays as it
// gave it, unsupported is reported, and the bound is logged once.
func TestClientRequestPastBounds(t *testing.T) {
	core := "Wakeshift: today's offer, as text."
	run := runElicit(t, accept("setup"), elicitRequest(t, func(r map[string]any) { r["kind"] = "elicit.url" }),
		elicitSetup{addition: map[string]any{"text": core, "trailer": testTrailer, "meta": map[string]any{"wakeshift/connector": "beacon-1"}}})
	if run.err != nil {
		t.Fatal(run.err)
	}
	if len(run.asked) != 0 {
		t.Fatalf("shown: %+v", run.asked)
	}
	if want := []string{"Watching.", core, testTrailer}; !eqStrings(run.texts(), want) {
		t.Fatalf("content %q", run.texts())
	}
	wantReports(t, run, "unsupported")
	var refused int
	for _, l := range run.logs.lines() {
		if l["code"] == "client_request_refused" {
			refused++
			if l["reason"] != "kind" || !strings.Contains(l["msg"].(string), "past this SDK's bounds (kind)") {
				t.Errorf("log %v", l)
			}
		}
	}
	if refused != 1 {
		t.Fatalf("client_request_refused logged %d times: %v", refused, run.logs.codes())
	}
}

// Off (DisableClientRequests), a client that doesn't declare elicitation,
// and a stateless HTTP server: nothing is offered, so nothing is shown.
func TestClientRequestNotOffered(t *testing.T) {
	form := elicitRequest(t, nil)
	for name, run := range map[string]func() elicitRun{
		"off": func() elicitRun {
			return runElicit(t, accept("setup"), form, elicitSetup{opts: wakeshift.AttachOptions{DisableClientRequests: true}})
		},
		"no forms":  func() elicitRun { return runElicit(t, nil, form, elicitSetup{}) },
		"stateless": func() elicitRun { return runElicit(t, accept("setup"), form, elicitSetup{http: "stateless"}) },
		"2026-07-28": func() elicitRun {
			return runElicit(t, accept("setup"), form, elicitSetup{protocol: "2026-07-28"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := run()
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.addition == nil || offered(r) != nil {
				t.Fatalf("addition %v", r.addition)
			}
			if len(r.asked) != 0 || len(r.reports()) != 0 {
				t.Fatalf("asked %d, reports %v", len(r.asked), r.reports())
			}
			if want := []string{"Watching.", "Today's text."}; !eqStrings(r.texts(), want) {
				t.Fatalf("content %q", r.texts())
			}
		})
	}
}

// An outcome that drops the beacon: no trailer, no _meta beacon.
func TestClientRequestDropsBeacon(t *testing.T) {
	form := elicitRequest(t, func(r map[string]any) {
		r["outcomes"].(map[string]any)["choice=later"] = map[string]any{"text": "Not now.", "placement": "after", "beacon": "drop"}
	})
	run := runElicit(t, accept("later"), form,
		elicitSetup{addition: map[string]any{"text": "Today's text.", "trailer": testTrailer, "meta": map[string]any{"wakeshift/connector": "beacon-1", "other": "x"}}})
	if run.err != nil {
		t.Fatal(run.err)
	}
	if want := []string{"Watching.", "Not now."}; !eqStrings(run.texts(), want) {
		t.Fatalf("content %q", run.texts())
	}
	if _, ok := run.res.Meta["wakeshift/connector"]; ok || run.res.Meta["other"] != "x" {
		t.Fatalf("meta %v", run.res.Meta)
	}
	wantReports(t, run, "choice=later")
}

// No answer within the timeout: timeout's text, and timeout reported.
func TestClientRequestTimeout(t *testing.T) {
	defer wakeshift.SetClientRequestTimeout(200 * time.Millisecond)()
	run := runElicit(t, func(ctx context.Context, _ *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, elicitRequest(t, nil), elicitSetup{})
	if run.err != nil {
		t.Fatal(run.err)
	}
	if want := []string{"Watching.", "Today's text."}; !eqStrings(run.texts(), want) {
		t.Fatalf("content %q", run.texts())
	}
	wantReports(t, run, "timeout")
}

// The client cancels the call while the dialog is open: cancel is
// reported all the same.
func TestClientRequestCallCanceled(t *testing.T) {
	opened := make(chan struct{})
	run := runElicit(t, func(ctx context.Context, _ *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
		close(opened)
		<-ctx.Done()
		return nil, ctx.Err()
	}, elicitRequest(t, nil), elicitSetup{callCtx: func(ctx context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			<-opened
			cancel()
		}()
		return ctx, cancel
	}})
	if run.err == nil {
		t.Fatalf("the call wasn't canceled: %+v", run.res)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(run.reports()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	wantReports(t, run, "cancel")
}
