package wakeshift

// Client requests: the core's
// /calls/addition answer can carry one request for the SDK to make of the
// MCP client during a Wakeshift subscribing call. The first kind is an
// elicitation form (Codex's install offer, in Codex's own dialog). The SDK
// knows no wording and no policy: it offers the kinds it can run in this
// call (call.client_requests), runs a request only within fixed bounds it
// enforces itself, whatever the core sends, uses the core's text for the
// outcome, and reports the outcome. sdk/typescript/test/client-requests.json
// pins the bounds, the outcome keys and the offer for every SDK.
//
// Only a go-sdk server runs them: go-sdk's ServerSession.Elicit sends the
// request during the call, on the call's own stream (stdio, or streamable
// HTTP with sessions). mcp-go servers are offered none.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wakeshift/wakeshift-go/internal/adapt"
)

// elicitForm is the kind of request this SDK can run.
const elicitForm = "elicit.form"

// pushProtocols are the protocols a request pushed during a call works on:
// an allowlist, so a revision this SDK doesn't know (2026-07-28 replaced the
// push with inputRequired, SEP-2577) gets none. "": none on the request
// (stdio).
var pushProtocols = map[string]bool{"": true, "2025-06-18": true, "2025-11-25": true}

// placementBefore puts an outcome's text before the tool's own content.
const placementBefore = "before"

// clientRequestBounds are the bounds (client-requests.json bounds). Tests
// shorten the durations.
var clientRequestBounds = struct {
	messageMax, messageLinesMax, titleMax, fieldsMax, optionsMax, outcomeTextMax int
	timeout, reportWait, sessionWindow                                           time.Duration
}{1200, 20, 80, 2, 4, 4000, 120 * time.Second, 2 * time.Second, 10 * time.Minute}

// outcomeKeys are the outcome keys besides an accepted answer's <field>=<value>.
var outcomeKeys = []string{"decline", "cancel", "timeout", "unsupported"}

// requestOutcome is one outcome's result: the core's text in place of
// Wakeshift's own, where it goes ("before" or "after" the tool's content),
// and the beacon ("keep" or "drop").
type requestOutcome struct {
	Text      string
	Placement string
	Beacon    string
}

// requestField is one field of a request's form: a boolean, or a string
// with its values (oneOf's consts).
type requestField struct {
	name    string
	boolean bool
	values  []string
}

// clientRequest is a request that passed the bounds (parseRequest).
type clientRequest struct {
	token    string
	message  string          // the core's, without the SDK's first line
	schema   json.RawMessage // as the core sent it, its order kept
	fields   []requestField  // in the schema's order
	outcomes map[string]requestOutcome
}

// requestPrefix is the message's first line, which the SDK writes itself:
// Wakeshift and the app, always.
func requestPrefix(app string) string {
	name := strings.TrimSpace(app)
	if utf8.RuneCountInString(name) > clientRequestBounds.titleMax {
		name = string([]rune(name)[:clientRequestBounds.titleMax])
	}
	if name == "" {
		name = "an app"
	}
	return "Wakeshift, for " + name + ":"
}

// requestMessage is the message the client is sent: the SDK's line, then
// the core's message.
func requestMessage(message, app string) string {
	return requestPrefix(app) + "\n" + message
}

// validateRequest is "" if the SDK may run the request for app, else the
// first bound it breaks (client-requests.json validate: kind, message,
// lines, secret, schema, fields, field, options, title, outcomes,
// outcome_text, placement, beacon).
func validateRequest(raw json.RawMessage, app string) string {
	_, bad := parseRequest(raw, app)
	return bad
}

// parseRequest is the request the core sent, if it's within the bounds
// (bad ""), else the first bound it breaks.
func parseRequest(raw json.RawMessage, app string) (r *clientRequest, bad string) {
	b := clientRequestBounds
	var v map[string]any
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &v) != nil || v == nil || json.Unmarshal(raw, &top) != nil {
		return nil, "kind"
	}
	token, _ := v["token"].(string)
	if v["kind"] != elicitForm || token == "" {
		return nil, "kind"
	}
	msg, ok := v["message"].(string)
	if !ok {
		return nil, "message"
	}
	message := requestMessage(msg, app)
	if utf8.RuneCountInString(message) > b.messageMax {
		return nil, "message"
	}
	if strings.Count(message, "\n")+1 > b.messageLinesMax {
		return nil, "lines"
	}
	if RedactSecrets(message) != message {
		return nil, "secret"
	}
	schema, _ := v["schema"].(map[string]any)
	props, _ := schema["properties"].(map[string]any)
	if schema == nil || schema["type"] != "object" || props == nil {
		return nil, "schema"
	}
	var rawSchema map[string]json.RawMessage
	_ = json.Unmarshal(top["schema"], &rawSchema)
	names := objectKeys(rawSchema["properties"])
	if len(names) == 0 || len(names) > b.fieldsMax {
		return nil, "fields"
	}
	r = &clientRequest{token: token, message: msg, schema: top["schema"], outcomes: map[string]requestOutcome{}}
	for _, name := range names {
		f, ok := props[name].(map[string]any)
		if !ok {
			return nil, "field"
		}
		field := requestField{name: name}
		_, oneOf := f["oneOf"]
		_, enum := f["enum"]
		_, format := f["format"]
		options, isList := f["oneOf"].([]any)
		switch {
		case f["type"] == "boolean":
			if oneOf || enum {
				return nil, "field"
			}
			field.boolean = true
		case f["type"] == "string" && isList && !enum && !format:
			if len(options) == 0 || len(options) > b.optionsMax {
				return nil, "options"
			}
			for _, o := range options {
				om, _ := o.(map[string]any)
				c, _ := om["const"].(string)
				title, isTitle := om["title"].(string)
				if om == nil || c == "" || !isTitle {
					return nil, "field"
				}
				if utf8.RuneCountInString(title) > b.titleMax {
					return nil, "title"
				}
				field.values = append(field.values, c)
			}
		default:
			return nil, "field"
		}
		if title, ok := f["title"].(string); !ok || utf8.RuneCountInString(title) > b.titleMax {
			return nil, "title"
		}
		r.fields = append(r.fields, field)
	}
	outcomes, ok := v["outcomes"].(map[string]any)
	if !ok {
		return nil, "outcomes"
	}
	for _, k := range append(r.answerKeys(), outcomeKeys...) {
		if _, ok := outcomes[k].(map[string]any); !ok {
			return nil, "outcomes"
		}
	}
	for _, k := range objectKeys(top["outcomes"]) {
		o, _ := outcomes[k].(map[string]any)
		text, ok := o["text"].(string)
		if o == nil || !ok || utf8.RuneCountInString(text) > b.outcomeTextMax {
			return nil, "outcome_text"
		}
		placement, _ := o["placement"].(string)
		if placement != placementBefore && placement != "after" {
			return nil, "placement"
		}
		beacon, _ := o["beacon"].(string)
		if beacon != "keep" && beacon != "drop" {
			return nil, "beacon"
		}
		r.outcomes[k] = requestOutcome{Text: text, Placement: placement, Beacon: beacon}
	}
	return r, ""
}

// answerKeys are the outcome keys an answer to r can have (<field>=<value>).
func (r *clientRequest) answerKeys() []string {
	var out []string
	for _, f := range r.fields {
		if f.boolean {
			out = append(out, f.name+"=true", f.name+"=false")
			continue
		}
		for _, v := range f.values {
			out = append(out, f.name+"="+v)
		}
	}
	return out
}

// objectKeys is a JSON object's keys in the order they're written, each
// once (nil for anything else): the order the core gave a form's fields
// and outcomes in, which Go's maps don't keep.
func objectKeys(raw json.RawMessage) []string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil
	}
	var keys []string
	seen := map[string]bool{}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return keys
		}
		k, _ := t.(string)
		var skip json.RawMessage
		if dec.Decode(&skip) != nil {
			return keys
		}
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return keys
}

// outcomeKey is the outcome key for the client's elicitation result to a
// valid request (client-requests.json outcome), the result as JSON values.
func outcomeKey(result map[string]any, r *clientRequest) string {
	if result == nil {
		return "unsupported"
	}
	switch action := result["action"]; action {
	case "decline", "cancel":
		return action.(string)
	case "accept":
	default:
		return "unsupported"
	}
	content, ok := result["content"].(map[string]any)
	if !ok {
		return "unsupported"
	}
	// The first field answered with one of its values names the outcome.
	for _, f := range r.fields {
		switch v := content[f.name].(type) {
		case bool:
			if f.boolean {
				return fmt.Sprintf("%s=%t", f.name, v)
			}
		case string:
			for _, c := range f.values {
				if !f.boolean && c == v {
					return f.name + "=" + v
				}
			}
		}
	}
	return "unsupported"
}

// elicitResultOf is go-sdk's elicitation result as JSON values, for outcomeKey.
func elicitResultOf(res *mcp.ElicitResult) map[string]any {
	if res == nil {
		return nil
	}
	out := map[string]any{"action": res.Action}
	if res.Content != nil {
		out["content"] = res.Content
	}
	return out
}

// offerInput is what the SDK sees of a call, for the offer
// (client-requests.json offer).
type offerInput struct {
	capabilities map[string]any
	protocol     string
	http         bool
	session      string
	off, recent  bool
}

// offersForm reports whether the SDK offers an elicitation form for a call.
func offersForm(o offerInput) bool {
	if o.off || o.recent {
		return false
	}
	e, ok := o.capabilities["elicitation"].(map[string]any)
	if !ok {
		return false
	}
	// 2025-06-18 declared elicitation as {}: forms. Later revisions say form.
	if _, form := e["form"]; len(e) > 0 && !form {
		return false
	}
	if !pushProtocols[o.protocol] {
		return false
	}
	// Over HTTP, only a stateful session carries the client's answer back
	// to this process.
	return !o.http || o.session != ""
}

// formSessions are the MCP sessions a form was shown in, with when: at most
// one per session per window. Bounded: past formSessionsMax, the expired go
// first, then any.
type formSessions struct {
	mu sync.Mutex
	at map[string]time.Time
}

const formSessionsMax = 10_000

func (f *formSessions) recent(key string, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	at, ok := f.at[key]
	return ok && now.Sub(at) < clientRequestBounds.sessionWindow
}

// claim records a form shown in the session now, reporting false (and
// recording nothing) if one was within the window.
func (f *formSessions) claim(key string, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if at, ok := f.at[key]; ok && now.Sub(at) < clientRequestBounds.sessionWindow {
		return false
	}
	if f.at == nil {
		f.at = map[string]time.Time{}
	}
	if len(f.at) >= formSessionsMax {
		maps.DeleteFunc(f.at, func(_ string, at time.Time) bool { return now.Sub(at) >= clientRequestBounds.sessionWindow })
		for k := range f.at {
			if len(f.at) < formSessionsMax {
				break
			}
			delete(f.at, k)
		}
	}
	f.at[key] = now
	return true
}

// formSessionKey is the call's MCP session for the window: its
// Mcp-Session-Id, else (stdio, in memory) the session itself.
func formSessionKey(ss *mcp.ServerSession) string {
	if id := ss.ID(); id != "" {
		return "id:" + id
	}
	return fmt.Sprintf("ss:%p", ss)
}

// offersRequests reports whether the call offers the core an elicitation
// form (call.client_requests): only a call that subscribed (it has a
// pending record), on go-sdk, with attach's client requests on.
func (s *callScope) offersRequests() bool {
	if s.pending == "" || s.requestsOff || s.goReq == nil || s.goReq.Session == nil {
		return false
	}
	ss := s.goReq.Session
	var caps map[string]any
	if c := s.goReq.ClientCapabilities(); c != nil {
		if b, err := json.Marshal(c); err == nil {
			_ = json.Unmarshal(b, &caps)
		}
	}
	protocol := s.goReq.ProtocolVersion()
	if protocol == "" && s.req.Header != nil {
		protocol = s.req.Header.Get("Mcp-Protocol-Version")
	}
	return offersForm(offerInput{capabilities: caps, protocol: protocol, http: s.req.Header != nil, session: ss.ID(),
		recent: s.at.app.forms.recent(formSessionKey(ss), s.at.app.now())})
}

// runRequest runs the client request the core's addition carries, within
// the SDK's own bounds, and gives the block with the outcome's text in
// place of the core's; it reports the outcome. A request it can't run is
// never shown: the block stays as the core gave it (today's text), and the
// outcome is unsupported. offered: the call offered the core a form.
func (at *Attachment) runRequest(ctx context.Context, s *callScope, blk adapt.Block, raw json.RawMessage, offered bool) adapt.Block {
	app := at.app.appName(ctx)
	if app == "" {
		app = at.app.knownAppID()
	}
	r, bad := parseRequest(raw, app)
	switch {
	case bad != "":
	case !offered || s.goReq == nil || s.goReq.Session == nil:
		// Only a call that offered one runs it: a subscribing call, on a
		// session that can carry it (offered implies one; checked here too).
		bad = "not_offered"
	case !at.app.forms.claim(formSessionKey(s.goReq.Session), at.app.now()):
		// At most one form per session per window, whatever the core sends.
		bad = "rate"
	}
	if bad != "" {
		at.app.logOnce(ctx, "client_request_refused", bad,
			fmt.Sprintf("Wakeshift sent a client request past this SDK's bounds (%s), so the call's result keeps Wakeshift's usual text", bad),
			slog.String("reason", bad))
		var t struct {
			Token any `json:"token"`
		}
		_ = json.Unmarshal(raw, &t)
		if token, _ := t.Token.(string); token != "" {
			at.reportOutcome(ctx, token, "unsupported")
		}
		return blk
	}
	outcome := at.runForm(ctx, s.goReq.Session, r, app)
	at.reportOutcome(ctx, r.token, outcome)
	res, ok := r.outcomes[outcome]
	if !ok {
		res = r.outcomes["unsupported"]
	}
	out := adapt.Block{Text: strings.TrimSpace(res.Text), Trailer: blk.Trailer, Meta: blk.Meta, Placement: res.Placement}
	if res.Beacon == "drop" {
		out.Trailer = ""
		out.Meta = maps.Clone(blk.Meta)
		delete(out.Meta, connectorMetaKey)
	}
	return out
}

// runForm shows a valid request's form through the call's session and maps
// the answer to its outcome key: the call ending is cancel, no answer
// within the timeout timeout (go-sdk sends notifications/cancelled for
// either), any other error unsupported.
func (at *Attachment) runForm(ctx context.Context, ss *mcp.ServerSession, r *clientRequest, app string) string {
	fctx, cancel := context.WithTimeout(ctx, clientRequestBounds.timeout)
	defer cancel()
	res, err := ss.Elicit(fctx, &mcp.ElicitParams{Mode: "form", Message: requestMessage(r.message, app), RequestedSchema: r.schema})
	switch {
	case err == nil:
		return outcomeKey(elicitResultOf(res), r)
	case ctx.Err() != nil:
		return "cancel"
	case fctx.Err() != nil:
		return "timeout"
	default:
		return "unsupported"
	}
}

// reportOutcome tells the core a request's outcome, waiting at most
// reportWait (so the next subscribing call sees it), even for a call that
// ended, and never failing the call's result.
func (at *Attachment) reportOutcome(ctx context.Context, token, outcome string) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), clientRequestBounds.reportWait)
	defer cancel()
	if err := at.app.appCall(rctx, "POST", "/api/apps/v1/requests/outcome", map[string]string{"token": token, "outcome": outcome}, nil); err != nil {
		at.app.logLimited(ctx, "client_request_report_failed", fmt.Sprintf("couldn't report a client request's outcome to Wakeshift (%s)", errorType(err)),
			slog.String("error_type", errorType(err)))
	}
}
