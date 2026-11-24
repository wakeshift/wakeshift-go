package wakeshift

// MCP Events for clients that speak it, at the
// HTTP layer (Handler): the events capability on server/discover and
// initialize (under both the draft's extension key and the top-level key
// ChatGPT reads), events/list, and events/subscribe, events/unsubscribe and
// events/poll passed to Wakeshift, which verifies the client's callback and
// POSTs occurrences to it, signed, in order. A tools-only event is refused
// here, on every request; so is a native client without a
// user, and one an event's Authorize refuses, asked on every request.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/wakeshift/wakeshift-go/internal/adapt"
	"github.com/wakeshift/wakeshift-go/internal/mcpevents/event"
)

// serveEvents answers one events/* request.
func (set *settings) serveEvents(w http.ResponseWriter, r *http.Request, post *postState, m rpcMessage) {
	a := set.app
	// The transport's standard headers come first, on every events/*
	// method (transportRefusal).
	if msg := transportRefusal(r.Header, m); msg != "" {
		headerMismatch(w, m.ID, msg)
		return
	}
	if m.Method == "events/list" {
		// To a native request while the core hands out subscribe vouchers,
		// the tools-only events too, each with a required wakeshift_voucher.
		// Asked first: whether push is served decides the delivery modes,
		// and a client push wakes is decided by what it declares.
		push := set.pushServed(r.Context())
		var app *string
		events := a.events()
		if len(a.openEvents()) < len(events) && nativeClient(r, m.Params, push) && a.vouchersOffered(r.Context()) {
			name := a.appName(r.Context())
			app = &name
		}
		defs := []any{}
		for _, t := range events {
			switch {
			case t.open():
				defs = append(defs, t.definition(push))
			case app != nil:
				defs = append(defs, t.definitionFor(app, push))
			}
		}
		rpcResult(w, m.ID, map[string]any{"resultType": event.ResultType, "events": defs})
		return
	}
	var p eventParams
	if err := json.Unmarshal(m.Params, &p); err != nil {
		// A name or delivery of the wrong type is refused below, each with
		// its own message (after the headers are checked); anything else
		// that isn't the params' shape, here.
		var te *json.UnmarshalTypeError
		if !errors.As(err, &te) || (te.Field != "name" && te.Field != "delivery" && te.Field != "delivery.mode") {
			rpcError(w, m.ID, event.InvalidParams, "invalid params", nil)
			return
		}
	}
	// params.name: a non-empty string, else InvalidParams (SEP-3415's
	// statically invalid request), once the headers are checked.
	var named struct {
		Name any `json:"name"`
	}
	json.Unmarshal(m.Params, &named)
	nameOK := false
	if s, ok := named.Name.(string); ok && s != "" {
		nameOK = true
	}
	base := wireCall{Meta: p.Meta, UserAgent: r.Header.Get("User-Agent")}
	if info, ok := p.Meta["io.modelcontextprotocol/clientInfo"].(map[string]any); ok {
		base.Client, _ = info["name"].(string)
	}
	// The dialect before identity: a refusal needs it, and identify runs
	// only once it's needed.
	dialect := event.DialectOf(base.Client, base.UserAgent, p.Meta)
	codes := event.CodesFor(dialect)
	msg := nameHeadersRefusal(r.Header.Values(event.NameHeader), p.Name)
	if t := a.eventByName(p.Name); msg == "" && t != nil && len(t.paramHeaders) > 0 {
		// Mcp-Param headers, strictly (paramHeaderRefusal). Events are served
		// only over HTTP (Handler), so there's always a request's headers.
		msg = paramHeaderRefusal(t.paramHeaders, paramArguments(m.Params), r.Header)
	}
	if msg != "" {
		headerMismatch(w, m.ID, msg)
		return
	}
	if !nameOK {
		rpcError(w, m.ID, event.InvalidParams, nameRequired, nil)
		return
	}
	if m.Method == "events/stream" {
		set.serveStream(w, r, post, m.ID, p, base, codes)
		return
	}
	t := a.eventByName(p.Name)
	if t == nil {
		nameJSON, _ := json.Marshal(p.Name)
		rpcError(w, m.ID, codes.NotFound, fill("native_unknown_event", "{event}", string(nameJSON)), map[string]string{"kind": "event"})
		return
	}
	// The voucher leaves the arguments before anything reads them
	// (Authorize, OnSubscribe, logs, the subscription key).
	var offered string
	p.Arguments, offered = takeVoucher(p.Arguments)
	creates := m.Method == "events/subscribe" || m.Method == "events/poll"
	// Only the app's tools subscribe to a tools-only event: every poll is
	// refused, and every subscribe but one with the subscribe voucher a
	// tool handed out, while the core hands them out. An open event ignores
	// a voucher.
	voucher := ""
	if !t.open() && m.Method != "events/poll" && offered != "" && a.vouchersOffered(r.Context()) {
		voucher = offered
	}
	if m.Method == "events/poll" && !t.open() {
		// Never a poll, voucher or not: a mode its events/list entry doesn't
		// offer (Unsupported); the refusal says a webhook is the way.
		rpcError(w, m.ID, codes.Unsupported, fill("native_tools_only_poll", "{event}", t.name),
			map[string]string{"feature": "deliveryMode", "value": "poll"})
		return
	}
	if creates && !t.open() && voucher == "" {
		rpcError(w, m.ID, codes.Forbidden, fill("native_tools_only", "{event}", t.name), nil)
		return
	}
	p.Arguments = t.known(p.Arguments)
	if m.Method == "events/subscribe" && p.Delivery.Mode == "" {
		rpcError(w, m.ID, event.InvalidParams, modeRequired, nil)
		return
	}
	if m.Method == "events/subscribe" && p.Delivery.Mode != "webhook" {
		rpcError(w, m.ID, codes.Unsupported, `delivery.mode must be "webhook": the only delivery offered`,
			map[string]string{"feature": "deliveryMode", "value": p.Delivery.Mode})
		return
	}
	id := post.identity(Request{ClientName: base.Client, Meta: p.Meta})
	id.logRequired(r.Context(), a, "")
	if id.refused != "" {
		rpcError(w, m.ID, codes.Forbidden, eventTools["native_no_user"], nil)
		return
	}
	call := id.id.apply(base)
	// With a voucher, no user is needed: the voucher is the authorization,
	// and Wakeshift compares the user when it was handed to one.
	if call.User == "" && voucher == "" {
		rpcError(w, m.ID, codes.Forbidden, eventTools["native_no_user"], nil)
		return
	}
	if voucher != "" {
		call.TransportSession = transportSession(r.Header.Get("Mcp-Session-Id"))
	}
	p.voucher = voucher
	// A poll has no subscribe step (each poll can start the subscription),
	// so every poll asks Authorize: access is checked again each time.
	if creates && t.authorize != nil {
		args, _ := json.Marshal(p.Arguments)
		sub := Subscriber{User: call.User, Client: call.Client, Native: true,
			Request: Request{Token: post.req.Token, Header: post.req.Header, ClientName: base.Client, Meta: p.Meta}}
		if !a.authorized(post.ctx, t, sub, args) {
			rpcError(w, m.ID, codes.Forbidden, fill("native_refused", "{event}", t.name), nil)
			return
		}
	}
	out, _, _, err := a.nativeCall(r.Context(), t, m.Method, call, dialect, p)
	if err != nil {
		mode := p.Delivery.Mode
		if m.Method == "events/poll" {
			mode = "poll"
		}
		code, msg, data := rpcErrorOf(err, codes, t.name, mode)
		rpcError(w, m.ID, code, msg, data)
		return
	}
	rpcResult(w, m.ID, out)
}

// nativeCall passes a native request on to Wakeshift and, when it started
// a subscription, runs OnSubscribe: a redeemed voucher's subscription is
// the handing-out tool's, with the Context its call gave Subscribe if this
// process remembers it (taken, so only this exact voucher's redemption
// ever gets it); any other is the native client's.
func (c *Client) nativeCall(ctx context.Context, t *eventType, method string, call wireCall, dialect string, p eventParams) (out map[string]any, created bool, done <-chan struct{}, err error) {
	out, created, tool, err := c.eventCall(ctx, method, call, dialect, p)
	if err != nil || !created {
		return out, created, nil, err
	}
	args, _ := json.Marshal(p.Arguments)
	if tool != "" {
		value, _ := c.vouchers.take(p.voucher, c.now())
		return out, true, c.onSubscribed(ctx, t, args, subscribedInfo{user: call.User, via: viaTool, tool: tool, value: value}), nil
	}
	return out, true, c.onSubscribed(ctx, t, args, subscribedInfo{user: call.User, via: viaNative}), nil
}

// apply puts the identity in a call: the user, the agent's token (whose
// user is the agent's) if there's an agent, and the client's name if the
// call has none.
func (id Identity) apply(call wireCall) wireCall {
	call.User = id.User
	if call.Client == "" {
		call.Client = id.Client
	}
	if id.Agent != nil {
		call.User, call.AgentToken = id.Agent.User(), id.Agent.token
	}
	return call
}

// nativeClient reports whether an HTTP request comes from a client that
// subscribes with MCP Events itself (nativeClientRule), from what the
// request carries: Handler sees no session's initialize.
// servesPush: push is listed for this request (nativeClientRule).
func nativeClient(r *http.Request, params json.RawMessage, servesPush bool) bool {
	var p struct {
		Meta map[string]any `json:"_meta"`
	}
	json.Unmarshal(params, &p)
	var client string
	if info, ok := p.Meta["io.modelcontextprotocol/clientInfo"].(map[string]any); ok {
		client, _ = info["name"].(string)
	}
	return nativeClientRule(client, r.Header.Get("User-Agent"), p.Meta, nil, servesPush)
}

// listsNative reports whether a tools/list comes from a client that
// subscribes with MCP Events itself, from the request as the MCP library
// gives it: its _meta's client name and capabilities, else its session's
// initialize (r.Client, r.Extensions), so a legacy-protocol session that
// declared the extension only in initialize (opencode's default config)
// counts as the 2026-07-28 request that declares it does. Handler sees
// only the request's _meta, not the session's initialize.
func (at *Attachment) listsNative(ctx context.Context, r adapt.Request) bool {
	var ua string
	if r.Header != nil {
		ua = r.Header.Get("User-Agent")
	}
	return nativeClientRule(r.Client, ua, r.Meta, r.Extensions, at.set.servesPushFor(ctx, r.Client, ua, r.Meta))
}

// requestClient is the client name a request's params carry (_meta's
// clientInfo) and its User-Agent.
func requestClient(r *http.Request, params json.RawMessage) (client, userAgent string) {
	client, _ = messageClient(params)
	return client, r.Header.Get("User-Agent")
}

// nativeClientRule is whether a client subscribes with MCP Events itself,
// the one rule on every path (tools/list, events/list, call.subscribe,
// after the app's IsNativeClient) and in every SDK: the client table
// (event.EvaluateClient), pinned by
// sdk/typescript/test/mcp-events-interop.json native_client. Its first
// matching entry decides (ChatGPT's desktop app never; ChatGPT and OpenClaw
// without declaring; a harness Wakeshift wakes itself never); otherwise a
// client that declares the extension, in the request's _meta or its
// session's initialize (sessionExtensions, nil without one). servesPush:
// push would be listed for this request (events/list's rule); then the
// table's without_push entries (opencode) stop applying, and capabilities
// decide.
func nativeClientRule(client, userAgent string, meta, sessionExtensions map[string]any, servesPush bool) bool {
	return event.EvaluateClient(event.ClientInput{Client: client, UserAgent: userAgent, Meta: meta,
		SessionExtensions: sessionExtensions, ServesPush: servesPush}).Native
}

// headerMismatch answers a request refused for its headers: HTTP 400 and
// HeaderMismatch (-32020), as the 2026-07-28 transport's Server Validation
// has it.
func headerMismatch(w http.ResponseWriter, id json.RawMessage, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{
		"code": event.HeaderMismatch, "message": msg}})
}

// modernProtocol is the first protocol version whose requests carry
// MCP-Protocol-Version and Mcp-Method on every request, and _meta's
// protocolVersion (go-sdk compares versions as strings too).
const modernProtocol = "2026-07-28"

// transportRefusal is why an events/* request is refused for the
// 2026-07-28 Streamable HTTP transport's standard headers (HTTP 400,
// HeaderMismatch), or "": Handler answers these methods before the MCP
// library, so its own checks never run on them. A request whose _meta
// names a protocol version needs MCP-Protocol-Version, equal to it; a
// 2026-07-28 request (the header or _meta says so) needs Mcp-Method; and
// an Mcp-Method that's sent, on any request, must be the body's method,
// exactly. A legacy request (an initialize session's) may leave Mcp-Method
// out. The same rule in every SDK (sdk/typescript/test/mcp-events-interop.json
// transport_headers).
func transportRefusal(h http.Header, m rpcMessage) string {
	var p struct {
		Meta map[string]any `json:"_meta"`
	}
	json.Unmarshal(m.Params, &p)
	metaVersion, _ := p.Meta["io.modelcontextprotocol/protocolVersion"].(string)
	versions := h.Values("MCP-Protocol-Version")
	if len(versions) > 1 {
		return "Header mismatch: more than one MCP-Protocol-Version header"
	}
	version := ""
	if len(versions) == 1 {
		version = strings.Trim(versions[0], " \t")
	}
	if metaVersion != "" {
		if len(versions) == 0 {
			return "Header mismatch: MCP-Protocol-Version is missing"
		}
		if version != metaVersion {
			return "Header mismatch: MCP-Protocol-Version doesn't match _meta's protocolVersion"
		}
	}
	modern := version >= modernProtocol || metaVersion >= modernProtocol
	methods := h.Values("Mcp-Method")
	switch {
	case len(methods) > 1:
		return "Header mismatch: more than one Mcp-Method header"
	case len(methods) == 0:
		if modern {
			return "Header mismatch: Mcp-Method is missing"
		}
	case strings.Trim(methods[0], " \t") != m.Method:
		return "Header mismatch: Mcp-Method doesn't match the method"
	}
	return ""
}

// streamRefusal is events/stream's answer, Unsupported with value "push",
// where push isn't served.
const streamRefusal = "push delivery isn't offered: use a webhook (events/subscribe) or poll (events/poll)"

// nameHeadersRefusal is nameHeaderRefusal over every Mcp-Name field the
// request has: more than one is refused, since a gateway may read another
// than the one checked here.
func nameHeadersRefusal(values []string, name string) string {
	if len(values) > 1 {
		return "Header mismatch: more than one Mcp-Name header"
	}
	if len(values) == 0 {
		return ""
	}
	return nameHeaderRefusal(values[0], name)
}

// nameHeaderRefusal is why an events/subscribe, events/unsubscribe,
// events/poll or events/stream over HTTP is refused for its Mcp-Name
// header (HTTP 400, HeaderMismatch), or "" if it isn't: one that's sent
// must decode (event.HeaderValue) to params.name, from any client, whatever
// it claims to be. A missing one is accepted from every client: SEP-3415
// and SEP-2243 say every client must send it, but ChatGPT, OpenClaw, the
// Vercel AI SDK, Hermes and Mastra don't yet (a documented deviation).
// header: the request's Mcp-Name ("" for none). The same rule in every SDK
// (sdk/typescript/test/mcp-events-interop.json mcp_name).
func nameHeaderRefusal(header, name string) string {
	if header == "" {
		return ""
	}
	if v, ok := event.HeaderValue(strings.Trim(header, " \t")); !ok || v != name {
		return "Header mismatch: Mcp-Name doesn't match params.name"
	}
	return ""
}

// declaresEventsExtension reports whether a request's _meta declares the
// MCP Events extension in its capabilities, as a 2026-07-28 client does
// on every request.
func declaresEventsExtension(meta map[string]any) bool {
	caps, _ := meta["io.modelcontextprotocol/clientCapabilities"].(map[string]any)
	ext, _ := caps["extensions"].(map[string]any)
	_, ok := ext[event.ExtensionKey]
	return ok
}

// eventParams are events/subscribe's, events/unsubscribe's and
// events/poll's params.
type eventParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
	Delivery  struct {
		Mode   string `json:"mode"`
		URL    string `json:"url"`
		Secret string `json:"secret,omitempty"`
	} `json:"delivery"`
	TTLMs     *int64  `json:"ttlMs"`
	Cursor    *string `json:"cursor"`
	MaxAgeMs  *int64  `json:"maxAgeMs"`
	MaxEvents *int    `json:"maxEvents"`
	// HeartbeatMs is events/stream's heartbeat suggestion.
	HeartbeatMs *int64         `json:"heartbeatMs"`
	Meta        map[string]any `json:"_meta"`
	// voucher is the subscribe voucher the client passed for a tools-only
	// event (wakeshift_voucher, taken out of Arguments), sent as voucher.
	voucher string
}

// transportSession is the SHA-256 (hex) of a request's Mcp-Session-Id, or
// "" without one: what binds an anonymous subscribe voucher to the
// transport session it was handed out in.
func transportSession(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// errNoSubscription is an events/unsubscribe that matched nothing.
var errNoSubscription = errors.New("no subscription matches")

// eventCall passes a native request on to Wakeshift; created: it started a
// subscription (events/subscribe, or a poll's first); tool: the tool that
// handed out the voucher it redeemed, if it did.
func (c *Client) eventCall(ctx context.Context, method string, call wireCall, dialect string, p eventParams) (map[string]any, bool, string, error) {
	if method == "events/poll" {
		res, created, err := c.pollCall(ctx, call, p)
		return res, created, "", err
	}
	body := map[string]any{"call": call, "name": p.Name, "arguments": p.Arguments, "delivery": p.Delivery, "dialect": dialect}
	if p.voucher != "" {
		body["voucher"] = p.voucher
	}
	if method == "events/unsubscribe" {
		var out struct {
			Ended bool `json:"ended"`
		}
		if err := c.eventsCall(ctx, "POST", "/api/apps/v1/events/unsubscribe", body, &out); err != nil {
			return nil, false, "", err
		}
		// The draft says NotFound for a key that matches nothing; ChatGPT
		// gets the empty result it always has.
		if !out.Ended && dialect != event.DialectChatGPT {
			return nil, false, "", errNoSubscription
		}
		return map[string]any{"resultType": event.ResultType}, false, "", nil
	}
	if p.TTLMs != nil {
		body["ttl_ms"] = *p.TTLMs
	}
	if p.Cursor != nil {
		body["cursor"] = *p.Cursor
	}
	if p.MaxAgeMs != nil {
		body["max_age_ms"] = *p.MaxAgeMs
	}
	var out struct {
		ID             string          `json:"id"`
		RefreshBefore  time.Time       `json:"refresh_before"`
		Cursor         string          `json:"cursor"`
		Truncated      bool            `json:"truncated"`
		DeliveryStatus json.RawMessage `json:"delivery_status"`
		Created        bool            `json:"created"`
		Via            string          `json:"via"`
		Tool           string          `json:"tool"`
	}
	if err := c.eventsCall(ctx, "POST", "/api/apps/v1/events/subscribe", body, &out); err != nil {
		return nil, false, "", err
	}
	tool := ""
	if rest, ok := strings.CutPrefix(out.Via, "tool:"); ok {
		tool = cmp.Or(out.Tool, rest)
	}
	res := map[string]any{"resultType": event.ResultType, "id": out.ID, "refreshBefore": out.RefreshBefore.UTC().Format(time.RFC3339),
		"cursor": out.Cursor, "truncated": out.Truncated}
	if len(out.DeliveryStatus) > 0 && string(out.DeliveryStatus) != "null" {
		res["deliveryStatus"] = out.DeliveryStatus
	}
	return res, out.Created, tool, nil
}

// pollCall passes an events/poll on to Wakeshift, which keeps the
// subscription the first poll starts and the events waiting for it
// (sdk/typescript/test/mcp-events-wire.json has both sides of each).
func (c *Client) pollCall(ctx context.Context, call wireCall, p eventParams) (map[string]any, bool, error) {
	body := map[string]any{"call": call, "name": p.Name, "arguments": p.Arguments}
	if p.Cursor != nil {
		body["cursor"] = *p.Cursor
	}
	if p.MaxAgeMs != nil {
		body["max_age_ms"] = *p.MaxAgeMs
	}
	if p.MaxEvents != nil {
		body["max_events"] = *p.MaxEvents
	}
	var out struct {
		Events     []json.RawMessage `json:"events"`
		Cursor     string            `json:"cursor"`
		Truncated  bool              `json:"truncated"`
		HasMore    bool              `json:"has_more"`
		NextPollMs int64             `json:"next_poll_ms"`
		Created    bool              `json:"created"`
	}
	if err := c.eventsCall(ctx, "POST", "/api/apps/v1/events/poll", body, &out); err != nil {
		return nil, false, err
	}
	if out.Events == nil {
		out.Events = []json.RawMessage{}
	}
	return map[string]any{"resultType": event.ResultType, "events": out.Events, "cursor": out.Cursor, "truncated": out.Truncated,
		"hasMore": out.HasMore, "nextPollMs": out.NextPollMs}, out.Created, nil
}

// rpcInternal is JSON-RPC's internal error.
const rpcInternal = -32603

// nameRequired and modeRequired are the InvalidParams messages for a
// request without params.name, and an events/subscribe without
// delivery.mode (sdk/typescript/test/mcp-events-wire.json next.refused).
const (
	nameRequired = "params.name is required: the name of the event"
	modeRequired = `delivery.mode is required: "webhook", the only delivery offered`
)

// rpcErrorOf is the JSON-RPC error for a failed call, in the client's
// dialect's codes. A conversation that isn't the caller's is Forbidden,
// whether it exists or not, so a handle can't be probed. mode: the
// delivery mode the request asked for ("poll" for events/poll, the
// delivery's for events/subscribe), which Unsupported names.
func rpcErrorOf(err error, codes event.Codes, name, mode string) (code int, msg string, data any) {
	if errors.Is(err, errNoSubscription) {
		return codes.NotFound, "no subscription matches that event, arguments and delivery URL", map[string]string{"kind": "subscription"}
	}
	var e *APIError
	if !errors.As(err, &e) || e.Code == "unreachable" {
		return rpcInternal, "Wakeshift couldn't be reached", nil
	}
	switch e.Code {
	case "bad_voucher":
		// One answer, whatever the reason.
		return codes.Forbidden, fill("native_bad_voucher", "{event}", name), nil
	case "callback_failed":
		if e.hint != nil { // the core's, as it is (an unreachable callback)
			return codes.CallbackEndpoint, e.Message, map[string]any{"reason": e.Reason, "hint": e.hint.data}
		}
		return codes.CallbackEndpoint, e.Message, map[string]string{"reason": e.Reason}
	case "unknown_event", "not_found":
		return codes.NotFound, e.Message, map[string]string{"kind": "event"}
	case "unsupported":
		if mode == "" {
			return codes.Unsupported, e.Message, map[string]string{"feature": "deliveryMode"}
		}
		return codes.Unsupported, e.Message, map[string]string{"feature": "deliveryMode", "value": mode}
	case "subscription_limit":
		return codes.ResourceExhausted, e.Message, map[string]string{"limit": "subscriptions"}
	case "subscription_stopped":
		return codes.Forbidden, e.Message, map[string]string{"reason": "stopped"}
	case "bad_request", "bad_arguments":
		if e.hint != nil {
			return event.InvalidParams, e.Message, map[string]any{"hint": e.hint.data}
		}
		return event.InvalidParams, e.Message, nil
	case "unknown_session", "other_client", "unknown_conversation":
		return codes.Forbidden, "another client of yours holds that subscription", nil
	}
	return rpcInternal, "Wakeshift refused the subscription: " + e.Message, nil
}

func rpcResult(w http.ResponseWriter, id json.RawMessage, result any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func rpcError(w http.ResponseWriter, id json.RawMessage, code int, msg string, data any) {
	e := map[string]any{"code": code, "message": msg}
	if data != nil {
		e["data"] = data
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": e})
}

// rewrite serves a request with next, passing each JSON-RPC message of the
// answer (a JSON one, or an SSE one's data lines) through f. A vendor's own
// handler answers too (a server/discover it serves itself): an answer
// without a Content-Type is tried as JSON, and an SSE data line may omit
// the space after "data:". f leaves anything it can't parse as it was.
func rewrite(next http.Handler, w http.ResponseWriter, r *http.Request, f func([]byte) []byte) {
	rec := &recorder{header: http.Header{}, code: http.StatusOK}
	next.ServeHTTP(rec, r)
	out := rec.body.Bytes()
	switch ct := rec.header.Get("Content-Type"); {
	case ct == "", strings.HasPrefix(ct, "application/json"):
		out = f(out)
	case strings.HasPrefix(ct, "text/event-stream"):
		lines := bytes.Split(out, []byte("\n"))
		for i, l := range lines {
			if data, ok := bytes.CutPrefix(l, []byte("data:")); ok {
				data = bytes.TrimPrefix(data, []byte(" "))
				lines[i] = append([]byte("data: "), f(data)...)
			}
		}
		out = bytes.Join(lines, []byte("\n"))
	}
	maps.Copy(w.Header(), rec.header)
	w.Header().Del("Content-Length")
	w.WriteHeader(rec.code)
	w.Write(out)
}

// recorder buffers a handler's answer, for rewrite.
type recorder struct {
	header http.Header
	code   int
	wrote  bool
	body   bytes.Buffer
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(code int) {
	if !r.wrote {
		r.code, r.wrote = code, true
	}
}

func (r *recorder) Write(b []byte) (int, error) {
	r.wrote = true
	return r.body.Write(b)
}

// Flush is a no-op: the answer is sent once it's rewritten.
func (r *recorder) Flush() {}

// withEventsCapability is a discover or initialize answer with the events
// capability added, unless the server declares MCP Events itself: then
// Wakeshift serves none, and says so once (events_capability_kept). It's
// added whenever the client has a key (Handler serves events/* only then)
// and the app defines any event, open or tools-only (a tools-only one is
// subscribed to with a voucher); with no event defined, it isn't.
func (set *settings) withEventsCapability(ctx context.Context, b []byte) []byte {
	if set.eventsKept.Load() {
		return b
	}
	if declaresEvents(b) {
		set.eventsKept.Store(true)
		set.app.logOnce(ctx, "events_capability_kept", "", "the server declares MCP Events itself, so Wakeshift serves none and leaves its capability as it is")
		return b
	}
	// Any event: one open to MCP Events clients, or a tools-only one, which
	// a native client subscribes to with the subscribe voucher a tool handed
	// it, while the core hands them out.
	if len(set.app.events()) == 0 {
		return b
	}
	return addEventsCapability(b)
}

// declaresEvents reports whether a discover or initialize answer already
// declares MCP Events.
func declaresEvents(b []byte) bool {
	var resp struct {
		Result struct {
			Capabilities map[string]json.RawMessage `json:"capabilities"`
		} `json:"result"`
	}
	if json.Unmarshal(b, &resp) != nil {
		return false
	}
	caps := resp.Result.Capabilities
	if _, ok := caps[event.TopLevelKey]; ok {
		return true
	}
	var ext map[string]json.RawMessage
	json.Unmarshal(caps["extensions"], &ext)
	_, ok := ext[event.ExtensionKey]
	return ok
}

// addEventsCapability adds the capability to a JSON-RPC result that has
// capabilities (anything else comes back as it was): the draft's
// extensions["io.modelcontextprotocol/events"], and the top-level "events"
// ChatGPT reads.
func addEventsCapability(b []byte) []byte {
	var resp map[string]any
	if json.Unmarshal(b, &resp) != nil {
		return b
	}
	res, ok := resp["result"].(map[string]any)
	if !ok {
		return b
	}
	caps, _ := res["capabilities"].(map[string]any)
	if caps == nil {
		caps = map[string]any{}
	}
	settings := map[string]any{"listChanged": false}
	caps[event.TopLevelKey] = settings
	ext, _ := caps["extensions"].(map[string]any)
	if ext == nil {
		ext = map[string]any{}
	}
	ext[event.ExtensionKey] = settings
	caps["extensions"] = ext
	res["capabilities"] = caps
	out, err := json.Marshal(resp)
	if err != nil {
		return b
	}
	return out
}
