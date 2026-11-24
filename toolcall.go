package wakeshift

// One call of a tool that subscribes: its *ToolCall,
// which event.Subscribe takes to subscribe the calling conversation. The
// attachment adds one Wakeshift block to whatever the tool returns, so a
// conversation can't be left unwakeable by a forgotten argument or an
// unreturned result, and a call that fails ends the subscriptions it made.

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wakeshift/wakeshift-go/internal/adapt"
	"github.com/wakeshift/wakeshift-go/internal/mcpevents/event"
)

// ToolCall is one call of a tool that subscribes: who's calling, and the
// conversation event.Subscribe subscribes. Printing or logging it never
// shows the request or the session.
type ToolCall struct {
	// Request is go-sdk's request (nil under mcp-go, whose handler takes
	// its own).
	Request *mcp.CallToolRequest
	// User is who Identify says is calling ("" for none, and always on a
	// disabled client). It's Wakeshift's view of the caller: your own
	// authorization rests on your own verified credential, never on it.
	User string
	// Agent is the agent the app acts for (Identity.Agent), if any.
	Agent *Agent
	// Tool is the tool being called.
	Tool string
	// Native: the client subscribes to the app's events itself (MCP
	// Events). Subscribe then makes no conversation subscription, and the
	// block tells it what to subscribe to.
	Native bool
	// NativeConversation is a native client's conversation, when it names
	// one, else "".
	NativeConversation string
	// Recognized is what Wakeshift recognized the agent's program as: nil
	// until a Subscribe or ResolveSession.
	Recognized *Recognized

	session  *SessionKey // the conversation's, once Wakeshift resolved it; behind a pointer, so printing the fields never shows it
	passed   *SessionKey // the session_key the agent passed: only sent to Wakeshift, never shown
	s        *callScope
	disabled bool
	app      *Client      // a disabled call's
	decl     *declaration // a disabled call's
	stand    *standIn     // a stand-in's (wakeshifttest.NewToolCall)
	// pushWoken: the client is one push wakes (opencode): its
	// subscribes go to the core whatever the SDK decided, which answers
	// native_line when it chose native push (Wakeshift's plugin doesn't
	// listen for the conversation), and keeps the bridged path otherwise
	// (the downgrade).
	pushWoken bool
	// eventsExt: the request declares the MCP Events extension (its
	// _meta's capabilities, or its session's initialize): sent as
	// call.events_ext.
	eventsExt bool
	// pushOn: this server serves push for the call, if the core does
	// (settings.pushOn): events_ext goes only then.
	pushOn bool
}

// Session is the conversation's session key, set once Wakeshift resolved
// the conversation (a Subscribe that reached it, or ResolveSession): ""
// until then, and never the raw session_key argument, which anyone can
// make up. It prints as [redacted]; string(call.Session()) is the key.
func (c *ToolCall) Session() SessionKey {
	if c == nil || c.session == nil {
		return ""
	}
	return *c.session
}

// wireSession is the session the call names to Wakeshift: the resolved
// one, else the one the agent passed.
func (c *ToolCall) wireSession() string {
	if k := c.Session(); k != "" {
		return string(k)
	}
	if c.passed != nil {
		return string(*c.passed)
	}
	return ""
}

func (c *ToolCall) setSession(k string) { v := SessionKey(k); c.session = &v }

// Wakes reports whether Wakeshift wakes this conversation for what the
// call subscribed: skip your own polling or waiting only when it's true;
// otherwise behave as without Wakeshift. Set by Subscribe; false before.
//
// It's true while a subscription a Subscribe of this call made or renewed
// is still live (not ended by the call's Unsubscribe) and Wakeshift said
// a wake is confirmed, as of the latest such Subscribe: the conversation's
// agent program can be woken, with no choice from the user still to come
// (false while Claude Code's question of how to be notified is pending); and for a native caller (a ChatGPT Work chat on the web,
// woken through MCP Events) once a Subscribe offered it a line the call
// didn't end. It's false for a nil call, a disabled client's call, a
// native caller whose Subscribe came back SubscriptionUnavailable, a
// Subscribe that returned an error, and an older Wakeshift that doesn't
// say. Safe for concurrent use.
func (c *ToolCall) Wakes() bool {
	switch {
	case c == nil || c.disabled:
		return false
	case c.stand != nil:
		return c.stand.wakesNow()
	case c.s == nil:
		return false
	}
	return c.s.wakesNow()
}

// CallLimit is how long the conversation's agent program lets one tool
// call take before it gives up on it, from what Wakeshift recognized the
// program as (Claude Code: 60 seconds, Codex: 300), or 0 when that isn't
// known. Set by Subscribe, from the latest that made or renewed a
// subscription; 0 before. It's the program's default: a user's own setting
// may raise it. Safe for concurrent use.
func (c *ToolCall) CallLimit() time.Duration {
	switch {
	case c == nil || c.disabled:
		return 0
	case c.stand != nil:
		return c.stand.limitNow()
	case c.s == nil:
		return 0
	}
	c.s.wmu.Lock()
	defer c.s.wmu.Unlock()
	return c.s.callLimit
}

// ShouldSkipWait reports whether to skip a wait the agent asked for (a
// vendor's wait_seconds) and return at once: true only when Wakes is true
// and wait reaches CallLimit, so the program would cut the call off before
// the wait ends. Keep the subscription either way. A wait of 0 (none asked
// for) is false.
func (c *ToolCall) ShouldSkipWait(wait time.Duration) bool {
	return skipsWait(c.Wakes(), c.CallLimit(), wait)
}

// skipsWait is ShouldSkipWait's rule (skip-wait.json): skip a wait the
// agent asked for only when Wakeshift wakes the conversation and the wait
// reaches the program's per-call limit.
func skipsWait(wakes bool, limit, wait time.Duration) bool {
	return wakes && limit > 0 && wait > 0 && wait >= limit
}

// callLimitOf is a core's call_limit in seconds as a duration, 0 for none
// or anything but a positive number.
func callLimitOf(raw json.RawMessage) time.Duration {
	var secs float64
	if len(raw) == 0 || json.Unmarshal(raw, &secs) != nil || secs <= 0 || secs > float64(math.MaxInt64/int64(time.Second)) {
		return 0
	}
	return time.Duration(secs * float64(time.Second))
}

func (c *ToolCall) String() string {
	if c == nil {
		return "wakeshift.ToolCall(nil)"
	}
	return fmt.Sprintf("wakeshift.ToolCall{tool: %s, user: %q, native: %t}", c.Tool, c.User, c.Native)
}

// GoString is String, for %#v.
func (c *ToolCall) GoString() string { return c.String() }

// Format prints String for every verb.
func (c *ToolCall) Format(f fmt.State, _ rune) { fmt.Fprint(f, c.String()) }

// LogValue logs the call without its request or session.
func (c *ToolCall) LogValue() slog.Value {
	if c == nil {
		return slog.StringValue("nil")
	}
	return slog.GroupValue(slog.String("tool", c.Tool), slog.String("user", c.User), slog.Bool("native", c.Native))
}

// SubscriptionState is what a Subscribe did.
type SubscriptionState string

// What a Subscribe did.
const (
	// SubscriptionCreated: a new conversation subscription.
	SubscriptionCreated SubscriptionState = "created"
	// SubscriptionRenewed: the conversation's subscription with these
	// arguments, renewed.
	SubscriptionRenewed SubscriptionState = "renewed"
	// SubscriptionNative: the client subscribes itself (MCP Events); the
	// block tells it what to subscribe to.
	SubscriptionNative SubscriptionState = "native"
	// SubscriptionUnavailable: nothing happened; Reason says why.
	SubscriptionUnavailable SubscriptionState = "unavailable"
	// SubscriptionDisabled: the client is disabled.
	SubscriptionDisabled SubscriptionState = "disabled"
)

// Why a Subscribe was unavailable (Subscription.Reason).
const (
	// UnavailableClientUnsupported: an MCP Events client and an event only
	// the app's tools may subscribe to.
	UnavailableClientUnsupported = "client_unsupported"
	// UnavailableNoUser: an MCP Events client and no signed-in user.
	UnavailableNoUser = "no_user"
	// UnavailableRefused: the event's Authorize refused this caller.
	UnavailableRefused = "refused"
)

// Subscription is what Subscribe did. ID and User address a created or
// renewed one later: an Emit's ToSubscription(User, ID).
type Subscription struct {
	State SubscriptionState `json:"state"`
	ID    string            `json:"id,omitempty"`
	User  string            `json:"user,omitempty"`
	// Session is the conversation's session key (it prints as [redacted]).
	Session SessionKey `json:"session,omitempty"`
	// Recognized is what Wakeshift recognized the agent's program as.
	Recognized Recognized `json:"recognized"`
	// Reason is why it's unavailable.
	Reason string `json:"reason,omitempty"`
}

// SubscribeOption shapes a Subscribe.
type SubscribeOption func(*subscribeOptions)

type subscribeOptions struct {
	ttl, replay   time.Duration
	replaySet     bool
	once          bool
	reason, label string
	context       any // Context's: never sent, never logged
}

// Context passes v to the event's OnSubscribe for the subscription this
// Subscribe makes (Subscribed.Context): the caller's credential, say, for
// a Watch that checks your API as the caller. v is yours: it's never sent
// to Wakeshift, logged or printed. For a native client handed a subscribe
// voucher, OnSubscribe gets v when the client redeems the voucher in this
// process within four hours; a redemption on another instance gets nil.
func Context(v any) SubscribeOption { return func(o *subscribeOptions) { o.context = v } }

// TTL ends the subscription after this long without a delivery or a fresh
// subscribe (default 30 days).
func TTL(d time.Duration) SubscribeOption { return func(o *subscribeOptions) { o.ttl = d } }

// Reason says why the app subscribes the conversation, shown in the console
// (at most 200 characters).
func Reason(r string) SubscribeOption { return func(o *subscribeOptions) { o.reason = r } }

// Label names a new session (agent@label), for an app acting for its
// agents.
func Label(l string) SubscribeOption { return func(o *subscribeOptions) { o.label = l } }

// Replay also delivers the latest occurrence with these arguments from this
// far back (more than 0, at most 24 hours), so one emitted just before the
// subscribe isn't missed: the answer when the job's ID exists only after
// it starts. Only on creation, not a renewal.
func Replay(d time.Duration) SubscribeOption {
	return func(o *subscribeOptions) { o.replay, o.replaySet = d, true }
}

// Once ends the subscription after its first delivery: "tell me when this
// finishes".
func Once() SubscribeOption { return func(o *subscribeOptions) { o.once = true } }

// maxReplay is the longest Replay.
const maxReplay = 24 * time.Hour

// scopeKey and disabledCallKey are the context keys of a call's scope and
// of a disabled client's call.
type (
	scopeKey        struct{}
	disabledCallKey struct{}
)

// callScope is one call of a tool that subscribes: what its handler
// subscribed, for the block and the rollback.
type callScope struct {
	at    *Attachment
	req   adapt.Request
	goReq *mcp.CallToolRequest // req's, when go-sdk serves it
	call  *ToolCall
	decl  *declaration

	mu    sync.Mutex // held for each request: one at a time per call
	ended bool

	id      resolved
	wake    wakeArgs
	pending string // the call's pending-addition record
	armed   bool   // a subscribe made or renewed a subscription
	created []createdSub
	native  []nativeLine
	// notes are Wakeshift's from the call's unsubscribes, for the block.
	notes []string
	// requestsOff: attach's DisableClientRequests (clientrequests.go).
	requestsOff bool

	// wmu guards what Watch reads of the call (it may run in another
	// goroutine): the state of each Subscribe by event and arguments, the
	// subscriptions the call made or renewed that are still live, the
	// watches it started for subscriptions it created, whether it was
	// rolled back, the core's wakes from the latest Subscribe that made or
	// renewed one, and the native lines the call offered and didn't end
	// (Wakes).
	wmu        sync.Mutex
	states     map[string]SubscriptionState
	live       map[string]bool
	watches    map[string]context.CancelCauseFunc
	rolledBack bool
	wakes      bool
	// callLimit: the core's call_limit from the latest Subscribe that made
	// or renewed one (CallLimit).
	callLimit time.Duration
	// subscribeFailed: a Subscribe of the call failed, so Wakes is false (the tool keeps its own wait).
	subscribeFailed bool
	nativeLive      map[string]bool
}

// subKey is an event and its arguments, canonical: how the call keeps its
// subscriptions apart.
func subKey(name string, args json.RawMessage) string { return name + "\x00" + compact(args) }

// noteState records what a Subscribe of the call did.
func (s *callScope) noteState(key string, st SubscriptionState) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.states == nil {
		s.states, s.live = map[string]SubscriptionState{}, map[string]bool{}
	}
	s.states[key] = st
	switch st {
	case SubscriptionCreated, SubscriptionRenewed:
		s.live[key] = true
	case SubscriptionNative:
		if s.nativeLive == nil {
			s.nativeLive = map[string]bool{}
		}
		s.nativeLive[key] = true
	}
}

// noteNativeEnded records that an Unsubscribe of the call ended what it
// offered a native caller.
func (s *callScope) noteNativeEnded(key string) {
	s.wmu.Lock()
	delete(s.nativeLive, key)
	s.wmu.Unlock()
}

// noteWakes records the core's wakes from a Subscribe that made or renewed
// a subscription.
func (s *callScope) noteWakes(w bool, limit time.Duration) {
	s.wmu.Lock()
	s.wakes, s.callLimit = w, limit
	s.wmu.Unlock()
}

// wakesNow is Wakes for a call the SDK serves: the latest wakes, while a
// subscription the call made or renewed is still live, or a native line
// the call offered and didn't end.
func (s *callScope) wakesNow() bool {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return !s.rolledBack && !s.subscribeFailed && (s.wakes && len(s.live) > 0 || len(s.nativeLive) > 0)
}

// noteEnded records that an Unsubscribe of the call ended its subscription.
func (s *callScope) noteEnded(key string) {
	s.wmu.Lock()
	delete(s.live, key)
	s.wmu.Unlock()
	s.created = slices.DeleteFunc(s.created, func(c createdSub) bool { return subKey(c.name, c.arguments) == key })
}

// endedAll reports whether the call subscribed and every subscription it
// made or renewed has since ended (an Unsubscribe in the same call).
func (s *callScope) endedAll() bool {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return s.armed && len(s.live) == 0
}

// watchFor ties a Watch of key started in this call to it: false if the
// call's Subscribe to key came back unavailable or disabled, or the call
// was rolled back (don't watch). A Watch for a subscription the call
// created is stopped if the call is rolled back.
func (s *callScope) watchFor(key string, stop context.CancelCauseFunc) bool {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.rolledBack {
		return false
	}
	switch s.states[key] {
	case SubscriptionUnavailable, SubscriptionDisabled:
		return false
	}
	if slices.ContainsFunc(s.created, func(c createdSub) bool { return subKey(c.name, c.arguments) == key }) {
		if s.watches == nil {
			s.watches = map[string]context.CancelCauseFunc{}
		}
		s.watches[key] = stop
	}
	return true
}

// unwatch forgets a Watch of key that ended.
func (s *callScope) unwatch(key string) {
	s.wmu.Lock()
	delete(s.watches, key)
	s.wmu.Unlock()
}

// stopWatches marks the call rolled back and stops the watches it started
// for subscriptions it created.
func (s *callScope) stopWatches() {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.rolledBack = true
	for _, stop := range s.watches {
		stop(errRolledBack)
	}
}

// createdSub is a subscription the call made: ended if the call fails.
type createdSub struct {
	name      string
	arguments json.RawMessage
	session   string
	pending   string
	// ctx is the ctx of the Subscribe that made (or last renewed) it,
	// without its cancellation, and value its Context: what OnSubscribe
	// gets once the call succeeded.
	ctx   context.Context
	value any
}

// nativeLine is an event and arguments a native caller is told to
// subscribe to (/calls/addition's native).
type nativeLine struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	// Subscribe's options, for a tools-only event's line: the core keeps
	// them with the subscribe voucher and applies them when it's redeemed
	// Seconds, as /event-subscriptions takes them.
	Once   bool     `json:"once,omitempty"`
	TTL    *float64 `json:"ttl,omitempty"`
	Replay *float64 `json:"replay,omitempty"`
	// context is the Subscribe's Context, never sent: remembered with the
	// voucher the line hands out (vouchers.go).
	context    any
	hasContext bool
}

// nativeLine is a native line's options from a Subscribe's: once, ttl and
// replay, and its Context, remembered with a voucher the line hands out.
func (o subscribeOptions) nativeLine() nativeLine {
	line := nativeLine{Once: o.once, context: o.context, hasContext: o.context != nil}
	if o.ttl > 0 {
		v := o.ttl.Seconds()
		line.TTL = &v
	}
	if o.replaySet {
		v := o.replay.Seconds()
		line.Replay = &v
	}
	return line
}

// maxNativeLines is the most native lines Wakeshift takes for one call.
const maxNativeLines = 20

// newScope is the scope of one call of a tool that subscribes, before its
// handler runs: who's calling (Identify, lazily, once per POST) and its
// ToolCall.
func (at *Attachment) newScope(ctx context.Context, r adapt.Request, wake wakeArgs, decl *declaration, post *postState) *callScope {
	s := &callScope{at: at, req: r, wake: wake, decl: decl}
	s.goReq, _ = r.Lib.(*mcp.CallToolRequest)
	set := at.set
	if post != nil {
		set = post.set
	}
	s.requestsOff = set.noRequests
	s.id = set.identityFor(ctx, post, requestFor(r))
	s.id.logRequired(ctx, at.app, r.Tool)
	c := &ToolCall{Request: s.goReq, Tool: r.Tool, s: s}
	if s.id.refused == "" {
		c.User, c.Agent = s.id.user(), s.id.id.Agent
	}
	if wake.SessionKey != "" {
		k := SessionKey(wake.SessionKey)
		c.passed = &k
	}
	c.Native, c.NativeConversation = at.nativeOf(ctx, set, r, s.id.id.Client)
	ua := ""
	if r.Header != nil {
		ua = r.Header.Get("User-Agent")
	}
	c.pushWoken = pushClient(cmp.Or(r.Client, s.id.id.Client), ua, r.Meta)
	_, sessionExt := r.Extensions[event.ExtensionKey]
	c.eventsExt = declaresEventsExtension(r.Meta) || sessionExt
	c.pushOn = set.pushOn()
	s.call = c
	return s
}

// requestFor is a tool call's request as Identify sees it.
func requestFor(r adapt.Request) Request {
	return Request{Token: r.Token, Header: r.Header, ClientName: r.Client, Meta: r.Meta}
}

// end closes the call to further requests, once its handler has returned.
func (s *callScope) end() {
	s.mu.Lock()
	s.ended = true
	s.mu.Unlock()
}

// nativeOf says whether a call comes from a client that subscribes to the
// app's events itself, on stateless servers too (which never see
// initialize): the app's override (IsNativeClient), else nativeClientRule,
// for the client name (initialize's, or _meta's, else Identity.Client) and
// User-Agent. conversation is ChatGPT's conversation, when it names one.
func (at *Attachment) nativeOf(ctx context.Context, set *settings, r adapt.Request, identified string) (native bool, conversation string) {
	meta := r.Meta
	conversation, _ = meta["openai/session"].(string)
	if f := set.isNative; f != nil && f(ctx, requestFor(r)) {
		return true, conversation
	}
	var ua string
	client := r.Client
	if client == "" {
		client = identified
	}
	if r.Header != nil {
		ua = r.Header.Get("User-Agent")
	}
	// ChatGPT's desktop app can't subscribe natively (Codex mode waits in
	// the foreground; a regular chat is told it can't be woken), though it
	// sends openai/session too: the rule says no before it reads that.
	if !nativeClientRule(client, ua, meta, r.Extensions, set.servesPushFor(ctx, client, ua, meta)) {
		return false, ""
	}
	return true, conversation
}

func scopeFrom(ctx context.Context) *callScope {
	s, _ := ctx.Value(scopeKey{}).(*callScope)
	return s
}

// ToolCallFrom is the call of a tool that subscribes that ctx belongs to:
// for a tool named in AttachOptions.Wrap or WrapFunc (its handler's ctx),
// or code its handler calls. nil outside one; event.Subscribe with a nil
// call returns CodeNoCall, except on a disabled client.
func ToolCallFrom(ctx context.Context) *ToolCall {
	if s := scopeFrom(ctx); s != nil {
		return s.call
	}
	if c, ok := ctx.Value(disabledCallKey{}).(*ToolCall); ok {
		return c
	}
	return nil
}

// enter starts a request of the call to Wakeshift; leave ends it.
func (c *ToolCall) enter() (*callScope, error) {
	s := c.s
	if s == nil {
		return nil, usage(CodeNoCall, "this ToolCall wasn't made by the SDK: subscribe was called outside a tool call")
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return nil, usage(CodeCallEnded, "the call of %s already returned: subscribe before the tool returns", c.Tool)
	}
	return s, nil
}

func (s *callScope) leave() { s.mu.Unlock() }

// noCall is Subscribe's and Unsubscribe's refusal without a call.
func noCall(name string) error {
	return usage(CodeNoCall, "%s: subscribe was called outside a tool call (a nil call)", name)
}

// wire is the call as Wakeshift takes it: the request's client, _meta and
// User-Agent, the agent's harness and conversation_id arguments, the
// conversation's session, the tool, and who's calling.
func (c *ToolCall) wire() wireCall {
	s := c.s
	call := wireOf(s.req, c.wireSession()).withArgs(s.wake).tasksIf(s.at.set.wait)
	// Only to a core that serves push, which knows the field (an older one
	// refuses fields it doesn't know).
	// Only where push is served for this request (PushOff or a server
	// whose events Wakeshift doesn't serve never gets a native push line).
	call.EventsExt = c.eventsExt && c.pushOn && s.at.app.pushKnown()
	if (c.Native || (call.EventsExt && c.pushWoken)) && s.at.app.vouchersKnown() {
		// A native caller's transport session binds an anonymous voucher;
		// only to a core that hands them out (an older one refuses fields
		// it doesn't know). A push-woken caller's native line can
		// carry a voucher too.
		id := ""
		if s.req.Header != nil {
			id = s.req.Header.Get("Mcp-Session-Id")
		}
		call.TransportSession = transportSession(cmp.Or(id, s.req.Session))
	}
	return s.id.id.apply(call)
}

// ResolveSession finds or starts the calling conversation's session without
// subscribing it to anything (it doesn't make the conversation wakeable),
// and returns it. It returns "", with no request to Wakeshift, when nothing
// names the conversation: no session_key, no conversation_id and no
// conversation in the call's _meta (a Codex thread). Without a user
// (identify's), only the session_key names it: a conversation ID alone
// returns "" too. On a disabled client it returns CodeDisabled.
func (c *ToolCall) ResolveSession(ctx context.Context) (SessionKey, error) {
	if c == nil {
		return "", usage(CodeNoCall, "ResolveSession was called outside a tool call (a nil call)")
	}
	if c.stand != nil {
		c.setSession(c.stand.session)
		return c.Session(), nil
	}
	if c.disabled {
		return "", errDisabled("ResolveSession")
	}
	s, err := c.enter()
	if err != nil {
		return "", err
	}
	defer s.leave()
	if err := s.id.refusal(); err != nil {
		return "", err
	}
	if s.pending != "" && c.Session() != "" {
		return c.Session(), nil
	}
	wire := c.wire()
	named := wire.Conversation != "" || namesConversation(wire.Meta)
	if wire.Session == "" && !(named && (wire.User != "" || wire.AgentToken != "")) {
		// Without a session key, only a user's (or an agent token's) call
		// finds its conversation by the ID its harness reports (a Codex
		// thread): Wakeshift never finds a session by that ID alone, so for
		// anything else it would only start a new one.
		return "", nil
	}
	var out struct {
		Session string     `json:"session"`
		Pending string     `json:"pending"`
		Agent   Recognized `json:"agent"`
	}
	app := s.at.app
	if err := app.inCall(ctx, app.subscribeBudget(), "POST", "/api/apps/v1/calls/session", map[string]any{"call": wire}, &out); err != nil {
		return "", err
	}
	s.pending = out.Pending
	c.setSession(out.Session)
	rec := out.Agent
	c.Recognized = &rec
	return c.Session(), nil
}

// Subscribe subscribes the conversation calling a tool that subscribes
// (call: its handler's, or ToolCallFrom(ctx)) to this event with args, or
// renews its subscription. The tool's result then carries what the
// conversation needs to be woken (one block per call, however many
// subscribes it makes), and if the call fails, the subscription ends.
//
// The event must be one the tool names in Subscribes
// (CodeUndeclaredSubscribe otherwise). Your tool decides who may: Authorize
// runs only for a native caller (an MCP Events client), before it's told
// to subscribe. For a native caller nothing is made here: the result's
// State is SubscriptionNative, or SubscriptionUnavailable with a Reason. A
// personal event needs a user. On a disabled client it checks its
// arguments and returns State SubscriptionDisabled.
func (e *Event[A, P]) Subscribe(ctx context.Context, call *ToolCall, args A, opts ...SubscribeOption) (Subscription, error) {
	sub, err := e.subscribe(ctx, call, args, opts...)
	if err != nil {
		call.noteSubscribeFailed()
	}
	return sub, err
}

// noteSubscribeFailed records that a Subscribe of the call failed: any
// failure makes Wakes false, so the tool keeps its own wait.
func (c *ToolCall) noteSubscribeFailed() {
	switch {
	case c == nil:
	case c.stand != nil:
		c.stand.mu.Lock()
		c.stand.failed = true
		c.stand.mu.Unlock()
	case c.s != nil:
		c.s.wmu.Lock()
		c.s.subscribeFailed = true
		c.s.wmu.Unlock()
	}
}

func (e *Event[A, P]) subscribe(ctx context.Context, call *ToolCall, args A, opts ...SubscribeOption) (Subscription, error) {
	var o subscribeOptions
	for _, f := range opts {
		if f != nil {
			f(&o)
		}
	}
	if o.replaySet && (o.replay <= 0 || o.replay > maxReplay) {
		return Subscription{}, usage(CodeBadArguments, "%s: replay is more than 0 and at most 24 hours (got %s)", e.t.name, o.replay)
	}
	if call == nil {
		if !e.app.Enabled() {
			return Subscription{State: SubscriptionDisabled}, nil
		}
		return Subscription{}, noCall(e.t.name)
	}
	if call.stand != nil {
		argsJSON, err := e.encodeArgs(args)
		if err != nil {
			return Subscription{}, err
		}
		return call.stand.subscribe(ctx, e.app, call, e.t, e.t.knownJSON(argsJSON), o)
	}
	if call.disabled {
		if call.app != e.app {
			return Subscription{}, otherAppsEvent(e.t.name)
		}
		if !call.decl.has(e.t) {
			return Subscription{}, undeclared(call.Tool, e.t.name)
		}
		return Subscription{State: SubscriptionDisabled}, nil
	}
	s, err := call.enter()
	if err != nil {
		return Subscription{}, err
	}
	defer s.leave()
	if s.at.app != e.app {
		return Subscription{}, otherAppsEvent(e.t.name)
	}
	if !s.decl.has(e.t) {
		return Subscription{}, undeclared(call.Tool, e.t.name)
	}
	if err := s.id.refusal(); err != nil {
		return Subscription{}, err
	}
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return Subscription{}, usage(CodeBadArguments, "%s: the arguments don't encode as JSON (%T)", e.t.name, err)
	}
	known := e.t.knownJSON(argsJSON)
	wire := call.wire()
	if call.Native && !call.pushWoken {
		sub, err := e.subscribeNative(ctx, call, s, wire, known, o)
		if err == nil {
			s.noteState(subKey(e.t.name, known), sub.State)
		}
		return sub, err
	}
	if e.t.personal && wire.User == "" {
		return Subscription{}, usage(CodeBadArguments, "%s is personal: Subscribe needs a user (AttachOptions.Identify's), who the subscription belongs to", e.t.name)
	}
	body := map[string]any{"call": wire, "name": e.t.name, "arguments": known, "defer_addition": true}
	if s.pending != "" {
		body["pending"] = s.pending
	}
	if o.ttl > 0 {
		body["ttl"] = o.ttl.Seconds()
	}
	if o.replaySet {
		body["replay"] = o.replay.Seconds()
	}
	if o.once {
		body["once"] = true
	}
	if o.reason != "" {
		body["reason"] = o.reason
	}
	if o.label != "" {
		body["label"] = o.label
	}
	var out struct {
		EventSubscription struct {
			ID      string `json:"id"`
			User    string `json:"user"`
			Created bool   `json:"created"`
		} `json:"event_subscription"`
		Session    string     `json:"session"`
		Agent      Recognized `json:"agent"`
		Pending    string     `json:"pending"`
		NativeLine bool       `json:"native_line"`
		// Wakes: the conversation's agent program has a wake path that
		// runs now. An older core leaves it out: false.
		Wakes bool `json:"wakes"`
		// CallLimit: the program's per-call limit in seconds, if known.
		// Raw, so anything but a number leaves it unknown.
		CallLimit json.RawMessage `json:"call_limit"`
	}
	if err := e.app.inCall(ctx, e.app.subscribeBudget(), "POST", "/api/apps/v1/event-subscriptions", body, &out); err != nil {
		return Subscription{}, err
	}
	if out.Pending != "" {
		s.pending = out.Pending
	}
	if out.NativeLine && call.pushWoken && wire.EventsExt {
		// The core chose native push for this caller: the block
		// carries the native line (a tools-only event's with a voucher),
		// and the bridged subscription stays as the fallback.
		s.addNativeLine(e.t.name, known, o.nativeLine())
	}
	s.armed = true
	call.setSession(out.Session)
	rec := out.Agent
	call.Recognized = &rec
	es := out.EventSubscription
	state := SubscriptionRenewed
	if es.Created {
		state = SubscriptionCreated
		s.created = append(s.created, createdSub{name: e.t.name, arguments: known, session: out.Session, pending: out.Pending,
			ctx: context.WithoutCancel(ctx), value: o.context})
	} else if i := slices.IndexFunc(s.created, func(c createdSub) bool { return subKey(c.name, c.arguments) == subKey(e.t.name, known) }); i >= 0 {
		// Made earlier in this call: OnSubscribe gets this Subscribe's ctx and Context.
		s.created[i].ctx, s.created[i].value = context.WithoutCancel(ctx), o.context
	}
	s.noteState(subKey(e.t.name, known), state)
	s.noteWakes(out.Wakes, callLimitOf(out.CallLimit))
	user := es.User
	if user == "" {
		user = call.User
	}
	return Subscription{State: state, ID: es.ID, User: user, Session: SessionKey(out.Session), Recognized: rec}, nil
}

// subscribeNative is Subscribe for a caller that subscribes itself: a
// native line telling it what to subscribe to, once the event's own checks
// pass for this caller, else unavailable, logged once per tool
// and reason.
func (e *Event[A, P]) subscribeNative(ctx context.Context, call *ToolCall, s *callScope, wire wireCall, args json.RawMessage, o subscribeOptions) (Subscription, error) {
	if !e.t.open() && wire.AgentToken == "" && e.app.vouchersOfferedBy(ctx, time.Now().Add(e.app.subscribeBudget())) {
		// A tools-only event: while the core hands out subscribe vouchers,
		// the line carries one, so the client subscribes with what the tool
		// gave it, signed in or not. Not
		// for an app acting for its agent, which the core gives none.
		s.addNativeLine(e.t.name, args, o.nativeLine())
		return Subscription{State: SubscriptionNative, User: wire.User}, nil
	}
	reason := ""
	switch {
	case !e.t.open():
		reason = UnavailableClientUnsupported
	case wire.User == "":
		reason = UnavailableNoUser
	case !e.app.authorized(ctx, e.t, Subscriber{User: wire.User, Client: wire.Client, Native: true, Request: requestFor(s.req)}, args):
		reason = UnavailableRefused
	}
	if reason != "" {
		e.app.logOnce(ctx, "native_unavailable", call.Tool+"\x00"+reason,
			fmt.Sprintf("%s couldn't subscribe an MCP Events client to %s (%s)", call.Tool, e.t.name, reason),
			slog.String("tool", call.Tool), slog.String("event", e.t.name), slog.String("reason", reason))
		return Subscription{State: SubscriptionUnavailable, Reason: reason}, nil
	}
	s.addNative(e.t.name, args)
	return Subscription{State: SubscriptionNative, User: wire.User}, nil
}

// undeclared is Subscribe's refusal of an event the tool doesn't name in
// Subscribes.
func undeclared(tool, name string) error {
	return usage(CodeUndeclaredSubscribe, "%s subscribed to %s, which isn't in its subscribes (wakeshift.Subscribes)", tool, name)
}

// otherAppsEvent is Subscribe's and Unsubscribe's refusal of an event
// defined on another Client.
func otherAppsEvent(name string) error {
	return usage(CodeUnknownEvent, "%s is another app's event: this app doesn't offer it", name)
}

// Unsubscribe ends the calling conversation's subscription to this event
// with args, reporting whether one ended. For a native caller it ends the
// user's own (webhook) subscriptions to the event with those arguments. On
// a disabled client it ends nothing.
func (e *Event[A, P]) Unsubscribe(ctx context.Context, call *ToolCall, args A) (bool, error) {
	if call == nil {
		if !e.app.Enabled() {
			return false, nil
		}
		return false, noCall(e.t.name)
	}
	if call.stand != nil {
		argsJSON, err := e.encodeArgs(args)
		if err != nil {
			return false, err
		}
		return call.stand.unsubscribe(e.t, e.t.knownJSON(argsJSON))
	}
	if call.disabled {
		if call.app != e.app {
			return false, otherAppsEvent(e.t.name)
		}
		return false, nil
	}
	s, err := call.enter()
	if err != nil {
		return false, err
	}
	defer s.leave()
	if s.at.app != e.app {
		return false, otherAppsEvent(e.t.name)
	}
	if err := s.id.refusal(); err != nil {
		return false, err
	}
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return false, usage(CodeBadArguments, "%s: the arguments don't encode as JSON (%T)", e.t.name, err)
	}
	known := e.t.knownJSON(argsJSON)
	wire := call.wire()
	if call.Native && !call.pushWoken {
		// The result no longer offers the line, whatever the core ends.
		s.dropNativeLine(e.t.name, known)
		s.noteNativeEnded(subKey(e.t.name, known))
		return e.app.endNative(ctx, wire.User, e.t.name, known)
	}
	var out struct {
		Ended bool `json:"ended"`
		// Addition: the call passed no session key, so it ended nothing;
		// the block says to call again with it.
		Addition addition `json:"addition"`
	}
	err = e.app.inCall(ctx, e.app.subscribeBudget(), "POST", "/api/apps/v1/event-subscriptions/unsubscribe",
		map[string]any{"call": wire, "name": e.t.name, "arguments": known}, &out)
	if note := strings.TrimSpace(out.Addition.Text); err == nil && note != "" && !slices.Contains(s.notes, note) {
		s.notes = append(s.notes, note)
	}
	if err == nil && out.Ended {
		s.noteEnded(subKey(e.t.name, known))
	}
	return out.Ended, err
}

// endNative ends user's webhook subscriptions to event with these
// arguments, each request an in-call step.
func (c *Client) endNative(ctx context.Context, user, name string, args json.RawMessage) (bool, error) {
	if user == "" {
		return false, nil
	}
	var subs []EventSubscription
	err := c.inCallBy(ctx, time.Now().Add(c.subscribeBudget()), func(ctx context.Context) (err error) {
		subs, err = c.listEventSubscriptions(ctx, map[string]any{"user": user})
		return err
	})
	if err != nil {
		return false, err
	}
	ended := false
	for _, sub := range subs {
		if sub.Event != name || sub.Destination != "webhook" || !sameJSON(sub.Arguments, args) {
			continue
		}
		var ok bool
		err := c.inCallBy(ctx, time.Now().Add(c.subscribeBudget()), func(ctx context.Context) (err error) {
			ok, err = c.endEventSubscription(ctx, map[string]any{"user": user, "id": sub.ID})
			return err
		})
		if err != nil {
			return ended, err
		}
		ended = ended || ok
	}
	return ended, nil
}

// sameJSON reports whether two JSON values are equal ({} and null alike).
func sameJSON(a, b json.RawMessage) bool {
	var x, y any
	json.Unmarshal(a, &x)
	json.Unmarshal(b, &y)
	if x == nil {
		x = map[string]any{}
	}
	if y == nil {
		y = map[string]any{}
	}
	return reflect.DeepEqual(x, y)
}

// addNative adds a native line, once per event and arguments.
func (s *callScope) addNative(name string, args json.RawMessage) {
	s.addNativeLine(name, args, nativeLine{})
}

// dropNativeLine removes the call's native line for name and args (an
// Unsubscribe in the call), so the result no longer offers it.
func (s *callScope) dropNativeLine(name string, args json.RawMessage) {
	key := subKey(name, args)
	kept := s.native[:0]
	for _, l := range s.native {
		if subKey(l.Name, l.Arguments) != key {
			kept = append(kept, l)
		}
	}
	s.native = kept
}

// addNativeLine is addNative, with line's options.
func (s *callScope) addNativeLine(name string, args json.RawMessage, line nativeLine) {
	args = json.RawMessage(compact(args))
	for i, l := range s.native {
		if l.Name == name && string(l.Arguments) == string(args) {
			if line.hasContext {
				s.native[i].context, s.native[i].hasContext = line.context, true
			}
			return
		}
	}
	if len(s.native) == maxNativeLines {
		return
	}
	line.Name, line.Arguments = name, args
	s.native = append(s.native, line)
}
