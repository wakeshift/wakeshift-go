package appkit

// Event types (https://wakeshift.ai/docs/events): what an app offers to be
// subscribed to, by name and arguments, the way MCP Events lists them. An
// app defines each one once, at startup, with DefineEvent, and sends an
// occurrence with Emit; Attach (events.go) lets every client
// subscribe: natively (MCP Events: ChatGPT) or through the tools it adds
// (every other harness), and Wakeshift delivers either way.
//
//	type msgArgs struct {
//		Channel string `json:"channel" jsonschema:"The channel to watch"`
//	}
//	type msg struct {
//		From string `json:"from"`
//		Text string `json:"text"`
//	}
//	message, err := appkit.DefineEvent(app, "chat.message", appkit.EventDef[msgArgs, msg]{
//		Description: "A new message in a channel",
//		Text:        func(m msg, a msgArgs) string { return m.From + " in #" + a.Channel + ": " + m.Text },
//		Authorize:   func(ctx context.Context, s appkit.Subscriber, a msgArgs) bool { return canRead(s.User, a.Channel) },
//	})
//	…
//	message.Emit(ctx, msgArgs{Channel: "eng"}, msg{From: "ada", Text: "shipped it"})
//
// It's the TypeScript SDK's API (wake.defineEvent, event.emit,
// wake.attach), with Go types where that has zod schemas; the two
// SDKs' events/list and tools are checked against the same golden files
// (sdk/typescript/test/events-list.json, event-tools.json).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
)

// EventDef describes an event type: A is a subscription's arguments, P an
// occurrence's payload. Their JSON Schemas come from the Go types (field
// names from json tags, descriptions from jsonschema tags), as the MCP SDK
// does for tools; ArgsSchema and PayloadSchema replace them.
type EventDef[A, P any] struct {
	// Description says what it is, for the agent and the person: "A new
	// message in a channel". Required.
	Description string
	// Title is a short title, for clients that show one.
	Title string
	// Text is the line an agent reads first for an occurrence (framed by
	// Wakeshift as information from outside): from + ": " + text. Required.
	Text func(payload P, args A) string
	// Authorize says whether s may subscribe with these arguments (nil:
	// everyone). It runs before any subscription, native or bridged.
	Authorize func(ctx context.Context, s Subscriber, args A) bool
	// Personal: each subscription belongs to its subscriber (Identity.User),
	// and Emit sends an occurrence to one of them (To), so it may
	// carry that subscriber's own private view. Nothing in the arguments
	// names the subscriber: Wakeshift takes it from the subscriber's own
	// sign-in.
	Personal bool
	// DeleteOnDelivery: Wakeshift deletes an occurrence's content (its
	// text and payload) once it's delivered to the agent, keeping only what
	// it needs to show that it happened (IDs, times, where it went). For
	// messages people wouldn't expect a third party to keep, such as chat
	// messages. One that isn't delivered expires as any other does. An
	// agent can't have it again after delivery: a replay skips it.
	DeleteOnDelivery bool
	// Group makes the event a group's posts:
	// it names the app's group (its type, app.<app>.…) that a subscription
	// with these arguments is for. A client that subscribes natively (MCP
	// Events) then gets the posts its agent's membership of that group
	// would wake it for, by its webhook, in place of its conversation:
	// joining stays the app's (Agent.SubscribeAs), and Wakeshift refuses
	// the subscription unless the agent is a member. The payload is
	// Wakeshift's (P is GroupMessage), there's no Emit or PublishURL (posts
	// come from the group), Text isn't needed, and subscribe_events refuses
	// it: a bridged conversation's membership already wakes it. An error
	// refuses the subscription, as Authorize does.
	Group func(ctx context.Context, s Subscriber, args A) (string, error)

	// ArgsSchema and PayloadSchema, if set, are the JSON Schemas used
	// instead of A's and P's.
	ArgsSchema, PayloadSchema json.RawMessage
	// Topic and Filter route occurrences (Wakeshift's topics; see the
	// core's apps.EventType), if the defaults don't fit. Default: a topic
	// segment per required string, integer or boolean argument
	// ("ev/chat.message/{channel}": each value is a topic segment, lowercase
	// a-z 0-9 . _ -), and an equality filter on the payload field of each
	// optional argument with the same name.
	Topic  string
	Filter map[string]string
}

// withArguments is a group's event's payload schema: GroupMessage's, with
// the arguments' properties (and which are required) added, as Wakeshift
// sends them; GroupMessage's own win a clash.
func withArguments(payload, args json.RawMessage) (json.RawMessage, error) {
	type schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	var p, a map[string]json.RawMessage
	var ps, as schema
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(payload, &ps); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(args, &as); err != nil {
		return nil, err
	}
	if ps.Properties == nil {
		ps.Properties = map[string]json.RawMessage{}
	}
	for k, v := range as.Properties {
		if _, clash := ps.Properties[k]; !clash {
			ps.Properties[k] = v
			if slices.Contains(as.Required, k) {
				ps.Required = append(ps.Required, k)
			}
		}
	}
	var err error
	if p["properties"], err = json.Marshal(ps.Properties); err != nil {
		return nil, err
	}
	if p["required"], err = json.Marshal(ps.Required); err != nil {
		return nil, err
	}
	return json.Marshal(p)
}

// GroupMessage is a group's post, as a group's event (EventDef.Group)
// carries it to one member, after the subscription's arguments.
type GroupMessage struct {
	ID        string         `json:"id" jsonschema:"The message's ID: reply to it with the app's tools"`
	From      string         `json:"from,omitempty" jsonschema:"Who posted it: their handle in the group; empty for the app itself"`
	Subject   string         `json:"subject" jsonschema:"A one-line summary"`
	Text      string         `json:"text,omitempty" jsonschema:"The message in full"`
	Mentioned bool           `json:"mentioned" jsonschema:"Whether it @-mentions you"`
	ReplyTo   string         `json:"reply_to,omitempty" jsonschema:"The message it replies to"`
	Data      map[string]any `json:"data,omitempty" jsonschema:"The app's own data about the message"`
}

// Subscriber is who's subscribing: what Authorize decides on.
type Subscriber struct {
	// User is the app's ID for the person (Identity.User), if it has one.
	User string
	// Client is the MCP client's name, if known.
	Client string
	// Native: subscribing natively (MCP Events), not through subscribe_events.
	Native bool
}

// DefinedEvent is one of the app's event types (DefineEvent): Emit sends
// an occurrence.
type DefinedEvent[A, P any] struct {
	app *App
	t   *eventType
	def EventDef[A, P]
}

// eventType is an event type without its Go types: what the manifest,
// events/list and the tools need.
type eventType struct {
	name, title, description string
	args, payload            json.RawMessage
	topic                    string
	filter                   map[string]string
	personal                 bool
	deleteOnDelivery         bool
	argNames                 []string // in name order
	authorize                func(ctx context.Context, s Subscriber, args json.RawMessage) bool
	// group is EventDef.Group's: nil for any other event.
	group func(ctx context.Context, s Subscriber, args json.RawMessage) (string, error)
}

var (
	eventNameRE = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*){0,4}$`)
	argNameRE   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// DefineEvent defines one of the app's event types: name is what clients
// subscribe to (chat.message, game.turn: lowercase dot-separated words, at
// most 64 characters, not wakeshift.*). Define every event before
// PublishManifest, which publishes them, and before Attach.
func DefineEvent[A, P any](app *App, name string, def EventDef[A, P]) (*DefinedEvent[A, P], error) {
	switch {
	case !eventNameRE.MatchString(name) || len(name) > 64:
		return nil, fmt.Errorf("appkit: event name %q: lowercase dot-separated words (chat.message), at most 64 characters", name)
	case strings.HasPrefix(name, "wakeshift."):
		return nil, errors.New("appkit: event names starting with wakeshift. are Wakeshift's")
	case strings.TrimSpace(def.Description) == "":
		return nil, fmt.Errorf("appkit: event %s needs a Description", name)
	case def.Text == nil && def.Group == nil:
		return nil, fmt.Errorf("appkit: event %s needs Text", name)
	case def.Group != nil && reflect.TypeFor[P]() != reflect.TypeFor[GroupMessage]():
		return nil, fmt.Errorf("appkit: event %s is a group's posts: its payload is GroupMessage", name)
	case def.Group != nil && (def.Personal || def.DeleteOnDelivery || len(def.Filter) > 0):
		return nil, fmt.Errorf("appkit: event %s is a group's posts: it can't be Personal, DeleteOnDelivery or have a Filter (the membership's wake filters it)", name)
	}
	args, err := schemaFor[A](def.ArgsSchema)
	if err != nil {
		return nil, fmt.Errorf("appkit: event %s: arguments: %w", name, err)
	}
	payload, err := schemaFor[P](def.PayloadSchema)
	if err != nil {
		return nil, fmt.Errorf("appkit: event %s: payload: %w", name, err)
	}
	if def.Group != nil {
		// Each post comes with the subscription's arguments too, so it
		// says which group it's from as the app names it ({"game": …}).
		if payload, err = withArguments(payload, args); err != nil {
			return nil, fmt.Errorf("appkit: event %s: payload: %w", name, err)
		}
	}
	t := &eventType{name: name, title: def.Title, description: def.Description, args: args, payload: payload,
		topic: def.Topic, filter: def.Filter, personal: def.Personal, deleteOnDelivery: def.DeleteOnDelivery}
	var props schemaProps
	json.Unmarshal(args, &props)
	for n := range props.Properties {
		t.argNames = append(t.argNames, n)
	}
	slices.Sort(t.argNames)
	if t.topic == "" {
		if t.topic, t.filter, err = defaultRoute(name, args, payload); err != nil {
			return nil, fmt.Errorf("appkit: event %s: %w", name, err)
		}
	} else if err := checkRoute(t.topic, t.filter, args, payload); err != nil {
		return nil, fmt.Errorf("appkit: event %s: %w", name, err)
	}
	if def.Group != nil {
		t.group = func(ctx context.Context, s Subscriber, raw json.RawMessage) (group string, err error) {
			var a A
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &a); err != nil {
					return "", err
				}
			}
			defer func() {
				if r := recover(); r != nil {
					slog.Warn("appkit: Group panicked, so the subscription was refused", "event", name, "panic", fmt.Sprint(r))
					group, err = "", errors.New("appkit: Group panicked")
				}
			}()
			return def.Group(ctx, s, a)
		}
	}
	if def.Authorize != nil {
		t.authorize = func(ctx context.Context, s Subscriber, raw json.RawMessage) (ok bool) {
			var a A
			if len(raw) > 0 && json.Unmarshal(raw, &a) != nil {
				return false
			}
			// An Authorize that panics refuses, the same way on both paths;
			// the app hears why.
			defer func() {
				if r := recover(); r != nil {
					slog.Warn("appkit: Authorize panicked, so the subscription was refused", "event", name, "panic", fmt.Sprint(r))
					ok = false
				}
			}()
			return def.Authorize(ctx, s, a)
		}
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if slices.ContainsFunc(app.eventTypes, func(e *eventType) bool { return e.name == name }) {
		return nil, fmt.Errorf("appkit: event %s is already defined", name)
	}
	app.eventTypes = append(app.eventTypes, t)
	return &DefinedEvent[A, P]{app: app, t: t, def: def}, nil
}

// Name is the event type's name.
func (e *DefinedEvent[A, P]) Name() string { return e.t.name }

// EmitOption shapes an Emit.
type EmitOption func(*emitOptions)

type emitOptions struct{ id, to string }

// To sends a personal event's occurrence to one user (the app's ID for
// them): only their subscriptions get it. Required for a personal event,
// refused for any other.
func To(user string) EmitOption { return func(o *emitOptions) { o.to = user } }

// WithID makes an Emit idempotent: retrying with the same ID delivers once.
func WithID(id string) EmitOption { return func(o *emitOptions) { o.id = id } }

// Emitted is what an Emit reached.
type Emitted struct {
	// Matched is how many subscriptions it went to (0: nobody's subscribed).
	Matched   int  `json:"matched"`
	Duplicate bool `json:"duplicate"` // an earlier Emit had this ID
}

// CallerSubscription is a subscription made for the conversation calling an
// app's tool (Subscribe).
type CallerSubscription struct {
	ID      string `json:"id"`
	Created bool   `json:"created"` // false: it had one already, now renewed
	// Session is the conversation's session key.
	Session string `json:"session"`
	// Addition is what the tool's result needs so the conversation is
	// woken (Apply it to the result), as subscribe_events' result has.
	Addition Addition `json:"addition"`
}

// Subscribe subscribes the conversation calling your tool (call: built from
// the tool's request, Args.Call) to this event with args, the way
// subscribe_events would, so a tool like a game's join sets up its own
// wakes in one step. It's for clients Wakeshift wakes: a client that speaks
// MCP Events (SpeaksEvents) subscribes itself. Your tool decides who may:
// Authorize isn't consulted here. A personal event needs call.User, who the
// subscription belongs to. It's the TypeScript SDK's event.subscribe.
func (e *DefinedEvent[A, P]) Subscribe(ctx context.Context, call Call, args A) (CallerSubscription, error) {
	if e.t.group != nil {
		return CallerSubscription{}, fmt.Errorf("appkit: %s is a group's posts: a conversation's membership of the group wakes it", e.t.name)
	}
	if e.t.personal && strings.TrimSpace(call.User) == "" {
		return CallerSubscription{}, fmt.Errorf("appkit: %s is personal: Subscribe needs call.User, who the subscription belongs to", e.t.name)
	}
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return CallerSubscription{}, fmt.Errorf("appkit: %s: encoding the arguments: %w", e.t.name, err)
	}
	var out struct {
		EventSubscription struct {
			ID      string `json:"id"`
			Created bool   `json:"created"`
		} `json:"event_subscription"`
		Session  string   `json:"session"`
		Addition Addition `json:"addition"`
	}
	if err := e.app.appCall(ctx, "POST", "/api/apps/v1/event-subscriptions",
		map[string]any{"call": call, "name": e.t.name, "arguments": e.t.knownJSON(argsJSON)}, &out); err != nil {
		return CallerSubscription{}, err
	}
	return CallerSubscription{ID: out.EventSubscription.ID, Created: out.EventSubscription.Created, Session: out.Session, Addition: out.Addition}, nil
}

// Unsubscribe ends the calling conversation's subscription to this event
// with args (call: as for Subscribe), reporting whether it had one.
func (e *DefinedEvent[A, P]) Unsubscribe(ctx context.Context, call Call, args A) (bool, error) {
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return false, fmt.Errorf("appkit: %s: encoding the arguments: %w", e.t.name, err)
	}
	var out struct {
		Ended bool `json:"ended"`
	}
	err = e.app.appCall(ctx, "POST", "/api/apps/v1/event-subscriptions/unsubscribe",
		map[string]any{"call": call, "name": e.t.name, "arguments": e.t.knownJSON(argsJSON)}, &out)
	return out.Ended, err
}

// End ends one of user's subscriptions to the app's events by its ID (from
// Subscribe or EventSubscriptions), wherever it delivers, reporting whether
// one ended. It's App.EndEventSubscription.
func (e *DefinedEvent[A, P]) End(ctx context.Context, user, id string) (bool, error) {
	return e.app.EndEventSubscription(ctx, user, id)
}

// Emit sends one occurrence: to everyone subscribed with arguments that
// match args (a personal event: to one user's subscriptions, To). The
// payload is checked against the event's schema.
func (e *DefinedEvent[A, P]) Emit(ctx context.Context, args A, payload P, opts ...EmitOption) (Emitted, error) {
	var o emitOptions
	for _, f := range opts {
		f(&o)
	}
	switch {
	case e.t.group != nil:
		return Emitted{}, fmt.Errorf("appkit: %s is a group's posts: post to the group", e.t.name)
	case e.t.personal && strings.TrimSpace(o.to) == "":
		return Emitted{}, fmt.Errorf("appkit: %s is personal: Emit needs To(user)", e.t.name)
	case !e.t.personal && o.to != "":
		return Emitted{}, fmt.Errorf("appkit: %s isn't personal: To is only for personal events", e.t.name)
	}
	text := e.def.Text(payload, args)
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return Emitted{}, fmt.Errorf("appkit: %s: encoding the arguments: %w", e.t.name, err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return Emitted{}, fmt.Errorf("appkit: %s: encoding the payload: %w", e.t.name, err)
	}
	body := map[string]any{"name": e.t.name, "arguments": json.RawMessage(argsJSON), "payload": json.RawMessage(payloadJSON), "text": text}
	if o.id != "" {
		body["id"] = o.id
	}
	if o.to != "" {
		body["to"] = o.to
	}
	var out Emitted
	err = e.app.appCall(ctx, "POST", "/api/apps/v1/events/emit", body, &out)
	return out, err
}

// PublishURL makes a publish URL for this event with args (a personal
// event: To, the one user it's for), for services that can only send
// webhooks. A POST's body, a JSON object, is an occurrence's payload,
// checked against the event's schema; the agent reads the event's name and
// the start of the payload, since Text doesn't run. Anyone with the URL
// can emit until App.RevokePublishURL.
func (e *DefinedEvent[A, P]) PublishURL(ctx context.Context, args A, opts ...EmitOption) (PublishURL, error) {
	var o emitOptions
	for _, f := range opts {
		f(&o)
	}
	switch {
	case e.t.group != nil:
		return PublishURL{}, fmt.Errorf("appkit: %s is a group's posts: post to the group", e.t.name)
	case e.t.personal && strings.TrimSpace(o.to) == "":
		return PublishURL{}, fmt.Errorf("appkit: %s is personal: PublishURL needs To(user)", e.t.name)
	case !e.t.personal && o.to != "":
		return PublishURL{}, fmt.Errorf("appkit: %s isn't personal: To is only for personal events", e.t.name)
	}
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return PublishURL{}, fmt.Errorf("appkit: %s: encoding the arguments: %w", e.t.name, err)
	}
	body := map[string]any{"name": e.t.name, "arguments": json.RawMessage(argsJSON)}
	if o.to != "" {
		body["to"] = o.to
	}
	var out PublishURL
	err = e.app.appCall(ctx, "POST", "/api/apps/v1/events/publish-urls", body, &out)
	return out, err
}

// definition is the event type as events/list describes it.
func (t *eventType) definition() map[string]any {
	d := map[string]any{"name": t.name, "description": t.description, "delivery": []string{"webhook"},
		"inputSchema": json.RawMessage(t.args), "payloadSchema": json.RawMessage(t.payload)}
	if t.title != "" {
		d["title"] = t.title
	}
	return d
}

// manifestEntry is the event type as the manifest carries it.
func (t *eventType) manifestEntry() ManifestEvent {
	return ManifestEvent{Name: t.name, Description: t.description, Arguments: t.args, Payload: t.payload, Topic: t.topic,
		Filter: t.filter, Personal: t.personal, DeleteOnDelivery: t.deleteOnDelivery, Group: t.group != nil}
}

// ManifestEvent is an event type in the manifest (see the core's
// apps.EventType). DefineEvent makes these; PublishManifest adds them.
type ManifestEvent struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Arguments   json.RawMessage   `json:"arguments,omitempty"`
	Payload     json.RawMessage   `json:"payload,omitempty"`
	Topic       string            `json:"topic"`
	Filter      map[string]string `json:"filter,omitempty"`
	Personal    bool              `json:"personal,omitempty"`
	// DeleteOnDelivery: EventDef.DeleteOnDelivery.
	DeleteOnDelivery bool `json:"delete_on_delivery,omitempty"`
	// Group: EventDef.Group is set.
	Group bool `json:"group,omitempty"`
}

// events is the app's event types, in the order they were defined.
func (a *App) events() []*eventType {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.eventTypes)
}

func (a *App) eventByName(name string) *eventType {
	for _, t := range a.events() {
		if t.name == name {
			return t
		}
	}
	return nil
}

// schemaFor is a JSON Schema: given, or T's, normalized (see normalize).
func schemaFor[T any](given json.RawMessage) (json.RawMessage, error) {
	var v any
	if len(given) > 0 {
		if err := json.Unmarshal(given, &v); err != nil {
			return nil, fmt.Errorf("not JSON: %w", err)
		}
	} else {
		s, err := jsonschema.For[T](nil)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &v); err != nil {
			return nil, err
		}
		open(v)
	}
	obj, ok := v.(map[string]any)
	if !ok || obj["type"] != "object" {
		return nil, errors.New(`the schema is an object's ("type": "object")`)
	}
	normalize(obj)
	return json.Marshal(obj) // map keys sort: one canonical form
}

// normalize puts a schema in the form both SDKs produce for the same
// definition (sdk/typescript/test/events-list.json): no $schema or $id,
// required sorted, and an optional property's ["null", T] type just T (Go
// makes an omitempty slice nullable; a missing property is how both say
// "absent").
func normalize(s map[string]any) {
	delete(s, "$schema")
	delete(s, "$id")
	req := map[string]bool{}
	if r, ok := s["required"].([]any); ok {
		names := make([]string, 0, len(r))
		for _, n := range r {
			if str, ok := n.(string); ok {
				names = append(names, str)
				req[str] = true
			}
		}
		slices.Sort(names)
		out := make([]any, len(names))
		for i, n := range names {
			out[i] = n
		}
		s["required"] = out
		if len(out) == 0 {
			delete(s, "required")
		}
	}
	if props, ok := s["properties"].(map[string]any); ok {
		for name, p := range props {
			ps, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if types, ok := ps["type"].([]any); ok && !req[name] {
				var rest []any
				for _, t := range types {
					if t != "null" {
						rest = append(rest, t)
					}
				}
				if len(rest) == 1 {
					ps["type"] = rest[0]
				} else {
					ps["type"] = rest
				}
			}
			normalize(ps)
		}
	}
	if items, ok := s["items"].(map[string]any); ok {
		normalize(items)
	}
}

// open drops the "additionalProperties": false jsonschema.For gives every
// struct, as zod does for an object: a property the type doesn't have is
// dropped (known), not refused. A schema the app gives is left as it is.
func open(v any) {
	switch s := v.(type) {
	case map[string]any:
		if s["additionalProperties"] == false {
			delete(s, "additionalProperties")
		}
		for _, sub := range s {
			open(sub)
		}
	case []any:
		for _, sub := range s {
			open(sub)
		}
	}
}

// known is args with only the event type's own arguments: zod's object
// parse in the TypeScript SDK drops the rest, so both send the core the
// same canonical arguments. Anything but an object is left for the core
// to refuse.
func (t *eventType) known(args map[string]any) map[string]any {
	if args == nil {
		return nil
	}
	out := make(map[string]any, len(args))
	for k, v := range args {
		if slices.Contains(t.argNames, k) {
			out[k] = v
		}
	}
	return out
}

// knownJSON is known for raw JSON arguments.
func (t *eventType) knownJSON(raw json.RawMessage) json.RawMessage {
	var obj map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&obj) != nil || obj == nil {
		return raw
	}
	b, err := json.Marshal(t.known(obj))
	if err != nil {
		return raw
	}
	return b
}

// schemaProps is a schema's properties (with their types) and required
// names.
type schemaProps struct {
	Properties map[string]struct {
		Type any `json:"type"`
	} `json:"properties"`
	Required []string `json:"required"`
}

var placeholderRE = regexp.MustCompile(`\{([^{}]*)\}`)

// checkRoute checks a Topic and Filter the app set, as Wakeshift will when
// the manifest is published, so a mistake shows at DefineEvent: a topic's
// placeholders are required scalar arguments, and a filter maps payload
// fields to arguments.
func checkRoute(topic string, filter map[string]string, args, payload json.RawMessage) error {
	var a, p schemaProps
	json.Unmarshal(args, &a)
	json.Unmarshal(payload, &p)
	for _, m := range placeholderRE.FindAllStringSubmatch(topic, -1) {
		n := m[1]
		if !argNameRE.MatchString(n) {
			return fmt.Errorf("topic %q: {%s}: argument names in a topic are lowercase a-z 0-9 _", topic, n)
		}
		prop, ok := a.Properties[n]
		if !ok || !slices.Contains(a.Required, n) {
			return fmt.Errorf("topic %q: {%s} must be a required argument", topic, n)
		}
		switch prop.Type {
		case "string", "integer", "boolean":
		default:
			return fmt.Errorf("topic %q: {%s} names a topic, so it must be a string, integer or boolean", topic, n)
		}
	}
	for field, tmpl := range filter {
		if _, ok := p.Properties[field]; !ok {
			return fmt.Errorf("filter: %q isn't a field of the payload", field)
		}
		m := placeholderRE.FindStringSubmatch(tmpl)
		if m == nil || m[0] != tmpl {
			return fmt.Errorf("filter: %q maps to %q: a filter value is one {argument}", field, tmpl)
		}
		if _, ok := a.Properties[m[1]]; !ok {
			return fmt.Errorf("filter: %q maps to {%s}, which isn't an argument", field, m[1])
		}
	}
	return nil
}

// defaultRoute is an event type's topic and filter when it names none: a
// topic segment per required scalar argument, in name order, and a filter
// on the payload's same-named field for each optional one.
func defaultRoute(name string, args, payload json.RawMessage) (string, map[string]string, error) {
	var a, p struct {
		Properties map[string]struct {
			Type any `json:"type"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	json.Unmarshal(args, &a)
	json.Unmarshal(payload, &p)
	topic := "ev/" + name
	var filter map[string]string
	names := make([]string, 0, len(a.Properties))
	for n := range a.Properties {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		if !argNameRE.MatchString(n) {
			return "", nil, fmt.Errorf("argument %q: argument names are lowercase a-z 0-9 _", n)
		}
		if slices.Contains(a.Required, n) {
			switch a.Properties[n].Type {
			case "string", "integer", "boolean":
			default:
				return "", nil, fmt.Errorf("argument %q is required, so it names a topic: it must be a string, integer or boolean (or set Topic)", n)
			}
			topic += "/{" + n + "}"
			continue
		}
		switch a.Properties[n].Type {
		case "string", "integer", "number", "boolean":
		default:
			return "", nil, fmt.Errorf("optional argument %q filters on the payload, so it must be a string, number or boolean (or set Topic and Filter)", n)
		}
		if _, ok := p.Properties[n]; !ok {
			return "", nil, fmt.Errorf("optional argument %q has no payload field of that name to filter on (set Topic and Filter)", n)
		}
		if filter == nil {
			filter = map[string]string{}
		}
		filter[n] = "{" + n + "}"
	}
	return topic, filter, nil
}
