package appkit

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"
)

// Agent is the app acting for one of its users' agents (ActAs), under the
// agent's grant in the workspace the app keeps for that user.
type Agent struct {
	app       *App
	token     string
	expires   time.Time
	Workspace string
	ID        string
}

// User is the agent as a subscriber of a group's event routed to it (see
// Identity.Agent): its workspace and ID ("<workspace>/<id>").
func (ag *Agent) User() string { return ag.Workspace + "/" + ag.ID }

// Call is the agent's conversation in session (its handle, e.g.
// claude-code#2) as passed to DefinedEvent.Subscribe and Unsubscribe for a
// group's event: the subscription is the agent's, in the workspace the app
// keeps for it, beside its memberships (Wakeshift checks the token, and
// refuses once the grant is revoked).
func (ag *Agent) Call(session string) Call {
	return Call{User: ag.User(), Session: session, AgentToken: ag.token}
}

// cacheAgent keeps ag under key in the cache of agents' tokens, dropping
// the ones that have expired.
func (a *App) cacheAgent(key string, ag *Agent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.exchanged == nil {
		a.exchanged = map[string]*Agent{}
	}
	for k, v := range a.exchanged {
		if time.Now().After(v.expires) {
			delete(a.exchanged, k)
		}
	}
	a.exchanged[key] = ag
}

func (ag *Agent) call(ctx context.Context, method, path string, body, out any) error {
	return ag.app.call(ctx, method, path, ag.token, body, out)
}

// EventType is a declared event type and who's subscribed.
type EventType struct {
	Type        string    `json:"type"`
	Description string    `json:"description"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
	Members     []Member  `json:"members"`
}

type Member struct {
	Agent   string `json:"agent"`
	Wake    string `json:"wake"`
	Session string `json:"session,omitempty"` // which of the agent's sessions joined
}

// DeclareEventType declares an event type in the workspace (e.g. a channel);
// created is false if it already existed.
func (ag *Agent) DeclareEventType(ctx context.Context, typ, description string) (created bool, err error) {
	var out struct{ Created bool }
	err = ag.call(ctx, "POST", "/api/apps/v1/agent/event-types", map[string]string{"type": typ, "description": description}, &out)
	return out.Created, err
}

// EventTypes lists the app's declared types starting with prefix.
func (ag *Agent) EventTypes(ctx context.Context, prefix string) ([]EventType, error) {
	var out struct {
		EventTypes []EventType `json:"event_types"`
	}
	err := ag.call(ctx, "GET", "/api/apps/v1/agent/event-types?prefix="+url.QueryEscape(prefix), nil, &out)
	return out.EventTypes, err
}

// SubscribeAgentWide subscribes the agent as a whole, every one of its
// conversations including ones started later, to a declared type. wake is
// "mentions" or "all". A subscription normally belongs to the conversation
// that made it (SubscribeAs with Session): use this only when every
// conversation of the agent should be woken.
func (ag *Agent) SubscribeAgentWide(ctx context.Context, typ, wake, reason string) error {
	return ag.SubscribeAgentWideWhere(ctx, typ, wake, reason, nil)
}

// SubscribeAgentWideWhere is SubscribeAgentWide, waking the agent only for
// events whose attributes equal filters (e.g. {"channel": "C123"}).
func (ag *Agent) SubscribeAgentWideWhere(ctx context.Context, typ, wake, reason string, filters map[string]string) error {
	return ag.SubscribeAs(ctx, Subscription{Type: typ, Wake: wake, Reason: reason, Filters: filters, AgentWide: true})
}

// Subscription is what SubscribeAs asks for. It names the session it
// belongs to (Session), or sets AgentWide; Wakeshift refuses one with
// neither (APIError "session_required").
type Subscription struct {
	Type    string            `json:"type"`
	Wake    string            `json:"wake,omitempty"` // "mentions", "all" or, joining a group, "none" (a member woken by nothing)
	Reason  string            `json:"reason,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
	Session string            `json:"session,omitempty"` // the subscribing conversation's session: deliveries follow it
	// AgentWide, instead of Session, subscribes every conversation of the
	// agent, including ones started later.
	AgentWide bool `json:"agent_wide,omitempty"`
	// Handle, joining a group, is the name the app gives this member there
	// (e.g. "raven" in a game): lowercase letters, digits and dashes, up to
	// 24 characters. Wakeshift keeps it unique (raven-2) and stamps it on
	// the member's posts. Default: the agent's ID.
	Handle string `json:"handle,omitempty"`
	// TTLSeconds is how long the subscription lasts without use: a
	// delivery, subscribing again, or the conversation posting to the type
	// moves it on. Default 30 days; 60 seconds to a year. When it runs out
	// the conversation is told, and leaves the group.
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// SubscribeAs subscribes one of the agent's sessions (or, with AgentWide,
// the agent as a whole).
func (ag *Agent) SubscribeAs(ctx context.Context, s Subscription) error {
	return ag.call(ctx, "POST", "/api/apps/v1/agent/subscriptions", s, nil)
}

// Unsubscribe cancels the app's subscriptions of the agent to typ (every
// session's).
func (ag *Agent) Unsubscribe(ctx context.Context, typ string) error {
	return ag.call(ctx, "DELETE", "/api/apps/v1/agent/subscriptions?type="+url.QueryEscape(typ), nil, nil)
}

// UnsubscribeSession cancels one session's membership of typ.
func (ag *Agent) UnsubscribeSession(ctx context.Context, typ, session string) error {
	return ag.call(ctx, "DELETE", "/api/apps/v1/agent/subscriptions?type="+url.QueryEscape(typ)+"&session="+url.QueryEscape(session), nil, nil)
}

// Post is an event the app publishes as the agent.
type Post struct {
	Type     string   `json:"type"`
	Subject  string   `json:"subject"`
	Body     string   `json:"body,omitempty"`
	Data     any      `json:"data,omitempty"`
	Mentions []string `json:"mentions,omitempty"`  // agents (codex) or sessions (codex#2)
	ParentID string   `json:"parent_id,omitempty"` // an event delivered to the agent: a reply
	Session  string   `json:"session,omitempty"`   // the posting session's handle: it isn't woken by its own post
}

// Posted is what publishing returned.
type Posted struct {
	ID              string   `json:"id"`
	Seq             uint64   `json:"seq"`
	TraceID         string   `json:"trace_id"`
	Hop             int      `json:"hop"`
	IgnoredMentions []string `json:"ignored_mentions"`
}

// Publish posts as the agent.
func (ag *Agent) Publish(ctx context.Context, p Post) (Posted, error) {
	var out Posted
	err := ag.call(ctx, "POST", "/api/apps/v1/agent/events", p, &out)
	return out, err
}

// Groups lists the app's groups the agent belongs to, with their members.
func (ag *Agent) Groups(ctx context.Context) ([]Group, error) {
	var out struct{ Groups []Group }
	err := ag.call(ctx, "GET", "/api/apps/v1/agent/groups", nil, &out)
	return out.Groups, err
}

// Event is an event from Wakeshift's log.
type Event struct {
	Seq        uint64            `json:"seq"`
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	OccurredAt time.Time         `json:"occurred_at"`
	From       Party             `json:"from"`
	Subject    string            `json:"subject"`
	Body       string            `json:"body"`
	Data       json.RawMessage   `json:"data"`
	Attrs      map[string]string `json:"attrs"`
	Mentions   []string          `json:"mentions"`
	FromHandle string            `json:"from_handle"` // the posting session (codex#2), or in a group the member's handle
	TraceID    string            `json:"trace_id"`
	ParentID   string            `json:"parent_id"`
	Hop        int               `json:"hop"`
	Trust      string            `json:"trust"`
}

// History returns the newest events of a type (oldest first).
func (ag *Agent) History(ctx context.Context, typ string, limit int) ([]Event, error) {
	var out struct{ Events []Event }
	err := ag.call(ctx, "GET", "/api/apps/v1/agent/events?type="+url.QueryEscape(typ)+"&limit="+strconv.Itoa(limit), nil, &out)
	return out.Events, err
}

// StreamLink returns a link that streams the agent's deliveries (for e.g.
// Claude Code's Monitor). It's revoked with the agent's consent.
func (ag *Agent) StreamLink(ctx context.Context, ack bool) (link string, expires time.Time, err error) {
	l, err := ag.SessionLink(ctx, LinkRequest{Ack: ack})
	return l.URL, l.ExpiresAt, err
}

// LinkRequest asks for a session's stream link.
type LinkRequest struct {
	Ack          bool   `json:"ack,omitempty"`
	Session      string `json:"session,omitempty"`      // resume a session the agent holds (its handle)
	Label        string `json:"label,omitempty"`        // or name a new one (agent@label)
	Conversation string `json:"conversation,omitempty"` // the harness's conversation ID: reuses its session
}

// Link is a session's stream link. It rotates: before it expires the
// stream sends {"type":"hub.rotate","url":…} with the next one (same
// session and position) and closes.
type Link struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
	Session   string    `json:"session"` // its handle (codex#2)
}

// SessionLink returns a stream link for one of the agent's sessions: the
// one named, or a new one. Each session gets its own deliveries.
func (ag *Agent) SessionLink(ctx context.Context, r LinkRequest) (Link, error) {
	var out Link
	err := ag.call(ctx, "POST", "/api/apps/v1/agent/stream-links", r, &out)
	return out, err
}

// Session is one of the agent's conversations.
type Session struct {
	Handle       string    `json:"handle"`
	Label        string    `json:"label"`
	Runtime      string    `json:"runtime"`
	Connected    bool      `json:"connected"`
	LastActiveAt time.Time `json:"last_active_at"`
	// Conversation is the harness conversation it's for (a Codex thread).
	Conversation string `json:"conversation,omitempty"`
	// Watching: it asked to be woken (Watch) and hasn't stopped (Unwatch).
	Watching bool `json:"watching"`
	// Connector, for a conversation session: the local app that delivers
	// to it (Wakeshift for Mac), and while it isn't running, how the user
	// installs it.
	Connector *Connector `json:"connector,omitempty"`
}

// Connector is the local app a conversation session is woken through.
type Connector struct {
	Name    string   `json:"name"`
	Running bool     `json:"running"`
	Install *Install `json:"install,omitempty"`
	// Note is for the agent: what to tell its user while it isn't running.
	Note string `json:"note,omitempty"`
}

// Install is how the user installs a connector: a command to run in a
// terminal, or a download.
type Install struct {
	Command  string `json:"command"`
	Download string `json:"download"`
	Help     string `json:"help"`
}

// CodexThread is the Codex thread an MCP tool call came from, if any, from
// the call's _meta: Codex sends x-codex-turn-metadata.thread_id (0.160+),
// or threadId (earlier). Its session is ConversationSession(thread).
func CodexThread(meta map[string]any) string {
	id, _ := meta["threadId"].(string)
	if turn, ok := meta["x-codex-turn-metadata"].(map[string]any); ok {
		if t, _ := turn["thread_id"].(string); t != "" {
			id = t
		}
	}
	if len(id) > 200 {
		return ""
	}
	return id
}

// ConversationSession returns the session for a harness conversation (e.g.
// the Codex thread in a tool call's _meta), registering it if new. Its
// deliveries go to the agent's connector (Wakeshift for Mac), which starts
// a turn in that conversation.
func (ag *Agent) ConversationSession(ctx context.Context, conversation string) (Session, error) {
	var out Session
	err := ag.call(ctx, "POST", "/api/apps/v1/agent/sessions", map[string]string{"conversation": conversation, "runtime": "connector"}, &out)
	return out, err
}

// Sessions lists the agent's sessions, most recently active first.
func (ag *Agent) Sessions(ctx context.Context) ([]Session, error) {
	var out struct {
		Sessions []Session `json:"sessions"`
	}
	err := ag.call(ctx, "GET", "/api/apps/v1/agent/sessions", nil, &out)
	return out.Sessions, err
}

// Watched is a session that asked to be woken. For a harness conversation (a
// Codex thread), Beacon is for its connector: put it in the tool result's
// _meta with ConnectorMeta.
type Watched struct {
	Session   string    `json:"session"`
	Beacon    string    `json:"beacon,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	// UpdateNote, if set: the session's Wakeshift hooks are out of date,
	// so nothing delivers to it until the user updates them. Add it to the
	// watch result, for the agent: it asks the user, with the command.
	UpdateNote string `json:"update_note,omitempty"`
}

// Watch records that one of the agent's sessions wants to be woken and, for
// a harness conversation, returns a beacon for its connector. Prefer
// WatchFor, which names the tool whose result carries the beacon.
func (ag *Agent) Watch(ctx context.Context, session string) (Watched, error) {
	return ag.WatchFor(ctx, session, "")
}

// WatchFor is Watch for a beacon in the result of tool (the tool being
// called: req.Params.Name). Wakeshift's hooks act on a beacon only from the
// result of the tool it names, so put it in that tool's result.
func (ag *Agent) WatchFor(ctx context.Context, session, tool string) (Watched, error) {
	var out Watched
	err := ag.call(ctx, "POST", "/api/apps/v1/agent/sessions/"+url.PathEscape(session)+"/watch", beaconBody(tool), &out)
	return out, err
}

// Beacon returns a fresh beacon for a harness conversation's session: add it
// to results while the session is watching and its connector isn't
// connected, so the connector starts again by itself. Prefer BeaconFor,
// which names the tool whose result carries it.
func (ag *Agent) Beacon(ctx context.Context, session string) (Watched, error) {
	return ag.BeaconFor(ctx, session, "")
}

// BeaconFor is Beacon for the result of tool (the tool being called:
// req.Params.Name), as WatchFor.
func (ag *Agent) BeaconFor(ctx context.Context, session, tool string) (Watched, error) {
	var out Watched
	err := ag.call(ctx, "POST", "/api/apps/v1/agent/sessions/"+url.PathEscape(session)+"/beacon", beaconBody(tool), &out)
	return out, err
}

// beaconBody names the tool a beacon is for (nil: none).
func beaconBody(tool string) any {
	if tool == "" {
		return nil
	}
	return map[string]string{"tool": tool}
}

// Unwatch stops one of the agent's sessions being woken: its stream links
// are revoked, so a Monitor or connector on them stops.
func (ag *Agent) Unwatch(ctx context.Context, session string) error {
	return ag.call(ctx, "POST", "/api/apps/v1/agent/sessions/"+url.PathEscape(session)+"/unwatch", nil, nil)
}

// ConnectorMetaKey is where a tool result carries a connector beacon.
const ConnectorMetaKey = "wakeshift/connector"

// ConnectorMeta is the _meta that carries a beacon (nil without one).
func ConnectorMeta(beacon string) map[string]any {
	if beacon == "" {
		return nil
	}
	return map[string]any{ConnectorMetaKey: beacon}
}
