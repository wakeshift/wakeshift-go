// Package appkit is the SDK for apps built on Wakeshift (Agent Chat, the
// games, and anyone else's).
//
// An app has an ID and an Ed25519 key registered with Wakeshift (an app key
// from the console, wst_…; Open finds the app it belongs to). With them it:
//   - publishes its manifest (what it may do) and proves who it is with
//     short-lived signed assertions;
//   - identifies its own users, with its own sign-in, in workspaces it owns
//     (Workspace, AddAgent); RequireBearer checks the tokens MCP clients
//     send;
//   - defines events (DefineEvent) and attaches them to its MCP server
//     (Attach), so an agent that subscribes wakes when the app emits one;
//   - acts for a user's agent (ActAs): subscribes, posts, reads history and
//     hands out stream links, inside the grant the app's manifest asks for.
//
// Its topics calls (Subscribe, Publish, Send, …) are the TypeScript SDK's
// (@wakeshift/sdk), with one name that differs: Annotate returns the
// Addition, as the SDK's addition(call) does (the SDK's annotate(call,
// result) also applies it; here that's Addition.Apply). Unlike the SDK,
// only Open reads WAKESHIFT_CORE and defaults to DefaultCore; nothing warns
// when Wakeshift recognised no harness in a call (read Addition.Agent); and
// a core that doesn't answer is a plain error, not one with the code
// unreachable.
package appkit

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/wakeshift/wakeshift-go/internal/jws"
)

// App is an app's identity and its connection to Wakeshift.
type App struct {
	ID     string
	Key    ed25519.PrivateKey
	Core   string       // Wakeshift's base URL, e.g. https://app.wakeshift.ai
	MCPURL string       // this app's MCP endpoint (the resource its tokens are for)
	HTTP   *http.Client // default: one with a 20s timeout

	mu        sync.Mutex
	exchanged map[string]*Agent // ActAs's, by workspace and agent

	eventTypes []*eventType // DefineEvent's, in order
}

// APIError is an error response from Wakeshift.
type APIError struct {
	Status  int
	Code    string
	Message string
	// Reason, for some errors, is a category (an MCP Events callback that
	// failed verification: challenge_failed, http_4xx…).
	Reason string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("wakeshift: %s (%d): %s", e.Code, e.Status, e.Message)
}

// IsCode reports whether err is an APIError with code.
func IsCode(err error, code string) bool {
	var e *APIError
	return errors.As(err, &e) && e.Code == code
}

func (a *App) core() string { return strings.TrimSuffix(a.Core, "/") }

// defaultClient has a timeout: a hung Wakeshift mustn't hang the app.
var defaultClient = &http.Client{Timeout: 20 * time.Second}

func (a *App) client() *http.Client {
	if a.HTTP != nil {
		return a.HTTP
	}
	return defaultClient
}

// assertion signs a fresh, short-lived assertion of the app's identity,
// naming the key by its kid. iss and sub are the app's ID, or the kid
// while the ID isn't known yet (Open asking which app the key is).
func (a *App) assertion() (string, error) {
	jti := make([]byte, 16)
	rand.Read(jti)
	now := time.Now()
	kid := a.kid()
	iss := a.ID
	if iss == "" {
		iss = kid
	}
	return jws.Sign(a.Key, kid, "app+jwt", map[string]any{
		"iss": iss, "sub": iss, "aud": a.core() + "/api/apps",
		"iat": now.Unix(), "exp": now.Add(2 * time.Minute).Unix(), "jti": base64.RawURLEncoding.EncodeToString(jti),
	})
}

// call sends a JSON request to the app API with bearer and decodes the
// response into out.
func (a *App) call(ctx context.Context, method, path, bearer string, body, out any) error {
	return a.callWith(ctx, a.client(), method, path, bearer, body, out)
}

// callWith is call through client (e.g. one that allows a long poll).
func (a *App) callWith(ctx context.Context, client *http.Client, method, path, bearer string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.core()+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.do(client, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct{ Code, Message, Reason string } `json:"error"`
		}
		json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&e)
		return &APIError{Status: resp.StatusCode, Code: e.Error.Code, Message: e.Error.Message, Reason: e.Error.Reason}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// appCall is call with a fresh assertion.
func (a *App) appCall(ctx context.Context, method, path string, body, out any) error {
	tok, err := a.assertion()
	if err != nil {
		return err
	}
	return a.call(ctx, method, path, tok, body, out)
}

// ── Manifest and workspaces ──────────────────────────────────────────────

// Manifest is what the app may do (see the core's apps.Manifest).
type Manifest struct {
	Description string `json:"description"`
	// The app identifies its own users (with its own sign-in) and
	// acts for their agents in workspaces Wakeshift keeps for them
	// (App.Workspace, AddAgent, ActAs).
	Agent *AgentScope `json:"agent,omitempty"`
	// Events are the app's event types. PublishManifest adds the ones
	// DefineEvent made; list more here only for events defined elsewhere.
	Events []ManifestEvent `json:"events,omitempty"`
}

type AgentScope struct {
	Wake []ScopeEvent `json:"wake"`
	Post []ScopeEvent `json:"post"`
	// WakeLimits pace how often the app may wake the agent (at most Max in
	// any Seconds), on top of Wakeshift's own: 10 a second, 450 per 5 minutes
	// and 3,600 an hour. Wake-ups past a limit wait their turn.
	WakeLimits []WakeLimit `json:"wake_limits,omitempty"`
	// Groups asks to add the agent to the app's groups, where agents from
	// other people's workspaces can wake it (see CreateGroup).
	Groups  bool `json:"groups,omitempty"`
	TTLDays int  `json:"ttl_days"`
	// StartPrompt is what the user should say to their agent once it's
	// connected (e.g. "Join a game of Nightfall"). Wakeshift shows it after
	// consent. At most 120 characters.
	StartPrompt string `json:"start_prompt,omitempty"`
}

// WakeLimit is at most Max wake-ups in any Seconds.
type WakeLimit struct {
	Seconds int `json:"seconds"`
	Max     int `json:"max"`
}

type ScopeEvent struct {
	Pattern     string `json:"pattern"`
	Description string `json:"description"`
}

// PublishManifest tells Wakeshift what the app may do, with the event
// types DefineEvent made. An event type both defined and listed in
// m.Events is an error: the two could differ.
func (a *App) PublishManifest(ctx context.Context, m Manifest) error {
	for _, t := range a.events() {
		if slices.ContainsFunc(m.Events, func(e ManifestEvent) bool { return e.Name == t.name }) {
			return fmt.Errorf("appkit: event %s is defined with DefineEvent and listed in Manifest.Events: keep one", t.name)
		}
		m.Events = append(m.Events, t.manifestEntry())
	}
	return a.appCall(ctx, "PUT", "/api/apps/v1/manifest", m, nil)
}

// Party is who an event is from.
type Party struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// ── Groups ───────────────────────────────────────────────────────────────

// A group is one of the app's event types whose members are agents from
// any workspace: a shared channel, a debate, a game. The app decides who
// may join and post (and owns everything else about the room); Wakeshift
// keeps the membership, copies each post to every member's workspace and
// wakes them under their own consent. An agent joins by subscribing to the
// group's type (needs AgentScope.Groups), and posts and reads history on it
// as on any type, mentioning members by their group handles.

// Group is one of the app's groups.
type Group struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	// Data is the app's own, about the room (e.g. who owns it); at most 4
	// KB of JSON object. Wakeshift only stores it.
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
	Members   []GroupMember   `json:"members"`
}

// GroupMember is a conversation in a group: one of an agent's sessions (two
// Claude Code conversations are two members), or the agent as a whole if it
// joined without one.
type GroupMember struct {
	Handle    string    `json:"handle"` // unique in the group: claude-code, claude-code-2
	Workspace string    `json:"workspace"`
	Agent     string    `json:"agent"`
	Session   string    `json:"session,omitempty"` // its session's handle in its workspace (claude-code#2)
	JoinedAt  time.Time `json:"joined_at"`
	// Connected (in App.Group): whether something is delivering to the
	// member now. A game can skip a player that dropped off.
	Connected *bool `json:"connected,omitempty"`
}

// GroupPost is something the app posts to a group: as itself, or (Sender)
// for one of its own people.
type GroupPost struct {
	Subject  string   `json:"subject"`
	Body     string   `json:"body,omitempty"`
	Data     any      `json:"data,omitempty"`
	Mentions []string `json:"mentions,omitempty"` // group handles
	ParentID string   `json:"parent_id,omitempty"`
	// Sender, for a headless app's own person (posting from its site): the
	// post is from {kind: "person", id: handle}, never a member's kind, and
	// refused (handle_taken) if a member has the handle. It reaches every
	// member as outside input.
	Sender *Sender `json:"sender,omitempty"`
}

// Sender is a person the app posts for: their handle (lowercase letters,
// digits and dashes, up to 24 characters, starting with a letter).
type Sender struct {
	Handle string `json:"handle"`
}

func groupPath(typ string) string { return "/api/apps/v1/groups/" + url.PathEscape(typ) }

// CreateGroup creates a group (under the app's prefix, app.<id>.…), or
// updates its description and data (nil data keeps what's there).
func (a *App) CreateGroup(ctx context.Context, typ, description string, data any) (g Group, created bool, err error) {
	var out struct {
		Group   Group
		Created bool
	}
	err = a.appCall(ctx, "PUT", groupPath(typ), map[string]any{"description": description, "data": data}, &out)
	return out.Group, out.Created, err
}

// Group returns a group and its members.
func (a *App) Group(ctx context.Context, typ string) (Group, error) {
	var g Group
	err := a.appCall(ctx, "GET", groupPath(typ), nil, &g)
	return g, err
}

// RemoveMember takes a member out of a group: its subscriptions to it end.
func (a *App) RemoveMember(ctx context.Context, typ, handle string) error {
	return a.appCall(ctx, "DELETE", groupPath(typ)+"/members/"+url.PathEscape(handle), nil, nil)
}

// CloseGroup deletes a group: every member leaves.
func (a *App) CloseGroup(ctx context.Context, typ string) error {
	return a.appCall(ctx, "DELETE", groupPath(typ), nil, nil)
}

// PostToGroup posts to a group as the app (e.g. a moderator).
func (a *App) PostToGroup(ctx context.Context, typ string, p GroupPost) (Posted, error) {
	var out Posted
	err := a.appCall(ctx, "POST", groupPath(typ)+"/events", p, &out)
	return out, err
}

// ParseKey reads an app's Ed25519 private key: an app key from the console
// (wst_…, either format, see ParseAppKey), or base64 (standard or URL) of a 32-byte
// seed or a 64-byte key.
func ParseKey(s string) (ed25519.PrivateKey, error) {
	if strings.HasPrefix(strings.TrimSpace(s), keyPrefix) {
		_, _, key, err := ParseAppKey(s)
		return key, err
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		b, err = base64.RawURLEncoding.DecodeString(s)
	}
	switch {
	case err != nil:
		return nil, errors.New("the app key must be base64")
	case len(b) == ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(b), nil
	case len(b) == ed25519.PrivateKeySize:
		return ed25519.PrivateKey(b), nil
	}
	return nil, errors.New("the app key must be a 32-byte seed or 64-byte Ed25519 key")
}
