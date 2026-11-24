package event

import (
	_ "embed"
	"encoding/json"
	"slices"
	"strings"

	"github.com/wakeshift/wakeshift-go/internal/harnesses"
)

// The MCP Events client table: which clients are exceptions to capability
// negotiation, and which wire dialect each needs. mcp-events-clients.json is
// a byte-for-byte copy of sdk/typescript/test/mcp-events-clients.json, the
// one source of truth, which the TypeScript and Python SDKs ship as
// generated constants (`go run ./internal/mcpevents/clientsgen/gen` writes
// all three; a test fails when one drifts). Its _about defines the match
// semantics; this file and each SDK implement them identically, pinned by
// sdk/typescript/test/mcp-events-interop.json native_client. Nothing here is
// public SDK API.
//
//go:embed mcp-events-clients.json
var clientsJSON []byte

// ClientsJSON is the embedded table, for the drift tests.
func ClientsJSON() []byte { return clientsJSON }

// clientTable is the table's shape.
type clientTable struct {
	Schema   int                       `json:"schema"`
	Dialects map[string]dialectProfile `json:"dialects"`
	Entries  []clientEntry             `json:"entries"`
}

// dialectProfile is a dialect's wire behavior.
type dialectProfile struct {
	Errors    string `json:"errors"`     // "spec" or "chatgpt": CodesFor
	WebhookID string `json:"webhook_id"` // "message" or "event_id"
	Envelopes bool   `json:"envelopes"`  // gap and terminated are sent
}

// clientEntry is one exception.
type clientEntry struct {
	ID          string              `json:"id"`
	Match       map[string][]string `json:"match"`
	WithoutPush bool                `json:"without_push"`
	Native      string              `json:"native"` // never, undeclared, capabilities
	Dialect     string              `json:"dialect"`
	// WebhookOnly: the client takes events by webhook alone, never poll or
	// push (only the core reads it: the SDKs' copies leave it out).
	WebhookOnly bool   `json:"webhook_only"`
	Reason      string `json:"reason"`
	Source      string `json:"source"`
}

// Native effects.
const (
	nativeNever        = "never"
	nativeUndeclared   = "undeclared"
	nativeCapabilities = "capabilities"
)

// Match fields.
const (
	matchClient       = "client"
	matchClientPrefix = "client_prefix"
	matchUAProduct    = "ua_product"
	matchMetaKey      = "meta_key"
	matchDetector     = "detector"
)

// detectors are the built-in checks an entry can name: one function in
// each codebase.
var detectors = map[string]func(client string, meta map[string]any) bool{
	"chatgpt_desktop": harnesses.ChatGPTNotNative,
}

var clients = func() clientTable {
	var t clientTable
	if err := json.Unmarshal(clientsJSON, &t); err != nil {
		panic("event: mcp-events-clients.json: " + err.Error())
	}
	return t
}()

// ClientInput is what a request tells about its client.
type ClientInput struct {
	Client    string         // the clientInfo name (the request's _meta, else the session's)
	UserAgent string         // the HTTP User-Agent
	Meta      map[string]any // the request's _meta
	// SessionExtensions are the extensions its session's initialize
	// declared (nil: no session).
	SessionExtensions map[string]any
	// ServesPush: the server serves push (events/stream). False until push
	// ships.
	ServesPush bool
}

// ClientVerdict is the table's answer for one request.
type ClientVerdict struct {
	Native  bool
	Dialect string
	Entry   string // the matched entry's id, or "" when none did (capabilities decided)
	// WebhookOnly: the matched entry's client takes events by webhook
	// alone, so nothing suggests it poll or push.
	WebhookOnly bool
}

// EvaluateClient is the table's verdict: the first entry, in file order,
// whose match holds (and that applies: a without_push entry doesn't when
// the server serves push) decides native-ness and the dialect; with none,
// capabilities decide, in the spec's dialect. The app's own
// IsNativeClient answers before this.
func EvaluateClient(in ClientInput) ClientVerdict {
	client := harnesses.ClientName(in.Client)
	products := uaProducts(in.UserAgent)
	for _, e := range clients.Entries {
		if e.WithoutPush && in.ServesPush {
			continue
		}
		if !e.matches(client, in.Client, products, in.Meta) {
			continue
		}
		v := ClientVerdict{Dialect: e.Dialect, Entry: e.ID, WebhookOnly: e.WebhookOnly}
		switch e.Native {
		case nativeNever:
		case nativeUndeclared:
			v.Native = true
		default:
			v.Native = declares(in)
		}
		return v
	}
	return ClientVerdict{Native: declares(in), Dialect: DialectSpec}
}

// declares reports whether a request declares the extension: in its
// _meta's clientCapabilities (every 2026-07-28 request) or its session's
// initialize.
func declares(in ClientInput) bool {
	if _, ok := in.SessionExtensions[ExtensionKey]; ok {
		return true
	}
	caps, _ := in.Meta["io.modelcontextprotocol/clientCapabilities"].(map[string]any)
	ext, _ := caps["extensions"].(map[string]any)
	_, ok := ext[ExtensionKey]
	return ok
}

// matches reports whether every field of the entry's match holds (any of
// its values). client is the trimmed, lowercased name; raw as sent (for a
// detector).
func (e clientEntry) matches(client, raw string, products []string, meta map[string]any) bool {
	for field, values := range e.Match {
		ok := false
		for _, v := range values {
			switch field {
			case matchClient:
				ok = client == v
			case matchClientPrefix:
				ok = strings.HasPrefix(client, v)
			case matchUAProduct:
				ok = slices.Contains(products, v)
			case matchMetaKey:
				s, _ := meta[v].(string)
				ok = s != ""
			case matchDetector:
				f := detectors[v]
				ok = f != nil && f(raw, meta)
			}
			if ok {
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// uaProducts are a User-Agent's products: each token's text before its
// first "/", ASCII-lowercased, the tokens split on spaces and tabs only
// (harnesses.ClientName says why ASCII only).
func uaProducts(ua string) []string {
	var out []string
	for _, tok := range strings.FieldsFunc(harnesses.ASCIILower(ua), func(r rune) bool { return r == ' ' || r == '\t' }) {
		if p, _, _ := strings.Cut(tok, "/"); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// DialectOf is a client's dialect by the table, from its clientInfo name,
// User-Agent and the request's _meta (nil if there's none).
func DialectOf(clientName, userAgent string, meta map[string]any) string {
	return EvaluateClient(ClientInput{Client: clientName, UserAgent: userAgent, Meta: meta}).Dialect
}

// WebhookDialect is the dialect a new webhook subscription is made with,
// fixed with it: the one the app's SDK names (sdkDialect) when it's a
// table dialect other than spec or draft (an SDK may know a client by
// _meta, or have a newer table), else the table's for the client, by its
// clientInfo name, User-Agent and _meta. An older SDK says draft for every
// client but ChatGPT, so draft and spec from an SDK are decided again here.
func WebhookDialect(sdkDialect, client, userAgent string, meta map[string]any) string {
	if _, known := clients.Dialects[sdkDialect]; known && sdkDialect != DialectSpec && sdkDialect != DialectDraft {
		return sdkDialect
	}
	return DialectOf(client, userAgent, meta)
}

// profile is a dialect's wire behavior; an unknown dialect is draft's,
// what every webhook was before dialects were per client.
func profile(dialect string) dialectProfile {
	if p, ok := clients.Dialects[dialect]; ok {
		return p
	}
	return clients.Dialects[DialectDraft]
}

// WebhookIDIsEventID reports whether a webhook dialect's webhook-id is the
// body's eventId. The spec's is its own, made for each message and kept
// for its retries (it MUST NOT be derived from eventId).
func WebhookIDIsEventID(dialect string) bool { return profile(dialect).WebhookID == "event_id" }

// TakesEnvelopes reports whether a webhook dialect's callback is sent gap
// and terminated envelopes.
func TakesEnvelopes(dialect string) bool { return profile(dialect).Envelopes }
