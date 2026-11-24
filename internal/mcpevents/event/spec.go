package event

import (
	"encoding/base64"
	"strings"
	"unicode/utf8"
)

// Every SEP-3415 constant Wakeshift uses, in one place: its error codes
// are provisional.
//
// Revision is the text these follow: SEP-3415 as merged, Final, on
// 2026-11-19 (modelcontextprotocol/modelcontextprotocol#3415, 8a18b4b),
// plus HeaderMismatch from the 2026-07-28 schema (SEP-2243). Its one wire
// change from the accepted text (f77a5fe, 2026-11-11) is the
// Mcp-Param-{Name} headers for subscription arguments marked x-mcp-header;
// its webhook and poll wire is the same as the 2026-10-27 revision's
// (b08e4f9).
//
// Where today's clients and the spec differ, Wakeshift follows each
// client: a webhook's dialect, from the client table
// (mcp-events-clients.json, beside this file), keeps webhook-id equal to
// the body's eventId only for the clients that refuse a delivery where
// they differ. The SDKs accept a missing Mcp-Name header from every
// client and refuse one that disagrees with params.name.
const Revision = "SEP-3415 (2026-11-19, 8a18b4b)"

// Delivery is the delivery modes Wakeshift offers for every event type
// (events/list's delivery): a webhook, or polls (events/poll). Push
// (events/stream) isn't offered.
var Delivery = []string{"webhook", "poll"}

// Capability keys: the draft's extension, and the top-level key the
// earlier sketch had, which ChatGPT still reads. A server advertises both;
// a client is read only for the extension (ChatGPT, which declares
// neither, is known by its name).
const (
	ExtensionKey = "io.modelcontextprotocol/events"
	TopLevelKey  = "events"
)

// ResultType is on every events/* result.
const ResultType = "complete"

// TextMeta is the _meta key on an occurrence with a rendered line for
// clients that show events to a person or a model as text; the payload
// (data) is the event type's own.
const TextMeta = "ai.wakeshift/text"

// NameHeader is the Streamable HTTP routing header events/subscribe,
// events/unsubscribe, events/poll and events/stream carry: params.name
// (SEP-2243's Mcp-Name).
const NameHeader = "Mcp-Name"

// Dialects: what a client speaks, named in the client table
// (mcp-events-clients.json, EvaluateClient), which also says what each
// one's webhooks are sent. A webhook's dialect is fixed when it is made
// (WebhookDialect):
//
//	dialect   clients                          webhook-id            gap, terminated  errors
//	spec      everyone else (SEP-3415)         its own, per message  sent             spec
//	draft     TanStack AI, the Vercel AI SDK   the body's eventId    sent             spec
//	openclaw  OpenClaw                         the body's eventId    not sent (400)   spec
//	chatgpt   ChatGPT                          the body's eventId    not sent         ChatGPT's
//
// Every webhook made before webhook dialects were per client (2026-11) is
// draft or chatgpt, and keeps what it was made with.
const (
	DialectSpec     = "spec"
	DialectDraft    = "draft"
	DialectOpenClaw = "openclaw"
	DialectChatGPT  = "chatgpt"
)

// Codes are the JSON-RPC error codes of one dialect.
type Codes struct {
	NotFound, Forbidden, ResourceExhausted, Unsupported, CallbackEndpoint int
}

// InvalidParams and HeaderMismatch are the same in every dialect.
const (
	InvalidParams  = -32602
	HeaderMismatch = -32020
)

// CodesFor is a dialect's error codes (its errors in the client table):
// the spec's (provisional) or ChatGPT's. ChatGPT has no ResourceExhausted
// or Unsupported code: those are InvalidParams there.
func CodesFor(dialect string) Codes {
	if profile(dialect).Errors == "chatgpt" {
		return Codes{NotFound: -32011, Forbidden: -32012, ResourceExhausted: InvalidParams, Unsupported: InvalidParams, CallbackEndpoint: -32015}
	}
	return Codes{NotFound: -32023, Forbidden: -32024, ResourceExhausted: -32025, Unsupported: -32026, CallbackEndpoint: -32027}
}

// HeaderValue decodes an Mcp-Name header: plain ASCII, or SEP-2243's
// Base64 sentinel (=?base64?…?=) for anything that isn't, holding
// canonical, padded standard Base64 of UTF-8 (the 2026-07-28 transport
// spec's examples; the official SDKs refuse anything else). ok is false
// for a sentinel that doesn't decode so: unpadded, non-canonical, not
// Base64 or not UTF-8. The same rule in every SDK
// (sdk/typescript/test/mcp-events-interop.json).
func HeaderValue(h string) (string, bool) {
	inner, found := strings.CutPrefix(h, "=?base64?")
	if !found || !strings.HasSuffix(inner, "?=") {
		return h, true
	}
	b, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSuffix(inner, "?="))
	if err != nil || !utf8.Valid(b) {
		return "", false
	}
	return string(b), true
}
