package event

import "testing"

// ChatGPT's MCP client is openai-mcp, by its client name or its
// User-Agent; Work chats send events/* and tools/call as "openai-mcp/1.0.0
// (Codex)" (the voucher probe, November 5, 2026), events/list as
// "openai-mcp/1.0.0". A client no table entry names speaks the spec.
func TestDialectOf(t *testing.T) {
	for _, c := range []struct{ client, ua, want string }{
		{"openai-mcp", "", DialectChatGPT},
		{"openai-mcp (Codex)", "", DialectChatGPT},
		{"", "openai-mcp/1.0.0", DialectChatGPT},
		{"", "openai-mcp/1.0.0 (Codex)", DialectChatGPT},
		{"", "codex-mcp-client/0.166.0", DialectSpec},
		{"claude-code", "claude-code/2.1.316 (cli)", DialectSpec},
	} {
		if got := DialectOf(c.client, c.ua, nil); got != c.want {
			t.Errorf("%q %q: %s, want %s", c.client, c.ua, got, c.want)
		}
	}
	if got := DialectOf("acme", "", map[string]any{"openai/session": "c1"}); got != DialectChatGPT {
		t.Errorf("openai/session: %s", got)
	}
}

// A webhook's dialect (WebhookDialect): the one the SDK names unless it's
// spec or draft (an older SDK says draft for everyone but ChatGPT), else
// the client table's by clientInfo name, User-Agent product and _meta.
func TestWebhookDialect(t *testing.T) {
	for _, c := range []struct{ sdk, client, ua, want string }{
		{DialectChatGPT, "", "", DialectChatGPT},
		{DialectOpenClaw, "acme", "", DialectOpenClaw},
		{"nonsense", "acme", "", DialectSpec},
		{DialectSpec, "openclaw", "", DialectOpenClaw},
		{DialectDraft, "openai-mcp", "", DialectChatGPT},
		{DialectDraft, "", "openai-mcp/1.0.0 (Codex)", DialectChatGPT},
		{DialectDraft, "openclaw", "node", DialectOpenClaw},
		{DialectDraft, "OpenClaw", "", DialectOpenClaw},
		{DialectDraft, "ai-sdk-mcp-client", "", DialectDraft},
		{DialectDraft, "my-app", "ai-sdk/2.0.97 runtime/node.js/22", DialectDraft},
		{DialectDraft, "my-app", "Mozilla/5.0 ai-sdk/2.0.97", DialectDraft},
		{DialectDraft, "tanstack-ai-mcp", "node", DialectDraft},
		{DialectDraft, "mastra", "node", DialectSpec},
		{DialectDraft, "hermes-agent", "", DialectSpec},
		{DialectDraft, "acme-agent", "acme-agent/2", DialectSpec},
		{DialectDraft, "", "", DialectSpec},
		{"", "openclaw-bundle-mcp", "", DialectSpec},         // OpenClaw's tools client, not its events one
		{DialectDraft, "my-app", "my-ai-sdk/1", DialectSpec}, // a product, not a substring
	} {
		if got := WebhookDialect(c.sdk, c.client, c.ua, nil); got != c.want {
			t.Errorf("%q %q %q: %s, want %s", c.sdk, c.client, c.ua, got, c.want)
		}
	}
	for _, c := range []struct {
		dialect            string
		eventID, envelopes bool
		chatgptCodes       bool
	}{
		{DialectSpec, false, true, false},
		{DialectDraft, true, true, false},
		{DialectOpenClaw, true, false, false},
		{DialectChatGPT, true, false, true},
		{"", true, true, false}, // unknown: as draft, what every webhook was before dialects
	} {
		if WebhookIDIsEventID(c.dialect) != c.eventID || TakesEnvelopes(c.dialect) != c.envelopes || (CodesFor(c.dialect).NotFound == -32011) != c.chatgptCodes {
			t.Errorf("%q: webhook-id is eventId %v, envelopes %v, codes %+v", c.dialect, WebhookIDIsEventID(c.dialect), TakesEnvelopes(c.dialect), CodesFor(c.dialect))
		}
	}
}
