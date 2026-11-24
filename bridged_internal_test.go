package wakeshift

import (
	"encoding/json"
	"net/http"
	"testing"
)

// The harnesses Wakeshift wakes through its own paths are never native,
// by clientInfo name or User-Agent product, Capy's and Muse's included,
// whatever they declare (the client table's bridged entries).
func TestBridgedClients(t *testing.T) {
	declares := map[string]any{"io.modelcontextprotocol/clientCapabilities": map[string]any{
		"extensions": map[string]any{"io.modelcontextprotocol/events": map[string]any{}}}}
	for _, c := range []struct{ client, ua string }{
		{"claude-code", ""}, {"", "claude-code/2.1.316 (sdk-cli)"}, {"Cursor", ""},
		{"capy", ""}, {"", "capy"}, {"muse-custom-connector", ""}, {"opencode", ""}, {"", "opencode/1"},
	} {
		if nativeClientRule(c.client, c.ua, declares, nil, false) {
			t.Errorf("%q %q: native", c.client, c.ua)
		}
	}
	for _, c := range []struct{ client, ua string }{{"acme-agent", "acme/1"}, {"muse-mcp", ""}} {
		if !nativeClientRule(c.client, c.ua, declares, nil, false) {
			t.Errorf("%q %q: bridged", c.client, c.ua)
		}
	}
}

// The capabilities ChatGPT's desktop app and a Work chat on the web sent
// on tools/call, as observed.
const (
	chatgptDesktopCaps = `{"experimental":{"openai/visibility":{"enabled":true}},"elicitation":{"form":{},"url":{}},"extensions":{"io.modelcontextprotocol/ui":{"mimeTypes":["text/html;profile=mcp-app","text/html+skybridge"]},"openai/elicitation":{"form":{},"userVerification":{}},"openai/form":{}}}`
	chatgptWebCaps     = `{"experimental":{"openai/visibility":{"enabled":true}},"extensions":{"io.modelcontextprotocol/ui":{"mimeTypes":["text/html;profile=mcp-app"]},"openai/elicitation":{"userVerification":{}}}}`
)

func capsOf(s string) map[string]any {
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		panic(err)
	}
	return m
}

// tools/list: ChatGPT's desktop app keeps subscribe_events (it can't
// subscribe natively); a Work chat on the web, with the same client name and
// User-Agent but its own capabilities, and ChatGPT's other calls don't.
func TestNativeClientChatGPTDesktop(t *testing.T) {
	params := func(caps map[string]any, client string) json.RawMessage {
		m := map[string]any{"io.modelcontextprotocol/clientInfo": map[string]any{"name": client, "version": "1.0.0"}}
		if caps != nil {
			m["io.modelcontextprotocol/clientCapabilities"] = caps
		}
		b, _ := json.Marshal(map[string]any{"_meta": m})
		return b
	}
	r, _ := http.NewRequest("POST", "/", nil)
	r.Header.Set("User-Agent", "openai-mcp/1.0.0 (Codex)")
	if nativeClient(r, params(capsOf(chatgptDesktopCaps), "openai-mcp (Codex)"), false) {
		t.Error("the desktop app is native")
	}
	if !nativeClient(r, params(capsOf(chatgptWebCaps), "openai-mcp (Codex)"), false) {
		t.Error("a Work chat on the web isn't native")
	}
	if !nativeClient(r, params(nil, "openai-mcp (Codex)"), false) {
		t.Error("no capabilities to go by isn't ChatGPT")
	}
	if !nativeClient(r, params(capsOf(chatgptDesktopCaps), "openai-mcp"), false) {
		t.Error("ChatGPT's own client name isn't native")
	}
}
