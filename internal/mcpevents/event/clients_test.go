package event

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// sharedFixture reads a file the SDKs share: beside the TypeScript SDK's
// tests here, or testdata/golden in the published Go SDK.
func sharedFixture(t *testing.T, name string) []byte {
	t.Helper()
	for _, dir := range []string{"../../../sdk/typescript/test", "../../../testdata/golden"} {
		if b, err := os.ReadFile(filepath.Join(dir, name)); err == nil {
			return b
		}
	}
	t.Skipf("no shared %s here", name)
	return nil
}

// The embedded table is a byte-for-byte copy of the source of truth beside
// the SDK fixtures (`go run ./internal/mcpevents/clientsgen/gen` copies it).
func TestClientTableIsTheSharedOne(t *testing.T) {
	if !bytes.Equal(ClientsJSON(), sharedFixture(t, "mcp-events-clients.json")) {
		t.Fatal("internal/mcpevents/event/mcp-events-clients.json differs from sdk/typescript/test/mcp-events-clients.json: run go run ./internal/mcpevents/clientsgen/gen")
	}
}

// Every native_client case of the shared fixture: native, dialect and the
// matched entry, as every SDK checks them.
func TestClientTableCases(t *testing.T) {
	var f struct {
		NativeClient struct {
			Cases []struct {
				Name              string         `json:"name"`
				Client            string         `json:"client"`
				UserAgent         string         `json:"user_agent"`
				Meta              map[string]any `json:"meta"`
				SessionExtensions map[string]any `json:"session_extensions"`
				ServesPush        bool           `json:"serves_push"`
				Native            bool           `json:"native"`
				Dialect           string         `json:"dialect"`
				Entry             *string        `json:"entry"`
			} `json:"cases"`
		} `json:"native_client"`
	}
	if err := json.Unmarshal(sharedFixture(t, "mcp-events-interop.json"), &f); err != nil {
		t.Fatal(err)
	}
	if len(f.NativeClient.Cases) == 0 {
		t.Fatal("no cases")
	}
	matched := map[string]bool{}
	for _, c := range f.NativeClient.Cases {
		if c.Entry == nil || c.Dialect == "" {
			t.Errorf("%s: no expected entry or dialect", c.Name)
			continue
		}
		v := EvaluateClient(ClientInput{Client: c.Client, UserAgent: c.UserAgent, Meta: c.Meta, SessionExtensions: c.SessionExtensions, ServesPush: c.ServesPush})
		if v.Native != c.Native || v.Dialect != c.Dialect || v.Entry != *c.Entry {
			t.Errorf("%s: %+v, want native %v dialect %s entry %q", c.Name, v, c.Native, c.Dialect, *c.Entry)
		}
		matched[v.Entry] = true
	}
	// Every entry is pinned by a case, so none can be dropped or reordered
	// unnoticed.
	for _, e := range clients.Entries {
		if !matched[e.ID] {
			t.Errorf("entry %s: no native_client case matches it", e.ID)
		}
	}
}

// The table is well formed: known fields, effects and dialects, lowercase
// values, a reason and a source for each entry, and every dialect's wire
// behavior spelled out.
func TestClientTableWellFormed(t *testing.T) {
	var raw struct {
		About    string                       `json:"_about"`
		Schema   int                          `json:"schema"`
		Dialects map[string]map[string]any    `json:"dialects"`
		Entries  []map[string]json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(clientsJSON, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.Schema != 1 || raw.About == "" {
		t.Errorf("schema %d, _about %q", raw.Schema, raw.About)
	}
	for _, name := range []string{DialectSpec, DialectDraft, DialectOpenClaw, DialectChatGPT} {
		p, ok := clients.Dialects[name]
		if !ok || (p.Errors != "spec" && p.Errors != "chatgpt") || (p.WebhookID != "message" && p.WebhookID != "event_id") {
			t.Errorf("dialect %s: %+v", name, p)
		}
	}
	if len(clients.Dialects) != 4 {
		t.Errorf("%d dialects: a new one needs a constant here and support in the core and every SDK", len(clients.Dialects))
	}
	fields := []string{"id", "match", "without_push", "native", "dialect", "webhook_only", "reason", "source"}
	matchFields := []string{matchClient, matchClientPrefix, matchUAProduct, matchMetaKey, matchDetector}
	id := regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	seen := map[string]bool{}
	for i, rawEntry := range raw.Entries {
		for k := range rawEntry {
			if !slices.Contains(fields, k) {
				t.Errorf("entry %d: unknown field %q", i, k)
			}
		}
		e := clients.Entries[i]
		if !id.MatchString(e.ID) || seen[e.ID] {
			t.Errorf("entry %d: id %q missing, malformed or repeated", i, e.ID)
		}
		seen[e.ID] = true
		if e.Native != nativeNever && e.Native != nativeUndeclared && e.Native != nativeCapabilities {
			t.Errorf("%s: native %q", e.ID, e.Native)
		}
		if _, ok := clients.Dialects[e.Dialect]; !ok {
			t.Errorf("%s: unknown dialect %q", e.ID, e.Dialect)
		}
		if strings.TrimSpace(e.Reason) == "" || strings.TrimSpace(e.Source) == "" {
			t.Errorf("%s: needs a reason and a source", e.ID)
		}
		if len(e.Match) == 0 {
			t.Errorf("%s: matches everything", e.ID)
		}
		for field, values := range e.Match {
			if !slices.Contains(matchFields, field) {
				t.Errorf("%s: unknown match field %q", e.ID, field)
			}
			if len(values) == 0 {
				t.Errorf("%s: %s has no values", e.ID, field)
			}
			for _, v := range values {
				if v == "" || v != strings.ToLower(strings.TrimSpace(v)) && field != matchMetaKey {
					t.Errorf("%s: %s value %q isn't trimmed lowercase", e.ID, field, v)
				}
				if field == matchUAProduct && strings.ContainsAny(v, "/ \t") {
					t.Errorf("%s: product %q has a / or a space", e.ID, v)
				}
				if field == matchDetector && detectors[v] == nil {
					t.Errorf("%s: unknown detector %q", e.ID, v)
				}
			}
		}
	}
}

// overlaps reports whether some single value can satisfy both a value of
// field a (one entry's) and of field b (another's), for the name fields
// (client, client_prefix), or the same value of any other field.
func overlaps(fa string, va []string, fb string, vb []string) bool {
	for _, x := range va {
		for _, y := range vb {
			switch {
			case fa == matchClient && fb == matchClient:
				if x == y {
					return true
				}
			case fa == matchClient && fb == matchClientPrefix:
				if strings.HasPrefix(x, y) {
					return true
				}
			case fa == matchClientPrefix && fb == matchClient:
				if strings.HasPrefix(y, x) {
					return true
				}
			case fa == matchClientPrefix && fb == matchClientPrefix:
				if strings.HasPrefix(x, y) || strings.HasPrefix(y, x) {
					return true
				}
			case fa == fb:
				if x == y {
					return true
				}
			}
		}
	}
	return false
}

// nameField folds client and client_prefix into one key: both test the
// clientInfo name.
func nameField(f string) string {
	if f == matchClientPrefix {
		return matchClient
	}
	return f
}

// The lint: no two entries can match the same input ambiguously. Two
// entries that test the same kinds of facts (the clientInfo name, the
// User-Agent's products, a _meta key, a detector) and whose values overlap
// on every one of them would both match some input, and only file order
// would choose: that's refused, so each entry's place in the order is
// never what decides between two readings of the same fact. (Entries that
// test different facts, a name and a User-Agent, are ordered on purpose:
// ChatGPT's session before the bridged names, names before User-Agents.)
// An entry is also refused when an earlier one tests a subset of its facts
// with overlapping values: the earlier one shadows it.
func TestClientTableUnambiguous(t *testing.T) {
	keys := func(e clientEntry) []string {
		var out []string
		for f := range e.Match {
			out = append(out, nameField(f))
		}
		slices.Sort(out)
		return slices.Compact(out)
	}
	values := func(e clientEntry, key string) map[string][]string {
		out := map[string][]string{}
		for f, v := range e.Match {
			if nameField(f) == key {
				out[f] = v
			}
		}
		return out
	}
	overlapOn := func(a, b clientEntry, key string) bool {
		for fa, va := range values(a, key) {
			for fb, vb := range values(b, key) {
				if overlaps(fa, va, fb, vb) {
					return true
				}
			}
		}
		return false
	}
	for i, a := range clients.Entries {
		for _, b := range clients.Entries[i+1:] {
			ka, kb := keys(a), keys(b)
			subset := true
			for _, k := range ka {
				if !slices.Contains(kb, k) {
					subset = false
				}
			}
			if !subset {
				continue
			}
			all := true
			for _, k := range ka {
				if !overlapOn(a, b, k) {
					all = false
				}
			}
			if !all {
				continue
			}
			if slices.Equal(ka, kb) {
				t.Errorf("entries %s and %s can match the same input", a.ID, b.ID)
			} else {
				t.Errorf("entry %s shadows %s: it tests a subset of its facts with overlapping values", a.ID, b.ID)
			}
		}
	}
}

// The lint itself: it catches an exact name listed twice, a prefix that
// covers a later name, and a narrower entry after a broader one.
func TestClientTableLintCatches(t *testing.T) {
	if !overlaps(matchClient, []string{"a"}, matchClient, []string{"b", "a"}) ||
		!overlaps(matchClientPrefix, []string{"openai"}, matchClient, []string{"openai-mcp"}) ||
		!overlaps(matchClient, []string{"openai-mcp"}, matchClientPrefix, []string{"openai"}) ||
		!overlaps(matchClientPrefix, []string{"open"}, matchClientPrefix, []string{"openai"}) ||
		!overlaps(matchUAProduct, []string{"x"}, matchUAProduct, []string{"x"}) {
		t.Error("an overlap went unseen")
	}
	if overlaps(matchClient, []string{"openclaw"}, matchClient, []string{"openclaw-bundle-mcp"}) ||
		overlaps(matchUAProduct, []string{"ai-sdk"}, matchUAProduct, []string{"my-ai-sdk"}) {
		t.Error("an overlap that isn't")
	}
}

// User-Agent products, per the table's definition.
func TestUAProducts(t *testing.T) {
	for ua, want := range map[string][]string{
		"ai-sdk/2.0.97 node.js/26":  {"ai-sdk", "node.js"},
		"openai-mcp/1.0.0 (Codex)":  {"openai-mcp", "(codex)"},
		"node":                      {"node"},
		"  Claude-Code/2.1  ":       {"claude-code"},
		"":                          nil,
		"/x":                        nil,
		"Mozilla/5.0 ai-sdk/2.0.97": {"mozilla", "ai-sdk"},
	} {
		if got := uaProducts(ua); !slices.Equal(got, want) {
			t.Errorf("%q: %q, want %q", ua, got, want)
		}
	}
}

// The clients that take events by webhook alone (webhook_only): ChatGPT,
// OpenClaw's events plugin and Hermes Agent, by name, User-Agent or
// ChatGPT's session; not the bridged harnesses, OpenClaw's tool-calling
// client or an unknown client.
func TestClientTableWebhookOnly(t *testing.T) {
	for _, c := range []struct {
		in   ClientInput
		want bool
	}{
		{ClientInput{Client: "openai-mcp"}, true},
		{ClientInput{UserAgent: "openai-mcp/1.0.0"}, true},
		{ClientInput{Meta: map[string]any{"openai/session": "s1"}}, true},
		{ClientInput{Client: "OpenClaw"}, true},
		{ClientInput{Client: "hermes-agent"}, true},
		{ClientInput{UserAgent: "hermes-agent/0.23.1"}, true},
		{ClientInput{Client: "openclaw-bundle-mcp"}, false},
		{ClientInput{Client: "claude-code"}, false},
		{ClientInput{Client: "ai-sdk-mcp-client"}, false},
		{ClientInput{Client: "some-draft-client"}, false},
	} {
		if got := EvaluateClient(c.in).WebhookOnly; got != c.want {
			t.Errorf("%+v: webhook only %v, want %v", c.in, got, c.want)
		}
	}
}
