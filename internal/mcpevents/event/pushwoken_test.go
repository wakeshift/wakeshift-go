package event

import "testing"

func TestWokenByPush(t *testing.T) {
	for _, c := range []struct {
		client, ua string
		want       bool
	}{
		{"opencode", "", true},
		{" OpenCode ", "", true},
		{"", "opencode/1.2", true},
		{"claude-code", "", false},
		{"", "claude-code/2.1", false},
		{"acme-agent", "acme/1", false},
		{"openai-mcp", "", false},
	} {
		if got := WokenByPush(c.client, c.ua, nil); got != c.want {
			t.Errorf("%q %q: %v, want %v", c.client, c.ua, got, c.want)
		}
	}
}
