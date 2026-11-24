package event

import "github.com/wakeshift/wakeshift-go/internal/harnesses"

// WokenByPush reports whether push (events/stream) is what wakes a client
// in its open conversation: its first matching entry while the server
// serves no push is a without_push one (opencode, by name or User-Agent
// product). Where push is served, such a client is decided by its
// capabilities (EvaluateClient with ServesPush), and its call.subscribe
// tells the core it declared the extension.
func WokenByPush(client, userAgent string, meta map[string]any) bool {
	name := harnesses.ClientName(client)
	products := uaProducts(userAgent)
	for _, e := range clients.Entries {
		if e.matches(name, client, products, meta) {
			return e.WithoutPush
		}
	}
	return false
}
