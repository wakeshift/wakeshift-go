package appkit

import (
	"net/http"
	"time"
)

// RateLimiter allows max requests per window from each client address, in
// memory (per instance), as GuestAuth limits sign-ups: an IPv6 address
// counts as its /64, X-Forwarded-For is read only as the trusted proxies
// wrote it, and the addresses tracked at once are capped (past the cap, a
// new address is refused until old ones expire). For an app's own
// unauthenticated endpoints, such as OAuth client registration.
type RateLimiter struct {
	// ProxyHops is how many trusted proxies are in front, each appending to
	// X-Forwarded-For (0: the connection's address).
	ProxyHops int
	l         rateLimiter
}

// NewRateLimiter allows max requests per window from each address.
func NewRateLimiter(max int, window time.Duration, proxyHops int) *RateLimiter {
	return &RateLimiter{ProxyHops: proxyHops, l: rateLimiter{max: max, window: window}}
}

// Allow records a request, reporting whether it's within the limit.
func (l *RateLimiter) Allow(r *http.Request, now time.Time) bool {
	return l.l.allow(clientAddr(r, l.ProxyHops), now)
}
