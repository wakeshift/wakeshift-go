package appkit

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"

	"github.com/wakeshift/wakeshift-go/internal/jws"
)

const testMeta = "https://app.example/.well-known/oauth-protected-resource"

// The challenge says error="invalid_token" only when a token was sent.
func TestChallengeHeader(t *testing.T) {
	a := &App{MCPURL: "https://app.example/mcp"}
	cases := []struct {
		name string
		do   func(http.ResponseWriter)
		want string
	}{
		{"no token", a.Challenge, `Bearer resource_metadata="` + testMeta + `"`},
		{"refused", func(w http.ResponseWriter) { a.RefuseToken(w, &TokenError{Reason: ReasonInvalid}) },
			`Bearer resource_metadata="` + testMeta + `", error="invalid_token", error_description="token not valid"`},
		{"expired", func(w http.ResponseWriter) { a.RefuseToken(w, &TokenError{Reason: ReasonExpired, Past: time.Second}) },
			`Bearer resource_metadata="` + testMeta + `", error="invalid_token", error_description="token expired"`},
		{"nil error", func(w http.ResponseWriter) { a.RefuseToken(w, nil) },
			`Bearer resource_metadata="` + testMeta + `", error="invalid_token", error_description="token not valid"`},
		// The description stays generic: no issuer or app details.
		{"another app", func(w http.ResponseWriter) { a.RefuseToken(w, &TokenError{Reason: ReasonAudience}) },
			`Bearer resource_metadata="` + testMeta + `", error="invalid_token", error_description="token not valid"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c.do(w)
			if w.Code != 401 || w.Header().Get("WWW-Authenticate") != c.want {
				t.Fatalf("got %d %q; want 401 %q", w.Code, w.Header().Get("WWW-Authenticate"), c.want)
			}
		})
	}
}

// logs captures what's logged while it runs.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func captureLogs(t *testing.T) *logs {
	l := &logs{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return l
}

// refusal is the one "bearer token refused" line logged.
func refusal(t *testing.T, l *logs) map[string]any {
	t.Helper()
	var found []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(l.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == "appkit: bearer token refused" {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d refusal lines; want 1:\n%s", len(found), l.String())
	}
	return found[0]
}

// Every way App.Verify refuses a token reaches the client as
// invalid_token and the log as its reason, never with the token.
func TestRequireBearerAppTokens(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	var fail atomic.Bool
	var fetches atomic.Int32
	core := jwksServer(t, pub, &fail, &fetches).URL
	a := &App{Core: core, MCPURL: "https://app.example/mcp"}
	sign := func(key ed25519.PrivateKey, kid, iss, aud string, exp time.Time) string {
		tok, err := jws.Sign(key, kid, "at+jwt", map[string]any{"iss": iss, "aud": aud, "exp": exp.Unix(),
			"sub": "user-123", "client_id": "client-xyz", "wakeshift_tenant": "ws-abc", "wakeshift_agent": "codex"})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	verify := func(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		c, err := a.Verify(ctx, token)
		if err != nil {
			return nil, err
		}
		return &auth.TokenInfo{UserID: c.Workspace + "/" + c.Agent, Expiration: time.Unix(c.Expires, 0)}, nil
	}
	var reached atomic.Bool
	h := RequireBearer(verify, testMeta)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth.TokenInfoFromContext(r.Context()) == nil {
			t.Error("no TokenInfo in the request's context")
		}
		reached.Store(true)
	}))
	hour := time.Now().Add(time.Hour)
	cases := []struct {
		name, token, reason, desc string
		jwksDown                  bool
	}{
		{"malformed", "not-a-token", ReasonInvalid, "token not valid", false},
		{"bad signature", sign(other, "k1", core, a.MCPURL, hour), ReasonInvalid, "token not valid", false},
		{"unknown key", sign(other, "k9", core, a.MCPURL, hour), ReasonUnknownKey, "token not valid", false},
		{"keys unavailable", sign(other, "k8", core, a.MCPURL, hour), ReasonKeysUnavailable, "token not valid", true},
		{"another issuer", sign(priv, "k1", "https://evil.example", a.MCPURL, hour), ReasonIssuer, "token not valid", false},
		{"another app", sign(priv, "k1", core, "https://other.example/mcp", hour), ReasonAudience, "token not valid", false},
		{"expired", sign(priv, "k1", core, a.MCPURL, time.Now().Add(-90*time.Second)), ReasonExpired, "token expired", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := captureLogs(t)
			fail.Store(c.jwksDown)
			a.mu.Lock()
			a.jwks, a.jwksNext, a.jwksFail = nil, time.Time{}, false // a fresh instance
			a.mu.Unlock()
			r := httptest.NewRequest("POST", "/mcp?code=secret-query", nil)
			r.Header.Set("Authorization", "Bearer "+c.token)
			r.Header.Set("User-Agent", "codex-mcp-client/0.163.0")
			w := httptest.NewRecorder()
			reached.Store(false)
			h.ServeHTTP(w, r)
			want := `Bearer resource_metadata="` + testMeta + `", error="invalid_token", error_description="` + c.desc + `"`
			if w.Code != 401 || w.Header().Get("WWW-Authenticate") != want || reached.Load() {
				t.Fatalf("got %d %q (reached %v); want 401 %q", w.Code, w.Header().Get("WWW-Authenticate"), reached.Load(), want)
			}
			m := refusal(t, l)
			if m["reason"] != c.reason || m["client"] != "codex-mcp-client/0.163.0" {
				t.Fatalf("logged %v; want reason %q, client codex-mcp-client/0.163.0", m, c.reason)
			}
			wantLevel := "WARN"
			if c.reason == ReasonExpired {
				wantLevel = "INFO"
				if s, _ := m["past_exp_s"].(float64); s < 89 || s > 95 {
					t.Fatalf("past_exp_s %v; want about 90", m["past_exp_s"])
				}
			} else if _, ok := m["past_exp_s"]; ok {
				t.Fatalf("past_exp_s logged for %s", c.reason)
			}
			if m["level"] != wantLevel {
				t.Fatalf("level %v; want %s", m["level"], wantLevel)
			}
			out := l.String()
			for _, secret := range []string{c.token, "secret-query", "user-123", "client-xyz", "ws-abc"} {
				if strings.Contains(out, secret) {
					t.Fatalf("the log has %q:\n%s", secret, out)
				}
			}
		})
	}

	// A good token reaches the handler, with its TokenInfo.
	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("Authorization", "Bearer "+sign(priv, "k1", core, a.MCPURL, hour))
	fail.Store(false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || !reached.Load() {
		t.Fatalf("a good token: %d, reached %v", w.Code, reached.Load())
	}
}

// Guest tokens: no token gets the plain challenge; a bad or expired one
// gets invalid_token, logged with why.
func TestRequireBearerGuestTokens(t *testing.T) {
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	g := &GuestAuth{App: &App{Key: key, MCPURL: "https://app.example/mcp"}}
	verify := func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		gu, err := g.Verify(token)
		if err != nil {
			return nil, err
		}
		return &auth.TokenInfo{UserID: gu.Workspace + "/" + gu.Agent, Expiration: gu.Expires}, nil
	}
	h := RequireBearer(verify, testMeta)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	expired := g.sign("access", guestToken{Workspace: "ws-abc", Agent: "codex", Client: "Codex", Expires: time.Now().Add(-5 * time.Second).Unix()})
	forged := g.sign("refresh", guestToken{Workspace: "ws-abc", Agent: "codex", Expires: time.Now().Add(time.Hour).Unix()})
	good := g.sign("access", guestToken{Workspace: "ws-abc", Agent: "codex", Expires: time.Now().Add(time.Hour).Unix()})
	cases := []struct {
		name, auth, header, reason string
		code                       int
	}{
		{"no token", "", `Bearer resource_metadata="` + testMeta + `"`, "", 401},
		{"not bearer", "Basic abc", `Bearer resource_metadata="` + testMeta + `"`, "", 401},
		{"expired", "Bearer " + expired, `Bearer resource_metadata="` + testMeta + `", error="invalid_token", error_description="token expired"`, ReasonExpired, 401},
		{"refresh token as access", "Bearer " + forged, `Bearer resource_metadata="` + testMeta + `", error="invalid_token", error_description="token not valid"`, ReasonInvalid, 401},
		{"good", "Bearer " + good, "", "", 200},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := captureLogs(t)
			r := httptest.NewRequest("POST", "/mcp", nil)
			if c.auth != "" {
				r.Header.Set("Authorization", c.auth)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != c.code || w.Header().Get("WWW-Authenticate") != c.header {
				t.Fatalf("got %d %q; want %d %q", w.Code, w.Header().Get("WWW-Authenticate"), c.code, c.header)
			}
			if c.reason == "" {
				if strings.Contains(l.String(), "bearer token refused") {
					t.Fatalf("logged a refusal: %s", l.String())
				}
				return
			}
			if m := refusal(t, l); m["reason"] != c.reason {
				t.Fatalf("logged %v; want reason %q", m, c.reason)
			}
			if strings.Contains(l.String(), strings.TrimPrefix(c.auth, "Bearer ")) || strings.Contains(l.String(), "ws-abc") {
				t.Fatalf("the log has the token or its claims: %s", l.String())
			}
		})
	}
}

// A verifier's own failure (not a refused token) is a 500, not a challenge.
func TestRequireBearerVerifierError(t *testing.T) {
	captureLogs(t)
	h := RequireBearer(func(context.Context, string, *http.Request) (*auth.TokenInfo, error) {
		return nil, context.DeadlineExceeded
	}, testMeta)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("reached the handler") }))
	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("Authorization", "Bearer x")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 500 || w.Header().Get("WWW-Authenticate") != "" {
		t.Fatalf("got %d %q; want 500 with no challenge", w.Code, w.Header().Get("WWW-Authenticate"))
	}
}
