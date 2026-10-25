package appkit

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// jwksServer serves a JWKS with pub as kid k1, or fails while fail is set,
// counting fetches.
func jwksServer(t *testing.T, pub ed25519.PublicKey, fail *atomic.Bool, fetches *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		if fail.Load() {
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]any{"error": "down"}) // decodes, with no keys
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kid": "k1", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(pub)}}})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A failed JWKS fetch keeps the keys the app had.
func TestJWKSFailureKeepsKeys(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	var fail atomic.Bool
	var fetches atomic.Int32
	a := &App{Core: jwksServer(t, pub, &fail, &fetches).URL}
	if k, _ := a.key(context.Background(), "k1"); k == nil {
		t.Fatal("no key")
	}
	fail.Store(true)
	a.mu.Lock()
	a.jwksAt, a.jwksNext = time.Now().Add(-2*time.Hour), time.Time{} // stale: refetch
	a.mu.Unlock()
	if k, _ := a.key(context.Background(), "k1"); k == nil {
		t.Fatal("a failed fetch dropped the keys")
	}
}

// An unknown kid after a fetch that worked is unknown (checked), and
// doesn't fetch again within jwksRefetch.
func TestJWKSUnknownKid(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	var fail atomic.Bool
	var fetches atomic.Int32
	a := &App{Core: jwksServer(t, pub, &fail, &fetches).URL}
	for range 3 {
		if k, checked := a.key(context.Background(), "k2"); k != nil || !checked {
			t.Fatalf("unknown kid: key %v, checked %v; want nil, checked", k != nil, checked)
		}
	}
	if n := fetches.Load(); n != 1 {
		t.Fatalf("%d fetches for an unknown kid; want 1 (then wait jwksRefetch)", n)
	}
	if k, _ := a.key(context.Background(), "k1"); k == nil {
		t.Fatal("the known kid wasn't found")
	}
}

// A failed fetch, such as a fresh instance's first after a deploy, is
// retried after jwksRetry, not a minute later, and until then reports the
// keys as unchecked (not "unknown signing key").
func TestJWKSFailedFetchRetriesSoon(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	var fail atomic.Bool
	var fetches atomic.Int32
	fail.Store(true)
	a := &App{Core: jwksServer(t, pub, &fail, &fetches).URL}
	if k, checked := a.key(context.Background(), "k1"); k != nil || checked {
		t.Fatalf("failed fetch: key %v, checked %v; want nil, unchecked", k != nil, checked)
	}
	a.mu.Lock()
	next := time.Until(a.jwksNext)
	a.mu.Unlock()
	if next > jwksRetry {
		t.Fatalf("after a failed fetch the next waits %v; want at most %v", next, jwksRetry)
	}
	fail.Store(false)
	a.mu.Lock()
	a.jwksNext = time.Now().Add(-time.Millisecond) // jwksRetry has passed
	a.mu.Unlock()
	if k, checked := a.key(context.Background(), "k1"); k == nil || !checked {
		t.Fatal("the retry after a failed fetch didn't find the key")
	}
	if n := fetches.Load(); n != 2 {
		t.Fatalf("%d fetches; want 2", n)
	}
}

// The request's cancellation doesn't fail the fetch (which would refuse
// every token until the retry).
func TestJWKSFetchOutlivesRequest(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	var fail atomic.Bool
	var fetches atomic.Int32
	a := &App{Core: jwksServer(t, pub, &fail, &fetches).URL}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if k, checked := a.key(ctx, "k1"); k == nil || !checked {
		t.Fatal("a canceled request's fetch failed")
	}
}

// A fresh instance (empty cache) under concurrent requests fetches once and
// finds the key for all of them.
func TestJWKSFreshInstanceFetchesOnce(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	var fail atomic.Bool
	var fetches atomic.Int32
	a := &App{Core: jwksServer(t, pub, &fail, &fetches).URL}
	var wg sync.WaitGroup
	var missing atomic.Int32
	for range 20 {
		wg.Go(func() {
			if k, _ := a.key(context.Background(), "k1"); k == nil {
				missing.Add(1)
			}
		})
	}
	wg.Wait()
	if missing.Load() != 0 || fetches.Load() != 1 {
		t.Fatalf("%d requests missed the key, %d fetches; want 0 and 1", missing.Load(), fetches.Load())
	}
}
