package appkit

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"strings"

	"github.com/wakeshift/wakeshift-go/internal/appkey"
)

// App keys from the Wakeshift console look like API keys but are the app's
// Ed25519 private key:
//
//	wst_<50 base58 characters>
//
// the 32-byte seed and a CRC32 checksum, which names no app: Open asks
// Wakeshift which app the key is (GET /api/apps/v1/app). Older keys,
// which keep working, carry the app's ID and a mode:
//
//	wst_<test|live>_<app id>_<seed, base64url>
//
// The key never leaves the app: it signs short-lived assertions (see App),
// each naming the key by its kid, and Wakeshift keeps only the public half.
const keyPrefix = appkey.Prefix

// DefaultCore is Wakeshift's production core: where a key goes unless
// WAKESHIFT_CORE (or Open's core) says otherwise. A wst_test_ key has no
// default: it came from another console.
const DefaultCore = "https://app.wakeshift.ai"

// ParseAppKey reads an app key from the console: its mode and the app's ID
// (both empty for a current key, which names neither: see Open) and its
// private key. A current key's checksum is checked, so a mistyped key
// fails here, not at Wakeshift.
//
// Deprecated: the mode means nothing now, and current keys don't carry the
// app's ID. Use Open, or ParseKey for the private key.
func ParseAppKey(s string) (mode, appID string, key ed25519.PrivateKey, err error) {
	k, err := appkey.Parse(s)
	if err != nil {
		return "", "", nil, errors.New("appkit: " + err.Error())
	}
	return k.Mode, k.AppID, k.Private(), nil
}

// FromKey returns the App an older app key from the console (wst_test_…,
// wst_live_…) names, connected to Wakeshift at core. mcpURL is the app's
// MCP endpoint, if it has one. A current key doesn't name its app: use
// Open, which asks Wakeshift.
func FromKey(appKey, core, mcpURL string) (*App, error) {
	_, id, key, err := ParseAppKey(appKey)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, errors.New("appkit: this key doesn't name its app; use appkit.Open(ctx, key, core, mcpURL), which asks Wakeshift")
	}
	return &App{ID: id, Key: key, Core: core, MCPURL: mcpURL}, nil
}

// Open returns the App for an app key from the console, either format,
// with its ID: from the key, if it names the app, or else from Wakeshift
// (GET /api/apps/v1/app, once). core is Wakeshift's base URL; "" reads
// WAKESHIFT_CORE, then falls back to DefaultCore, except for a wst_test_
// key, which needs one of the two. mcpURL is the app's MCP endpoint, if it
// has one.
func Open(ctx context.Context, appKey, core, mcpURL string) (*App, error) {
	k, err := appkey.Parse(appKey)
	if err != nil {
		return nil, errors.New("appkit: " + err.Error())
	}
	if core == "" {
		core = os.Getenv("WAKESHIFT_CORE")
	}
	if core == "" {
		if k.Mode == appkey.Test {
			return nil, errors.New("appkit: no core URL for this test key: set WAKESHIFT_CORE, or pass core, to the address of the console the key came from (for example https://app.staging.wakeshift.ai)")
		}
		core = DefaultCore
	}
	a := &App{ID: k.AppID, Key: k.Private(), Core: core, MCPURL: mcpURL}
	if a.ID == "" {
		info, err := a.Info(ctx)
		if err != nil {
			return nil, err
		}
		a.ID = info.ID
	}
	return a, nil
}

// AppInfo is which app and key signed a request, as Wakeshift says.
type AppInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Key  struct {
		KeyID string `json:"key_id"` // the console's key ID
		Name  string `json:"name"`
	} `json:"key"`
}

// Info asks Wakeshift which app and key a.Key is (GET /api/apps/v1/app).
func (a *App) Info(ctx context.Context) (AppInfo, error) {
	var info AppInfo
	err := a.appCall(ctx, "GET", "/api/apps/v1/app", nil, &info)
	if err == nil && info.ID == "" {
		err = errors.New("appkit: Wakeshift didn't say which app this key is")
	}
	return info, err
}

// kid names a.Key in assertions (appkey.KeyID): Wakeshift finds the key,
// and so the app, by it.
func (a *App) kid() string { return appkey.KeyID(a.Key.Public().(ed25519.PublicKey)) }

// Load returns the App for key: an app key from the console (wst_…), or a
// bare base64 Ed25519 key (ParseKey) for the app id, the way Wakeshift's
// own apps are deployed. An older key names its app; for a current one,
// which doesn't, the app is id.
func Load(id, key, core, mcpURL string) (*App, error) {
	if strings.HasPrefix(strings.TrimSpace(key), keyPrefix) {
		_, named, k, err := ParseAppKey(key)
		if err != nil {
			return nil, err
		}
		if named != "" {
			id = named
		}
		return &App{ID: id, Key: k, Core: core, MCPURL: mcpURL}, nil
	}
	k, err := ParseKey(key)
	if err != nil {
		return nil, err
	}
	return &App{ID: id, Key: k, Core: core, MCPURL: mcpURL}, nil
}
