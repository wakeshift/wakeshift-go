package wakeshift

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// client-requests.json: the bounds, the prefix, and what the SDK decides
// for each request, answer and call, as the TypeScript SDK's tests read it.

type clientRequestVectors struct {
	Bounds struct {
		MessageMax      int   `json:"message_max"`
		MessageLinesMax int   `json:"message_lines_max"`
		TitleMax        int   `json:"title_max"`
		FieldsMax       int   `json:"fields_max"`
		OptionsMax      int   `json:"options_max"`
		OutcomeTextMax  int   `json:"outcome_text_max"`
		TimeoutMs       int64 `json:"timeout_ms"`
		ReportWaitMs    int64 `json:"report_wait_ms"`
		SessionWindowMs int64 `json:"session_window_ms"`
	} `json:"bounds"`
	Prefix      string   `json:"prefix"`
	OutcomeKeys []string `json:"outcome_keys"`
	Validate    []struct {
		Name         string          `json:"name"`
		App          string          `json:"app"`
		Request      json.RawMessage `json:"request"`
		Base         *int            `json:"base"`
		Patch        map[string]any  `json:"patch"`
		DropOutcome  string          `json:"drop_outcome"`
		PatchOutcome map[string]any  `json:"patch_outcome"`
		Want         string          `json:"want"`
	} `json:"validate"`
	Outcome []struct {
		Name   string         `json:"name"`
		Base   int            `json:"base"`
		Result map[string]any `json:"result"`
		Want   string         `json:"want"`
	} `json:"outcome"`
	Offer []struct {
		Name         string         `json:"name"`
		Capabilities map[string]any `json:"capabilities"`
		Protocol     string         `json:"protocol"`
		HTTP         bool           `json:"http"`
		Session      string         `json:"session"`
		Off          bool           `json:"off"`
		Recent       bool           `json:"recent"`
		Want         bool           `json:"want"`
	} `json:"offer"`
}

func readClientRequestVectors(t *testing.T) clientRequestVectors {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(goldenDir, "client-requests.json"))
	if os.IsNotExist(err) {
		t.Skip("no shared golden files here")
	}
	if err != nil {
		t.Fatal(err)
	}
	var v clientRequestVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

var (
	vectorLetters = regexp.MustCompile(`^\{(\d+) x\}$`)
	vectorLines   = regexp.MustCompile(`^\{(\d+) lines\}$`)
)

// expandVector turns {N x} and {N lines} into their strings.
func expandVector(v any) any {
	switch v := v.(type) {
	case string:
		if m := vectorLetters.FindStringSubmatch(v); m != nil {
			n, _ := strconv.Atoi(m[1])
			return strings.Repeat("x", n)
		}
		if m := vectorLines.FindStringSubmatch(v); m != nil {
			n, _ := strconv.Atoi(m[1])
			return strings.TrimSuffix(strings.Repeat("x\n", n), "\n")
		}
		return v
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = expandVector(x)
		}
		return out
	case map[string]any:
		out := map[string]any{}
		for k, x := range v {
			out[k] = expandVector(x)
		}
		return out
	}
	return v
}

// requestOfVector is a validate vector's request, as JSON.
func requestOfVector(t *testing.T, f clientRequestVectors, i int) json.RawMessage {
	t.Helper()
	vec := f.Validate[i]
	if vec.Request != nil {
		var r any
		if err := json.Unmarshal(vec.Request, &r); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(expandVector(r))
		return b
	}
	var r map[string]any
	if err := json.Unmarshal(requestOfVector(t, f, *vec.Base), &r); err != nil {
		t.Fatal(err)
	}
	for k, v := range vec.Patch {
		r[k] = expandVector(v)
	}
	outcomes, _ := r["outcomes"].(map[string]any)
	if vec.DropOutcome != "" {
		delete(outcomes, vec.DropOutcome)
	}
	if vec.PatchOutcome != nil {
		o := outcomes[vec.PatchOutcome["key"].(string)].(map[string]any)
		for k, v := range vec.PatchOutcome {
			if k != "key" {
				o[k] = expandVector(v)
			}
		}
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestClientRequestBoundsGolden(t *testing.T) {
	f := readClientRequestVectors(t)
	b := clientRequestBounds
	got := []int64{int64(b.messageMax), int64(b.messageLinesMax), int64(b.titleMax), int64(b.fieldsMax), int64(b.optionsMax), int64(b.outcomeTextMax),
		b.timeout.Milliseconds(), b.reportWait.Milliseconds(), b.sessionWindow.Milliseconds()}
	fb := f.Bounds
	want := []int64{int64(fb.MessageMax), int64(fb.MessageLinesMax), int64(fb.TitleMax), int64(fb.FieldsMax), int64(fb.OptionsMax), int64(fb.OutcomeTextMax),
		fb.TimeoutMs, fb.ReportWaitMs, fb.SessionWindowMs}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("bounds: got %v, want %v", got, want)
			break
		}
	}
	if got, want := requestPrefix("Shipyard"), strings.ReplaceAll(f.Prefix, "{app}", "Shipyard"); got != want {
		t.Errorf("prefix %q, want %q", got, want)
	}
	if got := requestPrefix("  "); got != "Wakeshift, for an app:" {
		t.Errorf("no app: %q", got)
	}
	if got := requestPrefix(strings.Repeat("é", 90)); got != "Wakeshift, for "+strings.Repeat("é", 80)+":" {
		t.Errorf("a long name: %q", got)
	}
	if strings.Join(f.OutcomeKeys, ",") != strings.Join(outcomeKeys, ",") {
		t.Errorf("outcome keys %v, want %v", outcomeKeys, f.OutcomeKeys)
	}
}

func TestClientRequestValidateGolden(t *testing.T) {
	f := readClientRequestVectors(t)
	if len(f.Validate) == 0 {
		t.Fatal("no validate vectors")
	}
	for i, vec := range f.Validate {
		t.Run(vec.Name, func(t *testing.T) {
			if got := validateRequest(requestOfVector(t, f, i), vec.App); got != vec.Want {
				t.Errorf("got %q, want %q", got, vec.Want)
			}
		})
	}
}

func TestClientRequestOutcomeGolden(t *testing.T) {
	f := readClientRequestVectors(t)
	if len(f.Outcome) == 0 {
		t.Fatal("no outcome vectors")
	}
	for _, vec := range f.Outcome {
		t.Run(vec.Name, func(t *testing.T) {
			r, bad := parseRequest(requestOfVector(t, f, vec.Base), f.Validate[vec.Base].App)
			if bad != "" {
				t.Fatalf("the base request is refused: %s", bad)
			}
			if got := outcomeKey(vec.Result, r); got != vec.Want {
				t.Errorf("got %q, want %q", got, vec.Want)
			}
		})
	}
}

func TestClientRequestOfferGolden(t *testing.T) {
	f := readClientRequestVectors(t)
	if len(f.Offer) == 0 {
		t.Fatal("no offer vectors")
	}
	for _, vec := range f.Offer {
		t.Run(vec.Name, func(t *testing.T) {
			got := offersForm(offerInput{capabilities: vec.Capabilities, protocol: vec.Protocol, http: vec.HTTP, session: vec.Session, off: vec.Off, recent: vec.Recent})
			if got != vec.Want {
				t.Errorf("got %v, want %v", got, vec.Want)
			}
		})
	}
}

// The core's order of a form's fields decides which answers first, and is
// what the client is sent; Go's maps don't keep it, so the SDK reads it
// from the JSON.
func TestClientRequestFieldOrder(t *testing.T) {
	raw := json.RawMessage(`{"token":"t","kind":"elicit.form","message":"m",
	 "schema":{"type":"object","properties":{"z":{"type":"boolean","title":"Z"},"a":{"type":"boolean","title":"A"}}},
	 "outcomes":{"z=true":{"text":"","placement":"after","beacon":"keep"},"z=false":{"text":"","placement":"after","beacon":"keep"},
	  "a=true":{"text":"","placement":"after","beacon":"keep"},"a=false":{"text":"","placement":"after","beacon":"keep"},
	  "decline":{"text":"","placement":"after","beacon":"keep"},"cancel":{"text":"","placement":"after","beacon":"keep"},
	  "timeout":{"text":"","placement":"after","beacon":"keep"},"unsupported":{"text":"","placement":"after","beacon":"keep"}}}`)
	r, bad := parseRequest(raw, "App")
	if bad != "" {
		t.Fatal(bad)
	}
	if got := outcomeKey(map[string]any{"action": "accept", "content": map[string]any{"a": true, "z": false}}, r); got != "z=false" {
		t.Errorf("the first field in the core's order names the outcome: %q", got)
	}
	if !strings.Contains(string(r.schema), `"z":{"type":"boolean","title":"Z"},"a"`) {
		t.Errorf("the schema the client is sent: %s", r.schema)
	}
}

// At most one form per session in the window: a second claim within it
// fails and records nothing; after it, a claim succeeds again.
func TestFormSessionsWindow(t *testing.T) {
	var f formSessions
	now := time.Unix(1_000_000, 0)
	if f.recent("s", now) || !f.claim("s", now) {
		t.Fatal("first claim")
	}
	if !f.recent("s", now.Add(time.Minute)) || f.claim("s", now.Add(time.Minute)) {
		t.Fatal("a second claim within the window")
	}
	if !f.claim("other", now) {
		t.Fatal("another session")
	}
	later := now.Add(clientRequestBounds.sessionWindow)
	if f.recent("s", later) || !f.claim("s", later) {
		t.Fatal("after the window")
	}
}
