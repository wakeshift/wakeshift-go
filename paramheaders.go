package wakeshift

// SEP-3415's custom headers from subscription arguments: an x-mcp-header
// annotation on a property of an event's arguments' schema mirrors that
// argument into an Mcp-Param-{Name} header, under the 2026-07-28
// Streamable HTTP transport's rules for tools. The same rules in every SDK
// (sdk/typescript/test/mcp-events-interop.json mcp_param_schema,
// mcp_param; the TypeScript SDK's spec.ts paramHeadersOf and
// paramHeaderRefusal).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/textproto"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/wakeshift/wakeshift-go/internal/mcpevents/event"
)

// paramAnnotation is the JSON Schema annotation naming an argument's
// Mcp-Param-{Name} header.
const paramAnnotation = "x-mcp-header"

// paramHeader is one annotated argument: its path (a chain of properties
// keys), the header's {Name}, and its type.
type paramHeader struct {
	Path []string `json:"path"`
	Name string   `json:"name"`
	Type string   `json:"type"`
}

var (
	// RFC 9110 §5.6.2 token (1*tchar).
	tcharRE = regexp.MustCompile("^[!#$%&'*+\\-.^_`|~0-9A-Za-z]+$")
	// A plain (not Base64) header value: visible ASCII, spaces and tabs.
	plainHeaderRE = regexp.MustCompile(`^[\t\x20-\x7e]*$`)
	// A header an integer is compared with as a number: a plain decimal.
	decimalRE = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)
)

// offChain are the keywords whose subschemas aren't statically reachable
// (the transport's Schema Extension): an annotation under any is invalid.
// schemaMaps hold a map of subschemas rather than one or a list.
var (
	offChain = map[string]bool{"items": true, "prefixItems": true, "contains": true, "additionalProperties": true,
		"unevaluatedProperties": true, "unevaluatedItems": true, "propertyNames": true, "not": true, "if": true, "then": true,
		"else": true, "contentSchema": true, "anyOf": true, "oneOf": true, "allOf": true, "patternProperties": true,
		"dependentSchemas": true, "dependencies": true, "$defs": true, "definitions": true}
	schemaMaps = map[string]bool{"patternProperties": true, "dependentSchemas": true, "dependencies": true, "$defs": true, "definitions": true}
	paramTypes = map[string]bool{"string": true, "integer": true, "boolean": true}
)

// paramHeadersOf is the x-mcp-header annotations in an event's arguments'
// schema, sorted by path, or why one is invalid: each is a non-empty RFC
// 9110 token, on a property typed "string", "integer" or "boolean" (one
// type: never number), reached from the root through properties keys
// only, and unique in any case. A spec client drops an event type that
// breaks one from events/list, so DefineEvent refuses it.
func paramHeadersOf(schema any) ([]paramHeader, error) {
	type item struct {
		node      any
		path      []string
		reachable bool
	}
	var out []paramHeader
	seen := map[string]string{}
	stack := []item{{node: schema, reachable: true}}
	for len(stack) > 0 {
		it := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node, ok := it.node.(map[string]any)
		if !ok {
			continue
		}
		if raw, has := node[paramAnnotation]; has {
			where := "the schema's root"
			if len(it.path) > 0 {
				where = strings.Join(it.path, ".")
			}
			if !it.reachable || len(it.path) == 0 {
				return nil, fmt.Errorf("%s on %s: only a property reached from the root through properties (not items, anyOf, oneOf, allOf, not, if/then/else, additionalProperties or $defs) can carry it", paramAnnotation, where)
			}
			name, ok := raw.(string)
			if !ok || !tcharRE.MatchString(name) {
				b, _ := json.Marshal(raw)
				return nil, fmt.Errorf("%s on %s: %s isn't a header name (letters, digits and !#$%%&'*+-.^_`|~, at least one)", paramAnnotation, where, b)
			}
			typ, ok := node["type"].(string)
			if !ok || !paramTypes[typ] {
				b, _ := json.Marshal(node["type"])
				return nil, fmt.Errorf("%s on %s: only a string, integer or boolean property can carry it (its type is %s)", paramAnnotation, where, b)
			}
			if other, dup := seen[strings.ToLower(name)]; dup {
				return nil, fmt.Errorf("%s on %s: %s is already %s's (header names are case-insensitive)", paramAnnotation, where, name, other)
			}
			seen[strings.ToLower(name)] = where
			out = append(out, paramHeader{Path: it.path, Name: name, Type: typ})
		}
		for k, v := range node {
			if props, ok := v.(map[string]any); ok && k == "properties" {
				for prop, sub := range props {
					stack = append(stack, item{node: sub, path: append(slices.Clone(it.path), prop), reachable: it.reachable})
				}
				continue
			}
			if !offChain[k] {
				continue
			}
			var subs []any
			switch s := v.(type) {
			case []any:
				subs = s
			case map[string]any:
				if schemaMaps[k] {
					for _, sub := range s {
						subs = append(subs, sub)
					}
				} else {
					subs = []any{s}
				}
			}
			for _, sub := range subs {
				stack = append(stack, item{node: sub, path: it.path, reachable: false})
			}
		}
	}
	slices.SortFunc(out, func(a, b paramHeader) int {
		return strings.Compare(strings.Join(a.Path, "\x00"), strings.Join(b.Path, "\x00"))
	})
	return out, nil
}

// paramHeaderValue is an Mcp-Param header's value: trimmed of spaces and
// tabs, decoded (event.HeaderValue), and false if malformed (a bad
// sentinel, or a plain value with anything but visible ASCII, spaces and
// tabs).
func paramHeaderValue(h string) (string, bool) {
	v := strings.Trim(h, " \t")
	decoded, ok := event.HeaderValue(v)
	if !ok {
		return "", false
	}
	if decoded == v && !plainHeaderRE.MatchString(v) {
		return "", false
	}
	return decoded, true
}

// headerText is a body value as a client writes it in a header, before
// encoding: a string as it is, a boolean as true or false, a number in
// decimal; false for anything else (an object, an array), which has none.
func headerText(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case bool:
		if x {
			return "true", true
		}
		return "false", true
	case json.Number:
		if n, ok := integral(x); ok {
			return n.String(), true
		}
		f, err := x.Float64()
		if err != nil {
			return "", false
		}
		return strconv.FormatFloat(f, 'f', -1, 64), true
	}
	return "", false
}

// integral is a body number's value if it's a whole number (3, 3.0,
// 4.2e1), exactly. One in exponent form goes through float64: big.Rat
// would expand an exponent like 1e999999999 in full.
func integral(n json.Number) (*big.Int, bool) {
	if strings.ContainsAny(string(n), "eE") {
		f, err := n.Float64()
		if err != nil || math.IsInf(f, 0) || f != math.Trunc(f) {
			return nil, false
		}
		i, _ := big.NewFloat(f).Int(nil)
		return i, true
	}
	r, ok := new(big.Rat).SetString(string(n))
	if !ok || !r.IsInt() {
		return nil, false
	}
	return r.Num(), true
}

// paramArguments decodes params.arguments for the check, numbers exact.
func paramArguments(params json.RawMessage) any {
	var p struct {
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(params, &p) != nil || len(p.Arguments) == 0 {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(p.Arguments))
	d.UseNumber()
	var args any
	if d.Decode(&args) != nil {
		return nil
	}
	return args
}

// valueAt is the value at path in args, or nil.
func valueAt(args any, path []string) any {
	node := args
	for _, k := range path {
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = m[k]
	}
	return node
}

// paramHeaderRefusal is why an events/poll, events/stream,
// events/subscribe or events/unsubscribe is refused for its Mcp-Param
// headers (HTTP 400, HeaderMismatch), or "". Strict, as the spec requires,
// unlike a missing Mcp-Name: each annotated argument with a
// value other than null needs its header, once, decoding to the value as
// the client writes it (an integer compared as a number: 42.0 is 42), and
// a header for an argument with no value or no header form is refused
// too. Headers no annotation names are ignored. header: the request's
// headers.
func paramHeaderRefusal(annotated []paramHeader, args any, header http.Header) string {
	for _, p := range annotated {
		name := "Mcp-Param-" + p.Name
		where := "arguments." + strings.Join(p.Path, ".")
		got := header.Values(textproto.CanonicalMIMEHeaderKey(name))
		if len(got) > 1 {
			return "Header mismatch: more than one " + name + " header"
		}
		value := valueAt(args, p.Path)
		text, ok := "", false
		if value != nil {
			text, ok = headerText(value)
		}
		if !ok {
			if len(got) > 0 {
				return "Header mismatch: " + name + " is sent but " + where + " has no value it mirrors"
			}
			continue
		}
		if len(got) == 0 {
			return "Header mismatch: " + name + " is missing (" + where + " has a value)"
		}
		decoded, ok := paramHeaderValue(got[0])
		same := ok && decoded == text
		// An integer (an integral body number for an integer property)
		// against a plain decimal header: compared as numbers, exactly.
		if n, isNum := value.(json.Number); ok && isNum && p.Type == "integer" && decimalRE.MatchString(decoded) {
			if body, okB := integral(n); okB {
				whole, fraction, _ := strings.Cut(decoded, ".")
				head, okH := new(big.Int).SetString(whole, 10)
				same = okH && strings.Trim(fraction, "0") == "" && head.Cmp(body) == 0
			}
		}
		if !same {
			return "Header mismatch: " + name + " doesn't match " + where
		}
	}
	return ""
}
