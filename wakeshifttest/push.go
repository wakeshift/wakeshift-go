package wakeshifttest

// The fake's MCP Events push upstream (POST /api/apps/v1/events/stream), as
// Wakeshift serves it: NDJSON frames, active first (created when the
// stream made its subscription), an event for each matching emit, and
// heartbeats at the grant, until the SDK ends the request. Cursors are the
// fake's own (c2.test.<subscription>.<seq>). A client's events/stream to a
// server attached with the test client gets them, so a push client can be
// tested offline.

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// pushSub is one push subscription the fake holds.
type pushSub struct {
	id, event, args, user string
	seq                   atomic.Int64 // read by heartbeats without f.mu
}

// pushStream is one stream the fake holds open.
type pushStream struct {
	sub  *pushSub
	out  chan []byte
	done chan struct{}
	once sync.Once
}

// end closes the stream (its response's body ends).
func (s *pushStream) end() { s.once.Do(func() { close(s.done) }) }

// send queues one frame; false once the stream ended.
func (s *pushStream) send(frame map[string]any) bool {
	b, _ := json.Marshal(frame)
	select {
	case s.out <- append(b, '\n'):
		return true
	case <-s.done:
		return false
	}
}

func (s *pushStream) cursor() string { return "c2.test." + s.sub.id + "." + itoa(s.sub.seq.Load()) }

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// stream answers /events/stream (f.mu not held).
func (f *fake) stream(r *http.Request, in map[string]json.RawMessage) *http.Response {
	refused := func(status int, code, msg string) *http.Response {
		b, _ := json.Marshal(map[string]any{"error": map[string]any{"code": code, "message": msg}})
		return jsonResponse(r, status, b)
	}
	event, args := str(in["name"]), canonical(in["arguments"])
	call := callOf(in["call"])
	cursor := str(in["cursor"])
	var hb float64
	json.Unmarshal(in["heartbeat_ms"], &hb)
	grant := min(45000, max(10000, hb))
	if hb == 0 {
		grant = 30000
	}
	f.mu.Lock()
	var v *voucher
	if token := str(in["voucher"]); token != "" {
		v = f.vouchers[token]
		if v == nil || v.event != event || v.args != args || (v.user != "" && v.user != call.User) ||
			(v.used && (cursor == "" || f.pushSubs[subOfCursor(cursor)] == nil || f.pushSubs[subOfCursor(cursor)].id != v.subID)) {
			f.mu.Unlock()
			return refused(403, "bad_voucher", badVoucher)
		}
	} else if f.toolsOnly[event] {
		f.mu.Unlock()
		return refused(403, "tools_only", event+" can be subscribed only through this app's tools")
	}
	sub := f.pushSubs[subOfCursor(cursor)]
	created, truncated := false, false
	if sub == nil {
		// A null cursor (or one whose subscription is gone): a new subscription.
		truncated, created = cursor != "", true
		sub = &pushSub{id: f.next("sub_test_"), event: event, args: args, user: call.User}
		via, tool := "native", ""
		if v != nil {
			v.used, v.subID = true, sub.id
			sub.user, via, tool = v.user, "tool", v.tool
		}
		f.pushSubs[sub.id] = sub
		f.subscribes = append(f.subscribes, Subscription{Event: event, Args: objectOf([]byte(args)), Tool: tool, User: sub.user, Via: via,
			Once: v != nil && v.once, Created: true})
	}
	s := &pushStream{sub: sub, out: make(chan []byte, 64), done: make(chan struct{})}
	f.streams = append(f.streams, s)
	active := map[string]any{"type": "active", "cursor": s.cursor(), "truncated": truncated, "heartbeat_ms": grant, "created": created}
	if created && v != nil {
		active["via"], active["tool"] = "tool:"+v.tool, v.tool
	}
	f.mu.Unlock()
	s.send(active)

	pr, pw := io.Pipe()
	ctx := r.Context()
	go func() {
		defer pw.Close()
		defer f.dropStream(s)
		tick := time.NewTicker(time.Duration(grant) * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				s.end()
				return
			case <-tick.C:
				go s.send(map[string]any{"type": "heartbeat", "cursor": s.cursor()})
			case b := <-s.out:
				if _, err := pw.Write(b); err != nil {
					s.end()
					return
				}
			case <-s.done:
				// Flush what's queued (a close frame), then end.
				for {
					select {
					case b := <-s.out:
						pw.Write(b)
					default:
						return
					}
				}
			}
		}
	}()
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/x-ndjson"}}, Body: pr, Request: r, ContentLength: -1}
}

// subOfCursor is the subscription a fake cursor names ("" for none).
func subOfCursor(cursor string) string {
	parts := strings.Split(cursor, ".")
	if len(parts) != 4 || parts[0] != "c2" || parts[1] != "test" {
		return ""
	}
	return parts[2]
}

func (f *fake) dropStream(s *pushStream) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams = slices.DeleteFunc(f.streams, func(x *pushStream) bool { return x == s })
}

// deliver sends an emit to the push streams it matches (f.mu held: the
// frames go out after it's released).
func (f *fake) deliverLocked(event, args string, users []string, occurrence map[string]any) func() {
	var sends []func()
	for _, s := range f.streams {
		if s.sub.event != event || (len(users) > 0 && !slices.Contains(users, s.sub.user)) || (len(users) == 0 && s.sub.args != args) {
			continue
		}
		s.sub.seq.Add(1)
		o := map[string]any{}
		for k, v := range occurrence {
			o[k] = v
		}
		o["cursor"] = s.cursor()
		o["eventId"] = f.next("evt_test_")
		st := s
		sends = append(sends, func() { st.send(map[string]any{"type": "event", "occurrence": o}) })
	}
	return func() {
		for _, send := range sends {
			send()
		}
	}
}

// endStream answers /events/stream/end (an explicit cancel over stdio),
// f.mu held: the cursor's subscription ends, and its streams close.
func (f *fake) endStream(in map[string]json.RawMessage) (int, any) {
	id := subOfCursor(str(in["cursor"]))
	sub := f.pushSubs[id]
	if sub == nil {
		return 200, map[string]any{"ended": false}
	}
	delete(f.pushSubs, id)
	for _, s := range f.streams {
		if s.sub == sub {
			s.end()
		}
	}
	f.unsubscribes = append(f.unsubscribes, Unsubscribe{Event: sub.event, Args: objectOf([]byte(sub.args)), Ended: true})
	return 200, map[string]any{"ended": true}
}

func jsonResponse(r *http.Request, status int, b []byte) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(b))),
		Request: r, ContentLength: int64(len(b))}
}
