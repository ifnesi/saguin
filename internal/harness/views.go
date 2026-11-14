package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"
)

// The operations state views' answers (design.md, Operations, HTTP), as the
// conformance tests and the end-to-end fuzzer read them.

// The fields that hold each view's rows: "sessions returns sessions, each
// with [...]"; "bridges returns bridges, each with [...]"; "consumers
// returns channels, one entry per append or queue channel, by name"; "It
// returns the queue's unfinished jobs as records, oldest first".
const (
	SessionsRows  = "sessions"
	BridgesRows   = "bridges"
	ConsumersRows = "channels"
	QueueRows     = "records"
)

// StateRows decodes the rows of view, an answer's fields, from its list
// key into rows, and returns the answer's total and returned ("Every list
// is sorted and comes with total and returned"), or an error where view
// has no such list or a field does not decode.
func StateRows(view map[string]json.RawMessage, key string, rows any) (total, returned int, err error) {
	raw, ok := view[key]
	if !ok || !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("[")) {
		return 0, 0, fmt.Errorf("the answer has no list %q; it has %v", key, slices.Sorted(maps.Keys(view)))
	}
	if err := json.Unmarshal(raw, rows); err != nil {
		return 0, 0, fmt.Errorf("the answer's %q, %s: %w", key, raw, err)
	}
	if err := StateField(view, "total", &total); err != nil {
		return 0, 0, err
	}
	if err := StateField(view, "returned", &returned); err != nil {
		return 0, 0, err
	}
	return total, returned, nil
}

// StateField decodes view's field key into into, or returns an error where
// view has no such field or it does not decode.
func StateField(view map[string]json.RawMessage, key string, into any) error {
	raw, ok := view[key]
	if !ok {
		return fmt.Errorf("the answer has no field %q; it has %v", key, slices.Sorted(maps.Keys(view)))
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("the answer's %q, %s: %w", key, raw, err)
	}
	return nil
}

// UTCSecond returns the time s names, or an error unless s is RFC 3339 in
// UTC, to the second: what every time the operations routes answer must be
// (design.md, Operations, HTTP: "Times are RFC 3339 in UTC, to the second").
func UTCSecond(s string) (time.Time, error) {
	at, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return at, fmt.Errorf("%q: %w; want RFC 3339", s, err)
	}
	if want := at.UTC().Format(time.RFC3339); s != want {
		return at, fmt.Errorf("%q; want RFC 3339 in UTC, to the second (%s)", s, want)
	}
	return at, nil
}

// ValuesOf returns every value a key named key holds anywhere in v, a
// decoded JSON value, objects and lists searched through in turn: what a
// configuration holds wherever the answer nests it.
func ValuesOf(v any, key string) []any {
	var found []any
	switch v := v.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(v)) {
			if k == key {
				found = append(found, v[k])
			}
			found = append(found, ValuesOf(v[k], key)...)
		}
	case []any:
		for _, e := range v {
			found = append(found, ValuesOf(e, key)...)
		}
	}
	return found
}

// StateSession is one row of sessions: "sessions returns sessions, each
// with client_id, connected, user, protocol (3.1.1 or 5), listener, remote,
// keepalive and expires_after in seconds, clean_start, and subscriptions".
type StateSession struct {
	ClientID      string  `json:"client_id"`
	Connected     bool    `json:"connected"`
	Protocol      string  `json:"protocol"`
	Listener      *string `json:"listener"`
	Keepalive     *int64  `json:"keepalive"`
	ExpiresAfter  int64   `json:"expires_after"`
	CleanStart    bool    `json:"clean_start"`
	Subscriptions int     `json:"subscriptions"`
}

// StateConsumers is one entry of consumers, and StatePosition one of its
// positions: "Each entry has channel, next_offset, consumers and
// positions"; "Each position has reader (mqtt:<client id> or
// bridge:<name>), rule, offset, behind and last_seen".
type StateConsumers struct {
	Channel    string          `json:"channel"`
	NextOffset int64           `json:"next_offset"`
	Consumers  int             `json:"consumers"`
	Positions  []StatePosition `json:"positions"`
}

// StatePosition is one position of a StateConsumers.
type StatePosition struct {
	Reader string          `json:"reader"`
	Rule   json.RawMessage `json:"rule"`
	Offset int64           `json:"offset"`
	Behind int64           `json:"behind"`
}

// StateJob is one row of queues/<channel>: "Each has offset, topic,
// attempts and state (ready, backoff, offered or held). Where applicable,
// it also has holder (client id), held_until, first_attempt and
// last_attempt."
type StateJob struct {
	Offset    int64   `json:"offset"`
	Topic     string  `json:"topic"`
	Attempts  int     `json:"attempts"`
	State     string  `json:"state"`
	Holder    *string `json:"holder"`
	HeldUntil *string `json:"held_until"`
}

// JobStates are a job's states: "state (ready, backoff, offered or held)".
var JobStates = []string{"ready", "backoff", "offered", "held"}

// StateBridge is one row of bridges: "bridges returns bridges, each with
// name, peer, connected, since, subscriptions_granted,
// subscriptions_pending, unanswered_subscribes, last_suback, sent and
// received".
type StateBridge struct {
	Name                 string  `json:"name"`
	Peer                 string  `json:"peer"`
	Connected            bool    `json:"connected"`
	Since                *string `json:"since"`
	SubscriptionsGranted int     `json:"subscriptions_granted"`
	SubscriptionsPending int     `json:"subscriptions_pending"`
	UnansweredSubscribes int     `json:"unanswered_subscribes"`
	LastSuback           *string `json:"last_suback"`
	Sent                 int64   `json:"sent"`
	Received             int64   `json:"received"`
}

// PositionsInOrder returns an error unless ps, one channel's positions,
// come furthest behind first, equal ranks by their complete key, and each
// is behind by nextOffset minus its offset: "The behind is next_offset
// minus offset"; "Positions appear furthest behind first, at most 100 per
// channel"; "Equal sort ranks use the complete row key in UTF-8 byte order.
// A consumer's key is reader, rule filter and rule topic." The rule's form
// is not the design's to say, so equal ranks of one reader are left as
// they come.
func PositionsInOrder(ps []StatePosition, nextOffset int64) error {
	if len(ps) > 100 {
		return fmt.Errorf("%d positions; want at most 100", len(ps))
	}
	for i, p := range ps {
		if p.Behind != nextOffset-p.Offset {
			return fmt.Errorf("position %+v behind by %d; want next_offset %d minus its offset", p, p.Behind, nextOffset)
		}
		if i == 0 {
			continue
		}
		if prev := ps[i-1]; prev.Behind < p.Behind || (prev.Behind == p.Behind && prev.Reader > p.Reader) {
			return fmt.Errorf("position %+v after %+v; want furthest behind first, then by reader", p, prev)
		}
	}
	return nil
}
