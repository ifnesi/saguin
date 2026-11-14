// Package views defines the bounded, payload-free operations handovers.
// Values belong to the core, then a store reader where required, and finally
// the HTTP caller. No view retains a mutable broker owner.
package views

import (
	"cmp"
	"encoding/json"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Limit is the maximum number of rows in a state list.
const Limit = 100

// Request carries a private buffered reply and the caller's abandonment signal.
type Request struct {
	Route   int
	Channel string
	Done    <-chan struct{}
	Reply   chan Result
}

// Result transfers an answer, or an HTTP error status, to its caller.
type Result struct {
	Body any
	Code int
}

// Abandoned reports whether the connection or its five-second wait has ended.
func (r *Request) Abandoned() bool {
	select {
	case <-r.Done:
		return true
	default:
		return false
	}
}

// Counts accompanies every state list, including nested position lists.
type Counts struct {
	Total    int `json:"total"`
	Returned int `json:"returned"`
}

// Include retains only the first Limit rows under compare. Sorting always
// precedes display truncation. The standard slice operations keep memory
// bounded even when the store streams arbitrarily many away sessions.
func Include[T any](rows []T, row T, compare func(T, T) int) []T {
	i, _ := slices.BinarySearchFunc(rows, row, compare)
	if i >= Limit {
		return rows
	}
	if len(rows) == Limit {
		rows = rows[:Limit-1]
	}
	return slices.Insert(rows, i, row)
}

// Text keeps the longest complete UTF-8 prefix within the display limit.
func Text(s string) string {
	if len(s) <= 1024 {
		return s
	}
	i := 1024
	for !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i]
}

// Time formats a nonzero event time as RFC 3339 UTC, to the second.
func Time(at time.Time) *string {
	if at.IsZero() {
		return nil
	}
	s := at.UTC().Format(time.RFC3339)
	return &s
}

// Millis formats a stored Unix-millisecond time, or null when never observed.
func Millis(at int64) *string {
	if at == 0 {
		return nil
	}
	return Time(time.UnixMilli(at))
}

// Partition describes the declared partition set of one subscription.
type Partition struct {
	Filter  string   `json:"filter"`
	Count   uint32   `json:"count"`
	Indices []uint32 `json:"indices"`
}

// Session contains only the public session metadata, never its exchanges or Will.
type Session struct {
	ID            int64       `json:"-"`
	ClientID      string      `json:"client_id"`
	Connected     bool        `json:"connected"`
	User          string      `json:"user"`
	Protocol      string      `json:"protocol"`
	Listener      string      `json:"listener"`
	Remote        string      `json:"remote"`
	Keepalive     uint16      `json:"keepalive"`
	ExpiresAfter  uint32      `json:"expires_after"`
	CleanStart    bool        `json:"clean_start"`
	Subscriptions int         `json:"subscriptions"`
	Partitions    []Partition `json:"partitions,omitempty"`
}

// CompareSessions places away sessions first, then compares complete client ids.
func CompareSessions(a, b *Session) int {
	if a.Connected != b.Connected {
		if a.Connected {
			return 1
		}
		return -1
	}
	return strings.Compare(a.ClientID, b.ClientID)
}

// MarshalJSON cuts display identities only after the complete-key selection.
func (s *Session) MarshalJSON() ([]byte, error) {
	type plain Session
	display := plain(*s)
	display.ClientID, display.User = Text(s.ClientID), Text(s.User)
	return json.Marshal(display)
}

// Sessions counts all sessions, including those beyond its bounded rows.
type Sessions struct {
	Counts
	Connected int        `json:"connected"`
	Away      int        `json:"away"`
	Sessions  []*Session `json:"sessions"`
}

// Rule identifies a bridge reader's complete configured rule.
type Rule struct {
	Filter string `json:"filter"`
	Topic  string `json:"topic"`
}

// ReaderKey keeps a position's complete identity without constructing its
// display prefix or allocating a Rule while selecting bounded live owners.
type ReaderKey struct {
	Name, Filter, Topic string
	Bridge              bool
}

// CompareReaders follows the byte order of bridge: and mqtt:, then the full
// name and rule. Both stored rows and live selection use the same row key.
func CompareReaders(a, b ReaderKey) int {
	if a.Bridge != b.Bridge {
		if a.Bridge {
			return -1
		}
		return 1
	}
	if order := strings.Compare(a.Name, b.Name); order != 0 {
		return order
	}
	if order := strings.Compare(a.Filter, b.Filter); order != 0 {
		return order
	}
	return strings.Compare(a.Topic, b.Topic)
}

// Position is one session or bridge rule's progress on a channel.
type Position struct {
	Reader   string  `json:"reader"`
	Rule     *Rule   `json:"rule"`
	Offset   int64   `json:"offset"`
	Behind   int64   `json:"behind"`
	LastSeen *string `json:"last_seen"`
}

// ComparePositions orders lag first, then reader, rule filter and rule topic.
func ComparePositions(a, b Position) int {
	if order := cmp.Compare(b.Behind, a.Behind); order != 0 {
		return order
	}
	return CompareReaders(a.readerKey(), b.readerKey())
}

func (p Position) readerKey() ReaderKey {
	if p.Rule != nil {
		return ReaderKey{Name: strings.TrimPrefix(p.Reader, "bridge:"), Bridge: true,
			Filter: p.Rule.Filter, Topic: p.Rule.Topic}
	}
	return ReaderKey{Name: strings.TrimPrefix(p.Reader, "mqtt:")}
}

// MarshalJSON preserves the reader prefix while cutting its client identity.
func (p Position) MarshalJSON() ([]byte, error) {
	type plain Position
	if id, ok := strings.CutPrefix(p.Reader, "mqtt:"); ok {
		p.Reader = "mqtt:" + Text(id)
	}
	return json.Marshal(plain(p))
}

// Channel lists a channel's furthest-behind positions.
type Channel struct {
	Counts
	ID         int64      `json:"-"`
	Channel    string     `json:"channel"`
	NextOffset int64      `json:"next_offset"`
	Consumers  int        `json:"consumers"`
	Positions  []Position `json:"positions"`
}

// Add counts a position even when it falls beyond the returned rows.
func (c *Channel) Add(p Position) {
	p.Behind = c.NextOffset - p.Offset
	c.Consumers++
	c.Total = c.Consumers
	c.Positions = Include(c.Positions, p, ComparePositions)
	c.Returned = len(c.Positions)
}

// Consumers lists append and queue channels by name.
type Consumers struct {
	Counts
	Channels []Channel `json:"channels"`
}

// Job is an unfinished queue record without properties or payload.
type Job struct {
	Offset       int64   `json:"offset"`
	Topic        string  `json:"topic"`
	Attempts     int64   `json:"attempts"`
	State        string  `json:"state"`
	Holder       *string `json:"holder,omitempty"`
	HeldUntil    *string `json:"held_until,omitempty"`
	FirstAttempt *string `json:"first_attempt,omitempty"`
	LastAttempt  *string `json:"last_attempt,omitempty"`
}

// Queue lists the oldest unfinished jobs.
type Queue struct {
	Counts
	Records []Job `json:"records"`
}

// Bridge describes a configured link and its subscription progress.
type Bridge struct {
	Name                 string  `json:"name"`
	Peer                 string  `json:"peer"`
	Connected            bool    `json:"connected"`
	Since                *string `json:"since"`
	SubscriptionsGranted int     `json:"subscriptions_granted"`
	SubscriptionsPending int     `json:"subscriptions_pending"`
	UnansweredSubscribes uint64  `json:"unanswered_subscribes"`
	LastSuback           *string `json:"last_suback"`
	Sent                 uint64  `json:"sent"`
	Received             uint64  `json:"received"`
}

// Bridges lists links in their operational inspection order.
type Bridges struct {
	Counts
	Bridges []Bridge `json:"bridges"`
}
