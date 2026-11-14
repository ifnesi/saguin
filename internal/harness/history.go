package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// The operations histories as an operator reads them (design.md,
// Operations, HTTP: refused and position-lost), shared by the conformance
// tests and the end-to-end fuzzer.

// History is one answer of a history route: its rows, in the order given,
// and its tracked, beyond and returned.
type History[R any] struct {
	Rows                      []R
	Tracked, Beyond, Returned int64
}

// RefusedRow is one row of the refused history.
type RefusedRow struct {
	ClientID string  `json:"client_id"`
	User     *string `json:"user"`
	Reason   string  `json:"reason"`
	Count    int64   `json:"count"`
	LastSeen string  `json:"last_seen"`
}

// RefusedFields are the fields design.md gives every refused row: "Each
// row has client_id (empty before a CONNECT), user, reason, count and
// last_seen".
var RefusedFields = []string{"client_id", "user", "reason", "count", "last_seen"}

// LossRow is one row of the position-lost history.
type LossRow struct {
	Reader        string `json:"reader"`
	Channel       string `json:"channel"`
	Kind          string `json:"kind"`
	Count         int64  `json:"count"`
	RecordsMissed *int64 `json:"records_missed"` // nil for null: unknown
	LastPosition  int64  `json:"last_position"`
	LastFloor     int64  `json:"last_floor"`
	LastSeen      string `json:"last_seen"`
}

// LossFields are the fields design.md gives every position-lost row: "Each
// row has reader, channel, kind, count, records_missed, last_position,
// last_floor and last_seen".
var LossFields = []string{"reader", "channel", "kind", "count", "records_missed", "last_position", "last_floor", "last_seen"}

// Missed is r's records_missed as text, "null" for unknown.
func (r *LossRow) Missed() string {
	if r.RecordsMissed == nil {
		return "null"
	}
	return strconv.FormatInt(*r.RecordsMissed, 10)
}

func (r LossRow) String() string {
	return fmt.Sprintf("{%s %s %s count %d missed %s position %d floor %d at %s}",
		r.Reader, r.Channel, r.Kind, r.Count, r.Missed(), r.LastPosition, r.LastFloor, r.LastSeen)
}

// DecodeHistory reads body, a history route's answer, refusing it unless it
// is a JSON object holding counts tracked, beyond and returned beside the
// list named list, of rows each carrying every one of fields, and returned
// and tracked are both the number of rows given. design.md, Operations,
// HTTP: "Each answer under /v1/operations/ is an object"; "Every list is
// sorted and comes with total and returned"; "refused lists, under
// clients, [...]"; "position-lost lists, under readers, [...]"; of both
// histories, "Each returns every row it keeps. They include tracked, the
// number of rows kept, and beyond."
func DecodeHistory[R any](body []byte, list string, fields []string) (History[R], error) {
	var h History[R]
	var members map[string]json.RawMessage
	if err := json.Unmarshal(body, &members); err != nil {
		return h, fmt.Errorf("%.200q: %w; want a JSON object", body, err)
	}
	for name, count := range map[string]*int64{"tracked": &h.Tracked, "beyond": &h.Beyond, "returned": &h.Returned} {
		if err := json.Unmarshal(members[name], count); err != nil {
			return h, fmt.Errorf("%s is %s (%w); want a count", name, members[name], err)
		}
	}
	rows, ok := members[list]
	if !ok || !bytes.HasPrefix(bytes.TrimSpace(rows), []byte("[")) {
		return h, fmt.Errorf("%s is %.200q (present %v); want the list of rows", list, rows, ok)
	}
	var objects []map[string]json.RawMessage
	if err := json.Unmarshal(rows, &objects); err != nil {
		return h, fmt.Errorf("%s: %w; want a list of objects", list, err)
	}
	for i, o := range objects {
		for _, field := range fields {
			if _, ok := o[field]; !ok {
				return h, fmt.Errorf("%s: row %d %v has no %s; want each of %v", list, i, o, field, fields)
			}
		}
	}
	if err := json.Unmarshal(rows, &h.Rows); err != nil {
		return h, fmt.Errorf("%s: %w", list, err)
	}
	if h.Returned != int64(len(h.Rows)) || h.Tracked != int64(len(h.Rows)) {
		return h, fmt.Errorf("returned %d, tracked %d, beside %d rows in %s; want every row kept returned, and both counting them", h.Returned, h.Tracked, len(h.Rows), list)
	}
	return h, nil
}
