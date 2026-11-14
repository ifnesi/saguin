package main

import (
	"cmp"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The run's checks of the operations histories, refused and position-lost
// (design.md, Operations, HTTP; Words: Missed). At each checkpoint the run
// reads both over the plain operations listener, as root, and checks them
// against the design and against the broker's own counts on /metrics:
// rows kept and sorted as the design says, the refused list's counts plus
// beyond agreeing with the refusal and closure series, and no channel's
// counted losses above its saguin_missed_total.

// historyLimit is how many rows a history keeps (design.md, Operations,
// HTTP: "Both histories keep up to 1,000 rows in memory").
const historyLimit = 1000

// readHistory GETs the history route of the run's broker, through o, as
// root, and decodes it, its rows under list (harness.DecodeHistory).
func readHistory[R any](o *operations, route, list string, fields []string) (harness.History[R], error) {
	path := "/v1/operations/" + route
	root := operators[0]
	a, err := o.targets[opsPlain].Do(&harness.OpsCall{Method: http.MethodGet, Path: path, User: root.user, Password: root.password})
	if err != nil {
		return harness.History[R]{}, fmt.Errorf("GET %s: %w", path, err)
	}
	if a.Status != http.StatusOK || a.MediaType != "application/json" {
		return harness.History[R]{}, fmt.Errorf("GET %s: %d %s %.200q; want 200 and a JSON object", path, a.Status, a.MediaType, a.Body)
	}
	h, err := harness.DecodeHistory[R]([]byte(a.Body), list, fields)
	if err != nil {
		return h, fmt.Errorf("GET %s: %w", path, err)
	}
	return h, nil
}

// checkHistories checks both histories of a broker the run started; a
// broker given with -addr has no operations listener to read them from.
func (f *fuzzer) checkHistories() error {
	if f.ops == nil || f.scrape == nil {
		return nil
	}
	if err := f.checkRefused(); err != nil {
		return err
	}
	return f.checkLosses()
}

// unrefused are the closure reasons the refused history leaves out:
// metrics.md, saguin_disconnects_total, "client (the client's DISCONNECT),
// connection_lost"; design.md, Operations, HTTP: refused "lists
// connections refused at CONNECT or ended by the broker with a reason".
var unrefused = []string{"client", "connection_lost"}

// refusal reports whether s is a sample of a series the refused history
// records: saguin_connections_refused_total, or saguin_disconnects_total
// but its unrefused reasons.
func refusal(s testkit.Scraped) bool {
	return (s.Name == "saguin_connections_refused_total") || (s.Name == disconnectsTotal && !slices.Contains(unrefused, s.Labels["reason"]))
}

// checkRefused checks the refused history: one row per client id, most
// refused first, equal counts by client id in UTF-8 byte order, each
// reason one the refusal or closure series name, at most 1,000 rows, every
// one returned (design.md, Operations, HTTP: "with one row per client id";
// "Each returns every row it keeps";
// "The reason is the latest, named as in metrics.md. The most refused come
// first."; "Equal sort ranks use the complete row key in UTF-8 byte
// order"). And once quiet, saguin_connections_refused_total plus
// saguin_disconnects_total but client and connection_lost equals the rows'
// counts plus beyond (0020i, from Fable's sweep of metrics.md; metrics.md,
// saguin_connections_refused_total: "The refused history records that same
// closure event"): the series move on the goroutine that sees each event,
// so the run waits up to its timeout for the two to meet.
func (f *fuzzer) checkRefused() error {
	var h harness.History[harness.RefusedRow]
	var e testkit.Exposition
	var series, rows float64
	var err error
	agreed := testkit.Within(f.timeout, func() bool {
		if h, err = readHistory[harness.RefusedRow](f.ops, "refused", "clients", harness.RefusedFields); err != nil {
			return true
		}
		if e, err = f.scrape(); err != nil {
			return true
		}
		series, rows = 0, float64(h.Beyond)
		for _, s := range e.Samples {
			if refusal(s) {
				series += s.Value
			}
		}
		for _, r := range h.Rows {
			rows += float64(r.Count)
		}
		return series == rows
	})
	switch {
	case err != nil:
		return err
	case !agreed:
		return fmt.Errorf("the refused and closure series, but client and connection_lost, sum to %v; the refused history's counts plus beyond to %v; want them equal", series, rows)
	}
	reasons := map[string]bool{}
	for _, s := range e.Samples {
		if refusal(s) {
			reasons[s.Labels["reason"]] = true
		}
	}
	if err := checkKept(h.Tracked, h.Beyond); err != nil {
		return fmt.Errorf("refused: %w", err)
	}
	for i, r := range h.Rows {
		switch {
		case !reasons[r.Reason]:
			return fmt.Errorf("refused: row %q has reason %q; want one of the refusal and closure series' reasons but %v", r.ClientID, r.Reason, unrefused)
		case r.Count < 1:
			return fmt.Errorf("refused: row %q counted %d; want at least 1", r.ClientID, r.Count)
		case i == 0:
		case (h.Rows[i-1].Count < r.Count) || (h.Rows[i-1].Count == r.Count && h.Rows[i-1].ClientID >= r.ClientID):
			return fmt.Errorf("refused: row %q, counted %d, after %q, counted %d; want the most refused first, then by client id, one row each",
				r.ClientID, r.Count, h.Rows[i-1].ClientID, h.Rows[i-1].Count)
		}
		if _, err := harness.UTCSecond(r.LastSeen); err != nil {
			return fmt.Errorf("refused: row %q: last_seen %w", r.ClientID, err)
		}
	}
	f.note("refused history: %d rows, %d beyond", len(h.Rows), h.Beyond)
	return nil
}

// lossOrder is the position-lost history's order (design.md, Operations,
// HTTP: "Counted rows sort by decreasing missed count. Unknown rows
// follow, by decreasing count."; "Equal sort ranks use the complete row key
// in UTF-8 byte order"; "A position-loss key is reader and channel").
func lossOrder(a, b *harness.LossRow) int {
	switch {
	case (a.RecordsMissed == nil) != (b.RecordsMissed == nil):
		if a.RecordsMissed == nil {
			return 1
		}
		return -1
	case a.RecordsMissed != nil && *a.RecordsMissed != *b.RecordsMissed:
		return cmp.Compare(*b.RecordsMissed, *a.RecordsMissed)
	case a.RecordsMissed == nil && a.Count != b.Count:
		return cmp.Compare(b.Count, a.Count)
	}
	return cmp.Or(strings.Compare(a.Reader, b.Reader), strings.Compare(a.Channel, b.Channel))
}

// checkLosses checks the position-lost history: one row per reader and
// channel, in lossOrder, at most 1,000; each reader
// mqtt:<client id> or bridge:<name>, a bridge's row of kind bridge and
// another's consumer or session, a session row's records_missed unknown
// (design.md, Operations, HTTP: "The kind is consumer when passed while its
// client was connected, session when passed while away, or bridge for an
// out rule. The kind is the latest observation's."; "null once any
// observation represented by that row has an unknown count"; Words:
// Missed, an away session's return "adds no reconstructed loss count"). And
// no channel's counted losses exceed its saguin_missed_total, cause
// retention, read after the history: the rows hold exact counts of
// connected consumers and bridges alone, and the counter "the channel's
// total from each deletion's answer, away sessions included" (metrics.md).
func (f *fuzzer) checkLosses() error {
	h, err := readHistory[harness.LossRow](f.ops, "position-lost", "readers", harness.LossFields)
	if err != nil {
		return err
	}
	e, err := f.scrape()
	if err != nil {
		return err
	}
	if err := checkKept(h.Tracked, h.Beyond); err != nil {
		return fmt.Errorf("position-lost: %w", err)
	}
	counted := map[string]int64{}
	for i := range h.Rows {
		r := &h.Rows[i]
		bridge := strings.HasPrefix(r.Reader, "bridge:")
		switch {
		case !bridge && !strings.HasPrefix(r.Reader, "mqtt:"):
			return fmt.Errorf("position-lost: %v; want a reader mqtt:<client id> or bridge:<name>", r)
		case bridge != (r.Kind == "bridge"):
			return fmt.Errorf("position-lost: %v; want kind bridge for a bridge's rule alone", r)
		case !slices.Contains([]string{"consumer", "session", "bridge"}, r.Kind):
			return fmt.Errorf("position-lost: %v; want kind consumer, session or bridge", r)
		case (r.Kind == "session") && (r.RecordsMissed != nil):
			return fmt.Errorf("position-lost: %v; want records_missed null after a session's observation, its count unknown", r)
		case (r.Count < 1) || (r.RecordsMissed != nil && *r.RecordsMissed < 0):
			return fmt.Errorf("position-lost: %v; want counted at least once, missed unknown or at least 0", r)
		case i > 0 && lossOrder(&h.Rows[i-1], r) >= 0:
			return fmt.Errorf("position-lost: %v after %v; want counted rows by decreasing missed, then unknown rows by decreasing count, then by key, one row each", r, h.Rows[i-1])
		}
		if _, err := harness.UTCSecond(r.LastSeen); err != nil {
			return fmt.Errorf("position-lost: %v: last_seen %w", r, err)
		}
		if r.RecordsMissed != nil {
			counted[r.Channel] += *r.RecordsMissed
		}
	}
	for _, channel := range slices.Sorted(maps.Keys(counted)) {
		key := testkit.SeriesKey("saguin_missed_total", "channel", channel, "cause", "retention")
		total, ok := e.Values[key]
		if !ok || float64(counted[channel]) > total {
			return fmt.Errorf("position-lost: the rows on %s count %d records missed; %s reads %v (present %v); want no more than it counts",
				channel, counted[channel], key, total, ok)
		}
	}
	f.note("position-lost history: %d rows, %d beyond", len(h.Rows), h.Beyond)
	return nil
}

// checkKept checks a history's counts, every row kept being returned and
// counted by tracked (harness.DecodeHistory): at most historyLimit rows, and
// beyond not negative (design.md, Operations, HTTP: "Both histories keep up
// to 1,000 rows in memory"; "They include tracked, the number of rows kept,
// and beyond").
func checkKept(tracked, beyond int64) error {
	if tracked > historyLimit || beyond < 0 {
		return fmt.Errorf("tracked %d, beyond %d; want at most %d rows kept, beyond at least 0", tracked, beyond, historyLimit)
	}
	return nil
}
