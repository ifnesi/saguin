package config

import (
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/ifnesi/saguin/internal/topic"
)

var (
	errAlternatives = errors.New("expected a whole {a,b} level with at least two nonempty, distinct written alternatives")
	errChannelLevel = errors.New("expected a written, nonempty first level not starting with '$', and no __dlq level")
)

func checkChannelLevel(level string, first bool) error {
	if level == "__dlq" || (first && (level == "" || strings.HasPrefix(level, "$") || strings.ContainsAny(level, "+#"))) {
		return errChannelLevel
	}
	return nil
}

// Routing is the resolved channel policy shared by publication admission and
// the offline route command. Channel indexes are stable within this policy;
// -1 denotes $broadcast. The trie makes topic lookup independent of unrelated
// channel filters.
type Routing struct {
	Channels []Channel
	Table    []Route
	root     topic.Tree
	regions  []topic.Filter
	named    map[string]int
}

// Route is one expanded filter in resolution order.
type Route struct {
	Filter  string
	Channel int
}

// NewRouting compiles configured filters and the queues' derived dead-letter
// filters. It changes neither the configuration nor stored channel identities.
func NewRouting(channels []Channel) *Routing {
	r := &Routing{Channels: slices.Clone(channels)}
	for i := range channels {
		ch := &channels[i]
		if ch.Type != Queue {
			continue
		}
		filter := "__dlq/" + ch.Filter
		r.Channels = append(r.Channels, Channel{Name: ch.Name + "__dlq", Type: Append,
			Filter: filter, Filters: []string{filter}, RetentionPeriod: ch.DLQRetentionPeriod,
			RetentionBytes: ch.DLQRetentionBytes})
	}
	r.named = make(map[string]int, len(r.Channels))
	for i := range r.Channels {
		ch := &r.Channels[i]
		r.named[ch.Name] = i
		for _, filter := range ch.Filters {
			r.Table = append(r.Table, Route{Filter: filter, Channel: i})
			r.root.Add(filter, i+1)
		}
	}
	slices.SortStableFunc(r.Table, func(a, b Route) int { return compareFilters(a.Filter, b.Filter) })
	for _, row := range r.Table {
		r.regions = append(r.regions, topic.Filter{Pattern: row.Filter, Value: row.Channel + 1})
	}
	return r
}

func compareFilters(a, b string) int {
	for {
		al, ar, am := strings.Cut(a, "/")
		bl, br, bm := strings.Cut(b, "/")
		rank := func(level string) int {
			switch level {
			case "+":
				return 1
			case "#":
				return 2
			}
			return 0
		}
		if d := rank(al) - rank(bl); d != 0 {
			return d
		}
		if d := strings.Compare(al, bl); d != 0 {
			return d
		}
		if !am || !bm {
			return len(ar) - len(br)
		}
		a, b = ar, br
	}
}

// Resolve returns the channel whose filter spells name most exactly, or -1
// for $broadcast. The name has already passed MQTT topic validation.
func (r *Routing) Resolve(name string) int { return r.root.Match(name) - 1 }

// Named returns a configured or derived channel's stable routing index.
func (r *Routing) Named(name string) (int, bool) {
	if r == nil {
		return -1, false
	}
	i, ok := r.named[name]
	return i, ok
}

// Reaches returns each channel owning a topic matched by filter. Routing and
// access coverage share topic.WalkFilter's partition of wildcard levels.
func (r *Routing) Reaches(filter string) []int {
	seen := make([]bool, len(r.Channels)+1)
	topic.WalkFilter(filter, r.regions, func(matches []int) bool {
		winner := 0
		if len(matches) != 0 {
			winner = matches[0]
		}
		seen[winner] = true
		return true
	})
	var reached []int
	for i, yes := range seen {
		if yes {
			reached = append(reached, i-1)
		}
	}
	return reached
}

// AccessRoutes exposes immutable routing regions for access-file compilation.
// Values index Channels from one; zero denotes an uncovered broadcast region.
func (r *Routing) AccessRoutes() []topic.Filter { return r.regions }

// filterAlternatives checks the shared braces grammar before expansion.
// A comma is ordinary unless the whole level is an alternatives expression.
func filterAlternatives(level string) ([]string, error) {
	if !strings.ContainsAny(level, "{}") {
		return []string{level}, nil
	}
	if !strings.HasPrefix(level, "{") || !strings.HasSuffix(level, "}") {
		return nil, errAlternatives
	}
	choices := strings.Split(level[1:len(level)-1], ",")
	if len(choices) < 2 {
		return nil, errAlternatives
	}
	seen := make(map[string]bool, len(choices))
	for _, choice := range choices {
		if choice == "" || strings.ContainsAny(choice, "+#{}") {
			return nil, errAlternatives
		}
		if seen[choice] {
			return nil, errAlternatives
		}
		seen[choice] = true
	}
	return choices, nil
}

// expandChannelFilter adds only the channel-specific level and queue rules
// to the grammar and bounds shared with bridge filters.
func expandChannelFilter(filter string, queue bool, limits *Limits) ([]string, error) {
	return expandFilter(filter, limits, func(level string, index int, alternative bool) error {
		if queue && alternative {
			return errors.New("alternatives are not allowed on a queue")
		}
		return checkChannelLevel(level, index == 0)
	})
}

// expandFilter validates every level before building the Cartesian
// product. The longest possible expansion bounds all of them, so an invalid
// length or depth cannot first allocate a large set of expanded strings.
// checkLevel, when supplied, adds the caller's restrictions to each choice.
func expandFilter(filter string, limits *Limits, checkLevel func(string, int, bool) error) ([]string, error) {
	if filter == "" || filter == "none" || !utf8.ValidString(filter) || strings.ContainsRune(filter, '\x00') {
		return nil, topic.ErrMalformed
	}
	levels := strings.Split(filter, "/")
	if len(levels) > limits.MaxTopicLevels {
		return nil, topic.ErrOverLimit
	}
	choices := make([][]string, len(levels))
	maxLength := len(levels) - 1
	hasAlternatives := false
	for i, level := range levels {
		alternatives, err := filterAlternatives(level)
		if err != nil {
			return nil, err
		}
		hasAlternatives = hasAlternatives || len(alternatives) > 1
		longest := 0
		for _, alternative := range alternatives {
			if checkLevel != nil {
				if err := checkLevel(alternative, i, len(alternatives) > 1); err != nil {
					return nil, err
				}
			}
			// topic checks wildcard placement within each nonempty level.
			if alternative != "" {
				if err := topic.CheckFilter(alternative, topic.Limits{MaxLength: limits.MaxTopicLength, MaxLevels: 1}); err != nil {
					return nil, err
				}
			}
			if alternative == "#" && i != len(levels)-1 {
				return nil, topic.ErrMalformed
			}
			longest = max(longest, len(alternative))
		}
		maxLength += longest
		if maxLength > limits.MaxTopicLength {
			return nil, topic.ErrOverLimit
		}
		choices[i] = alternatives
	}
	if !hasAlternatives {
		return []string{filter}, nil
	}
	var expanded []string
	var expand func(int, []byte)
	expand = func(level int, prefix []byte) {
		if level == len(choices) {
			expanded = append(expanded, string(prefix))
			return
		}
		if level != 0 {
			prefix = append(prefix, '/')
		}
		for _, choice := range choices[level] {
			expand(level+1, append(prefix, choice...))
		}
	}
	expand(0, make([]byte, 0, maxLength))
	return expanded, nil
}

// SeekChannel recognizes an exact request or reply topic for a seekable channel.
// Reserved names never fall through to ordinary publication routing or ACLs.
func (r *Routing) SeekChannel(name string, reply bool) (int, bool) {
	const prefix = "$saguin/consumer/"
	suffix := "/seek"
	if reply {
		suffix += "/reply"
	}
	if r == nil || len(name) <= len(prefix)+len(suffix) || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	channel := name[len(prefix) : len(name)-len(suffix)]
	i, ok := r.named[channel]
	return i, ok && (r.Channels[i].Type == Append || r.Channels[i].Type == Queue)
}

// QueueResponseChannel recognizes the queue's exact response topic.
func (r *Routing) QueueResponseChannel(name string) (int, bool) {
	worker, ok := strings.CutSuffix(name, "/response")
	if !ok {
		return 0, false
	}
	return r.WorkerChannel(worker)
}

// WorkerChannel recognizes the other subscribable service topic. Keep its
// existing admission rules separate from the seek reply's unconditional grant.
func (r *Routing) WorkerChannel(filter string) (int, bool) {
	if r == nil {
		return 0, false
	}
	name, ok := strings.CutPrefix(filter, "$saguin/queue/")
	if !ok {
		return 0, false
	}
	i, found := r.named[name]
	return i, found && r.Channels[i].Type == Queue
}
