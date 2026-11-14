package acl

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/ifnesi/saguin/internal/topic"
)

// Verb indexes the independent trees of grants on one subject.
type Verb uint8

// Supported grants. Disconnect is a broker grant rather than a topic grant.
const (
	Write Verb = iota
	Read
	Seek
	Consume
	Disconnect
)

// Feature identifies a capability that any selected role may withhold.
type Feature uint8

// Feature bits are combined across all the selected entry's roles.
const (
	Persistent Feature = 1 << iota
	Will
	Share
	Retained
	QoS2
)

var verbNames = [...]string{"write", "read", "seek", "consume", "disconnect"}

// String returns the spelling used in access files and service descriptions.
func (v Verb) String() string { return verbNames[v] }

var featureNames = [...]string{"persistent", "will", "share", "retained", "qos2"}

// Rule retains the file's spelling and its checked filter expansions. Filters
// belong only to channel and topic rules; broker rules have none.
type Rule struct {
	Role    string
	Kind    string
	Subject string
	Filter  string
	Filters []string
	Verbs   []string
	Denies  []string
	verbs   []Verb
	denied  Feature
	user    bool // the rule's own filter or topic contains %u
	client  bool // the rule's own filter or topic contains %c
}

// Entry is one whole users: entry. Zero limits inherit the broker's values.
type Entry struct {
	Pattern      string
	Roles        []string
	PublishRate  int
	PublishBytes int64
	ClientIDs    []string
	matcher      *regexp.Regexp
	clients      []*regexp.Regexp
	rules        []int
	literals     int
}

// File is immutable policy, built and validated before listeners start.
type File struct {
	Path     string
	rules    []Rule
	routes   []topic.Filter
	channels []string
	entries  []Entry // decreasing precedence; the file's rule order is separate
}

// New compiles checked rules and entries and reports every ambiguous pair.
// The configuration reader validates subjects, verbs, features and references.
func New(path string, rules []Rule, entries []Entry) (*File, error) {
	f := &File{Path: path, rules: rules, entries: entries}
	for i := range f.rules {
		r := &f.rules[i]
		text := r.Subject
		if r.Kind == "channel" {
			text = r.Filter
		}
		r.user, r.client = strings.Contains(text, "%u"), strings.Contains(text, "%c")
		for _, name := range r.Verbs {
			r.verbs = append(r.verbs, Verb(slices.Index(verbNames[:], name)))
		}
		for _, name := range r.Denies {
			r.denied |= 1 << slices.Index(featureNames[:], name)
		}
	}
	var findings []error
	for i := range f.entries {
		e := &f.entries[i]
		var err error
		e.matcher, err = Pattern(e.Pattern)
		if err != nil {
			findings = append(findings, fmt.Errorf("users.%s: %w", e.Pattern, err))
		}
		e.literals = utf8.RuneCountInString(strings.ReplaceAll(e.Pattern, "*", ""))
		roles := make(map[string]bool, len(e.Roles))
		for _, role := range e.Roles {
			roles[role] = true
		}
		for j := range f.rules {
			if roles[f.rules[j].Role] {
				e.rules = append(e.rules, j)
			}
		}
		e.clients = make([]*regexp.Regexp, len(e.ClientIDs))
		for j, pattern := range e.ClientIDs {
			if !strings.Contains(pattern, "%u") {
				e.clients[j], err = Pattern(pattern)
				if err != nil {
					findings = append(findings, fmt.Errorf("users.%s.client_ids: %w", e.Pattern, err))
				}
			}
		}
		for j := range i {
			other := &f.entries[j]
			if e.literals == other.literals && strings.Contains(e.Pattern, "*") && strings.Contains(other.Pattern, "*") && PatternsOverlap(e.Pattern, other.Pattern) {
				findings = append(findings, fmt.Errorf("users.%s and users.%s: equally specific patterns overlap", other.Pattern, e.Pattern))
			}
		}
	}
	if err := errors.Join(findings...); err != nil {
		return nil, err
	}
	slices.SortStableFunc(f.entries, func(a, b Entry) int {
		aExact, bExact := !strings.Contains(a.Pattern, "*"), !strings.Contains(b.Pattern, "*")
		if aExact != bExact {
			if aExact {
				return -1
			}
			return 1
		}
		return b.literals - a.literals
	})
	return f, nil
}

// Select returns the sole entry that supplies this user's whole policy.
func (f *File) Select(user string) *Entry {
	for i := range f.entries {
		if f.entries[i].matcher.MatchString(user) {
			return &f.entries[i]
		}
	}
	return nil
}

// AllowsClient checks an entry's id restrictions. A user containing * cannot
// supply %u to an id pattern. Other entries do not contribute restrictions.
func (e *Entry) AllowsClient(user, id string) bool {
	if e == nil || len(e.ClientIDs) == 0 {
		return true
	}
	for i, pattern := range e.ClientIDs {
		re := e.clients[i]
		if re == nil {
			if strings.Contains(user, "*") {
				continue
			}
			var err error
			re, err = Pattern(strings.ReplaceAll(pattern, "%u", user))
			if err != nil {
				continue // an unrepresentable pattern grants no id
			}
		}
		if re.MatchString(id) {
			return true
		}
	}
	return false
}

// Client holds only this client's compiled grants. The reader builds it before
// handing CONNECT to the core, after which it is immutable for both owners.
// A nil Client means there is no acl_file, with no trees or entries to check.
type Client struct {
	denied       Feature
	grants       map[string][Disconnect]*topic.Tree
	disconnect   bool
	regions      []topic.Filter
	channels     []string
	readSubjects []string
}

// Compile substitutes names once and builds each subject's grant trees using
// the same MQTT trie as channel routing. A topic rule uses the empty subject,
// denoting $broadcast; channel rules remain isolated from topic rules.
func (f *File) Compile(e *Entry, user, id string) *Client {
	c := &Client{channels: f.channels}
	if e == nil {
		return c
	}
	var replacer *strings.Replacer
	for _, i := range e.rules {
		r := &f.rules[i]
		c.denied |= r.denied
		if r.Kind == "broker" {
			c.disconnect = c.disconnect || slices.Contains(r.verbs, Disconnect)
			continue
		}
		if withheld(r, user, id) {
			continue
		}
		subject := r.Subject
		if r.Kind == "topic" {
			subject = ""
		}
		if c.grants == nil {
			c.grants = make(map[string][Disconnect]*topic.Tree)
		}
		trees := c.grants[subject]
		for _, filter := range r.Filters {
			// A bare channel grant uses its configured filters literally.
			// Only text written in the access rule is substituted.
			if r.user || r.client {
				if replacer == nil {
					replacer = newSubstitution(user, id)
				}
				filter = replacer.Replace(filter)
			}
			for _, v := range r.verbs {
				if trees[v] == nil {
					trees[v] = new(topic.Tree)
				}
				trees[v].Add(filter, 1)
				if v == Read {
					if c.regions == nil {
						c.regions = slices.Clone(f.routes)
					}
					c.readSubjects = append(c.readSubjects, subject)
					c.regions = append(c.regions, topic.Filter{Pattern: filter, Value: -len(c.readSubjects)})
				}
			}
		}
		c.grants[subject] = trees
	}
	return c
}

// Denied reports a withheld feature; no acl_file withholds none.
func (c *Client) Denied(feature Feature) bool { return c != nil && c.denied&feature != 0 }

// Allows checks one verb on a routed topic. An empty channel is $broadcast.
// Topic validity belongs to packet admission; this only matches the grants.
// The caller also applies the broker's reserved-topic and channel rules.
func (c *Client) Allows(verb Verb, channel, name string) bool {
	if c == nil {
		return true
	}
	if verb == Disconnect {
		return c.disconnect
	}
	return c.grants[channel][verb].Match(name) != 0
}

func withheld(r *Rule, user, id string) bool {
	return (r.user && strings.ContainsAny(user, "+#/")) ||
		(r.client && strings.ContainsAny(id, "+#/"))
}

// SetRouting attaches immutable channel routing before the file is published.
// The configuration loader supplies filters in resolution order.
func (f *File) SetRouting(routes []topic.Filter, channels []string) {
	f.routes, f.channels = routes, channels
}

// Coverage describes a subscription's read permission. Full is zero so a
// broker without an access file needs neither compilation nor delivery checks.
type Coverage uint8

// Coverage values distinguish a free delivery path, topic checks and a hold.
const (
	Full Coverage = iota
	Partial
	None
)

// ReadCoverage classifies the whole filter under routing and the union of
// read grants. A route owns each region before channel-specific grants apply.
// This runs only at SUBSCRIBE, session recovery and access recompilation.
func (c *Client) ReadCoverage(filter string) Coverage {
	if c == nil {
		return Full
	}
	if len(c.readSubjects) == 0 {
		return None
	}
	allowed, denied := false, false
	topic.WalkFilter(filter, c.regions, func(matches []int) bool {
		channel := ""
		if len(matches) != 0 && matches[0] > 0 {
			channel = c.channels[matches[0]-1]
		}
		for _, value := range matches {
			if value < 0 && c.readSubjects[-value-1] == channel {
				allowed = true
				return !denied
			}
		}
		denied = true
		return !allowed
	})
	if !allowed {
		return None
	}
	if denied {
		return Partial
	}
	return Full
}

// AllowsChannel checks a channel operation independent of any record topic.
// Seek changes the reader's channel position, rather than publishing a record.
func (c *Client) AllowsChannel(verb Verb, channel string) bool {
	return c == nil || c.grants[channel][verb] != nil
}
