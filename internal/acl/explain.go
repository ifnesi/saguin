package acl

import (
	"slices"
	"strings"
)

// Grant explains one rule, keeping role and rule order from the file. Filter
// is present on a narrowed channel grant; subject names the channel itself.
type Grant struct {
	Role    string   `json:"role"`
	Kind    string   `json:"kind"`
	Subject string   `json:"subject"`
	Filter  string   `json:"filter,omitempty"`
	Verbs   []string `json:"verbs"`
	Denies  []string `json:"denies"`
}

// Limit explains a selected publish limit in messages or bytes per second.
type Limit struct {
	Value  *int64 `json:"value"` // nil means none
	Source string `json:"source"`
}

// newLimit represents an unlimited configuration value as JSON null.
func newLimit(value int64, source string) Limit {
	l := Limit{Source: source}
	if value != 0 {
		l.Value = &value
	}
	return l
}

// Explanation is shared by the offline command and HTTP handler. No mutable
// session state is needed; both use the same entry and substitution policy.
type Explanation struct {
	User            string   `json:"user"`
	ACLFile         string   `json:"acl_file"`
	PatternApplied  *string  `json:"pattern_applied"`
	PatternsMatched []string `json:"patterns_matched"`
	PatternsInFile  []string `json:"patterns_in_file"`
	Grants          []Grant  `json:"grants"`
	GrantsWithheld  []Grant  `json:"grants_withheld"`
	ClientIDAllowed *bool    `json:"client_id_allowed"`
	PublishRate     Limit    `json:"publish_rate"`
	PublishBytes    Limit    `json:"publish_bytes"`
}

// EverythingAllowed is the complete JSON response without an acl_file.
type EverythingAllowed struct {
	ACLFile           *string `json:"acl_file"`
	EverythingAllowed bool    `json:"everything_allowed"`
}

// Explain reports the selected entry, matching patterns and effective grants.
// List fields are sorted except grants, whose file order is significant.
func (f *File) Explain(user string, clientID *string, rate, bytes int64) Explanation {
	x := Explanation{User: user, ACLFile: f.Path,
		PatternsInFile: make([]string, 0, len(f.entries)), PatternsMatched: []string{},
		Grants: []Grant{}, GrantsWithheld: []Grant{},
		PublishRate: newLimit(rate, "broker"), PublishBytes: newLimit(bytes, "broker")}
	var entry *Entry
	for i := range f.entries {
		e := &f.entries[i]
		x.PatternsInFile = append(x.PatternsInFile, e.Pattern)
		if e.matcher.MatchString(user) {
			x.PatternsMatched = append(x.PatternsMatched, e.Pattern)
			if entry == nil {
				entry = e
				x.PatternApplied = &e.Pattern
			}
		}
	}
	slices.Sort(x.PatternsInFile)
	slices.Sort(x.PatternsMatched)
	if clientID != nil {
		allowed := entry.AllowsClient(user, *clientID)
		x.ClientIDAllowed = &allowed
	}
	if entry == nil {
		return x
	}
	if entry.PublishRate != 0 {
		x.PublishRate = newLimit(int64(entry.PublishRate), "users."+entry.Pattern)
	}
	if entry.PublishBytes != 0 {
		x.PublishBytes = newLimit(entry.PublishBytes, "users."+entry.Pattern)
	}
	id := ""
	if clientID != nil {
		id = *clientID
	}
	replacer := newSubstitution(user, id)
	for _, i := range entry.rules {
		r := &f.rules[i]
		g := Grant{Role: r.Role, Kind: r.Kind, Subject: r.Subject, Filter: r.Filter, Verbs: r.Verbs, Denies: r.Denies}
		if g.Verbs == nil {
			g.Verbs = []string{}
		}
		if g.Denies == nil {
			g.Denies = []string{}
		}
		if withheld(r, user, id) {
			x.GrantsWithheld = append(x.GrantsWithheld, g)
			continue
		}
		// Keep a client-dependent rule as written until an id is supplied.
		if clientID != nil || !r.client {
			if r.Kind == "topic" {
				g.Subject = replacer.Replace(g.Subject)
			}
			g.Filter = replacer.Replace(g.Filter)
		}
		x.Grants = append(x.Grants, g)
	}
	return x
}

// newSubstitution never interprets replacement text as another placeholder.
func newSubstitution(user, id string) *strings.Replacer {
	return strings.NewReplacer("%u", user, "%c", id)
}
