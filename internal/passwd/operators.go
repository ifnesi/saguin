package passwd

import (
	"fmt"
	"slices"
	"strings"
)

// prefixReaches matches a route prefix at a slash boundary.
func prefixReaches(prefix, path string) bool {
	suffix, ok := strings.CutPrefix(path, prefix)
	return ok && (suffix == "" || strings.HasPrefix(suffix, "/"))
}

func parseScope(field string, routes []string) ([]string, error) {
	prefixes := strings.Split(field, ",")
	for _, prefix := range prefixes {
		if prefix == "" || !slices.ContainsFunc(routes, func(path string) bool { return prefixReaches(prefix, path) }) {
			return nil, fmt.Errorf("scope %q names no operations route", prefix)
		}
	}
	return prefixes, nil
}

// Reaches checks membership and scope without checking a password, for an
// identity already proved by a verified TLS certificate or PROXY header.
func (f *File) Reaches(user, path string) bool {
	e, exists := f.entries[user]
	return exists && (e.scope == nil || slices.ContainsFunc(e.scope, func(prefix string) bool { return prefixReaches(prefix, path) }))
}

// SetScope sets a listed operator's prefixes; all removes the third field.
// Save installs the edited source with the same guarantees as a password edit.
func (f *File) SetScope(user, field string) error {
	if f.routes == nil {
		return fmt.Errorf("%s: scope is only for an operators' file", f.name)
	}
	e, exists := f.entries[user]
	if !exists {
		return fmt.Errorf("%s is not listed in %s", user, f.name)
	}
	line := strings.TrimSuffix(f.text[e.start:e.end], "\n")
	_, rest, _ := strings.Cut(line, ":")
	hash, _, _ := strings.Cut(rest, ":")
	line = user + ":" + hash
	if field != "all" {
		if _, err := parseScope(field, f.routes); err != nil {
			return err
		}
		line += ":" + field
	}
	return f.replace(user, line)
}
