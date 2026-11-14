package passwd

import (
	"slices"
	"strings"
	"testing"
	"unicode"
)

// The operators' password file's reader, fuzzed (design.md, Testing: "Code
// that reads bytes or text from outside Sagüin is fuzzed, whether the
// input comes from clients, a bridge's peer or the operator's files"). The
// reader this target calls, which the broker's start, --check-config and
// `saguin --passwd scope` share:
//
//	// ParseOperators reads the operators' password file named name: the
//	// lines Parse reads, each of which may end in a third field of route
//	// prefixes separated by commas. routes are the operations routes'
//	// paths; a prefix that reaches none of them at a / boundary refuses
//	// the file, naming the file and the line.
//	func ParseOperators(name string, data []byte, routes []string) (*File, error)
//	// Reaches reports whether user's line reaches path: any path where the
//	// line has no third field, else a path equal to one of its prefixes or
//	// continuing one after a /.
//	func (f *File) Reaches(user, path string) bool
//
// The oracle is config.md's password_file sentence (expected, lineForm) for
// each line's user and hash, and design.md, Operations, Who may call: "A
// line of the operators' password file may end in a third field containing
// route prefixes. These are separated by commas and matched at a /
// boundary (prometheus:$7$…:/metrics). Without that field, the user reaches
// every route. A scope naming no route stops startup". A prefix the design
// leaves open is read but not judged: an empty one, one ending in a /, one
// holding white space, and one reaching below a route, as
// /v1/operations/queues/orders does below the queues route.

// operationsRoutes are the routes of design.md, Operations, HTTP, by path:
// /health, /metrics and each answer under /v1/operations/, queues/<channel>
// by the path its channels continue.
var operationsRoutes = []string{"/health", "/metrics", "/v1/operations/acl", "/v1/operations/config",
	"/v1/operations/bridges", "/v1/operations/consumers", "/v1/operations/sessions", "/v1/operations/users",
	"/v1/operations/queues", "/v1/operations/refused", "/v1/operations/position-lost"}

// reaches reports whether one of prefixes reaches path at a / boundary.
func reaches(prefixes []string, path string) bool {
	return slices.ContainsFunc(prefixes, func(p string) bool { return path == p || strings.HasPrefix(path, p+"/") })
}

// scopeForm reads a third field: each prefix valid where it reaches a
// route, invalid where it reaches none, and open where the design leaves
// it so (above). A carriage return breaks the line's form.
func scopeForm(field string) form {
	if strings.ContainsRune(field, '\r') {
		return invalid
	}
	forms := []form{valid}
	for p := range strings.SplitSeq(field, ",") {
		switch {
		case p == "" || strings.HasSuffix(p, "/") || strings.ContainsFunc(p, unicode.IsSpace):
			forms = append(forms, open)
		case slices.ContainsFunc(operationsRoutes, func(r string) bool { return reaches([]string{p}, r) }):
		case slices.ContainsFunc(operationsRoutes, func(r string) bool { return strings.HasPrefix(p, r+"/") }):
			forms = append(forms, open)
		default:
			forms = append(forms, invalid)
		}
	}
	return worst(forms...)
}

// expectedOperators reads data as an operators' file: expected's verdict,
// a line's third field judged by scopeForm, and the scope of each user
// accepted, nil for a line with none.
func expectedOperators(data []byte) (v verdict, scopes map[string][]string) {
	v = verdict{judged: true}
	seen := map[string]int{}
	scopes = map[string][]string{}
	for i, line := range strings.Split(string(data), "\n") {
		n := i + 1
		if rest := strings.TrimLeft(line, " "); rest == "" || rest[0] == '#' {
			continue // empty, spaces alone, or # after spaces: ignored
		}
		user, rest, ok := strings.Cut(line, ":")
		hash, field, scoped := strings.Cut(rest, ":")
		f := lineForm(user, hash, ok)
		if scoped {
			f = worst(f, scopeForm(field))
		}
		switch {
		case f == open:
			return verdict{}, nil
		case f == invalid:
			v.faults = append(v.faults, []int{n})
		case seen[user] != 0:
			v.faults = append(v.faults, []int{seen[user], n})
		default:
			seen[user] = n
			v.users = append(v.users, user)
			if scoped {
				scopes[user] = strings.Split(field, ",")
			}
		}
	}
	return v, scopes
}

func FuzzParseOperators(f *testing.F) {
	six := "$6$" + b64(12, 's') + "$" + b64(64, 'h')
	for _, seed := range []string{
		"",
		"# operators\nroot:" + six + "\n",
		"prom:" + six + ":/metrics\n",
		"prom:" + six + ":/metrics,/health\n\n# auditors\naudit:" + six + ":/v1/operations\n",
		"pair:" + six + ":/health,/v1/operations/acl\nroot:" + six + "\n",
		"prom:" + six + ":/metric\n",
		"prom:" + six + ":/metricsx\n",
		"prom:" + six + ":/v1/operations/ac\n",
		"prom:" + six + ":/nowhere\n",
		"prom:" + six + ":metrics\n",
		"prom:" + six + ":\n",
		"prom:" + six + ":/metrics,\n",
		"prom:" + six + ":/v1/operations/\n",
		"prom:" + six + ":/v1/operations/queues/orders\n",
		"prom:" + six + ":/metrics:/health\n",
		"prom:" + six + ":/metrics\r\n",
		"prom:" + six + ": /metrics\n",
		"prom:" + six + ":/metrics\nprom:" + six + ":/health\n",
		"pr\x01om:" + six + ":/metrics\n",
	} {
		f.Add([]byte(seed))
	}
	probes := slices.Clone(operationsRoutes)
	for _, r := range operationsRoutes {
		probes = append(probes, r+"x", r+"/x")
	}
	probes = append(probes, "/", "/v1", "/v1/operations")
	f.Fuzz(func(t *testing.T, data []byte) {
		file, err := ParseOperators(fileName, data, operationsRoutes)
		want, scopes := expectedOperators(data)
		if !judge(t, data, file, err, want) {
			return
		}
		for _, user := range want.users {
			for _, path := range probes {
				if got, want := file.Reaches(user, path), scopes[user] == nil || reaches(scopes[user], path); got != want {
					t.Fatalf("accepted %q: %s reaches %s: %v; want %v, its scope %q", data, user, path, got, want, scopes[user])
				}
			}
		}
	})
}
