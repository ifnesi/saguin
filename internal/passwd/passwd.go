// Package passwd reads password files and verifies their credentials.
package passwd

import (
	"crypto/pbkdf2"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	defaultIterations = 1000
	saltBytes         = 64
)

// File is a parsed client password file. Once shared with readers it is
// immutable; editing methods belong only to the standalone password command.
type File struct {
	name    string
	text    string
	users   []string
	entries map[string]entry
	routes  []string // nil for a client file; immutable scope validation paths
}

type entry struct {
	credential
	scope      []string // nil grants every route
	line       int
	start, end int // complete source line, including its line feed if present
}

type credential struct {
	salt, sum []byte
	count     int // zero selects $6$, a positive count selects $7$
}

var unknownCredential = credential{
	salt:  make([]byte, saltBytes),
	sum:   make([]byte, sha512.Size),
	count: defaultIterations,
}

// checkCredential is the one check Verify runs; a test records it.
var checkCredential = credential.matches

// Parse reads the client password file named name. Diagnostics count lines
// from one. Source bytes are kept so edits preserve unrelated lines exactly.
func Parse(name string, data []byte) (*File, error) {
	return parse(name, data, nil)
}

// ParseOperators reads the same lines as Parse, allowing a third field of
// comma-separated route prefixes. Every prefix must reach a supplied route.
func ParseOperators(name string, data []byte, routes []string) (*File, error) {
	return parse(name, data, append([]string{}, routes...))
}

func parse(name string, data []byte, routes []string) (*File, error) {
	f := &File{name: name, text: string(data), entries: make(map[string]entry), routes: routes}
	for rest, start, line := f.text, 0, 1; rest != ""; line++ {
		text, next, newline := strings.Cut(rest, "\n")
		end := start + len(text)
		if newline {
			end++
		}
		if trimmed := strings.TrimLeft(text, " "); trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			if strings.ContainsRune(text, '\r') {
				return nil, fmt.Errorf("%s: line %d: carriage return in password file", name, line)
			}
			user, hash, ok := strings.Cut(text, ":")
			hash, field, scoped := strings.Cut(hash, ":")
			if !ok || (scoped && routes == nil) {
				return nil, fmt.Errorf("%s: line %d: expected user:hash, without a third field", name, line)
			}
			if err := checkUser(user); err != nil {
				return nil, fmt.Errorf("%s: line %d: %w", name, line, err)
			}
			if previous, exists := f.entries[user]; exists {
				return nil, fmt.Errorf("%s: lines %d and %d: duplicate user %q", name, previous.line, line, user)
			}
			key, err := parseCredential(hash)
			if err != nil {
				return nil, fmt.Errorf("%s: line %d: %w", name, line, err)
			}
			var scope []string
			if scoped {
				scope, err = parseScope(field, routes)
				if err != nil {
					return nil, fmt.Errorf("%s: line %d: %w", name, line, err)
				}
			}
			f.entries[user] = entry{credential: key, scope: scope, line: line, start: start, end: end}
			f.users = append(f.users, user)
		}
		rest, start = next, end
	}
	return f, nil
}

// checkUser is shared by file loading and the password command. Space means
// the file format's ASCII space; control characters include tabs and newlines.
func checkUser(user string) error {
	if user == "" || len(user) > 65535 || !utf8.ValidString(user) ||
		strings.ContainsFunc(user, unicode.IsControl) || strings.ContainsRune(user, ':') ||
		strings.HasPrefix(user, "#") || strings.HasPrefix(user, " ") || strings.HasSuffix(user, " ") {
		return fmt.Errorf("user %q cannot be represented in a password file", user)
	}
	return nil
}

func parseCredential(hash string) (credential, error) {
	parts := strings.Split(hash, "$")
	var key credential
	var salt, sum string
	switch {
	case len(parts) == 5 && parts[0] == "" && parts[1] == "7":
		count := parts[2]
		if count == "" || strings.ContainsFunc(count, func(r rune) bool { return r < '0' || r > '9' }) {
			return key, errors.New("iterations must be a positive decimal")
		}
		n, err := strconv.Atoi(count)
		if err != nil || n <= 0 {
			return key, errors.New("iterations must be a positive decimal within the integer range")
		}
		key.count, salt, sum = n, parts[3], parts[4]
	case len(parts) == 4 && parts[0] == "" && parts[1] == "6":
		salt, sum = parts[2], parts[3]
	default:
		return key, errors.New("expected a $7$ or $6$ password hash")
	}
	var err error
	key.salt, err = base64.StdEncoding.DecodeString(salt)
	if err != nil {
		return key, errors.New("salt is not standard base64")
	}
	key.sum, err = base64.StdEncoding.DecodeString(sum)
	if err != nil {
		return key, errors.New("hash is not standard base64")
	}
	return key, nil
}

// Users returns the users in file order, in a slice the caller owns.
func (f *File) Users() []string { return slices.Clone(f.users) }

// Verify checks a password and compares its hash in constant time. An unknown
// user performs a fixed $7$ check with defaultIterations and a 64-byte salt and
// sum. Known users retain the cost of their stored scheme and iteration count.
func (f *File) Verify(user, password string) bool {
	e, known := f.entries[user]
	if !known {
		e.credential = unknownCredential
	}
	match := checkCredential(e.credential, password)
	return known && match
}

func (key credential) matches(password string) bool {
	var sum []byte
	if key.count == 0 {
		h := sha512.New()
		_, _ = h.Write([]byte(password)) // hash writes cannot fail
		_, _ = h.Write(key.salt)
		sum = h.Sum(nil)
	} else {
		var err error
		sum, err = pbkdf2.Key(sha512.New, password, key.salt, key.count, sha512.Size)
		if err != nil {
			return false // Parse admits only valid PBKDF2 parameters.
		}
	}
	return subtle.ConstantTimeCompare(sum, key.sum) == 1
}
