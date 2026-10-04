// Package passwd reads Mosquitto password files.
//
// **The format is Mosquitto's, exactly, so that an existing file works.**
// An operator moving a fleet to saguin has a password file already and no
// way to re-hash it - they do not hold their users' passwords, only the
// hashes - so a format of saguin's own would mean every device on the fleet
// getting a new credential before the broker could be swapped. That is not
// a migration anybody performs.
//
// Two schemes are read, both verified against files written by
// mosquitto_passwd 2.1.2 rather than from its documentation:
//
//   - `$6$<salt>$<hash>` is SHA512 of the password followed by the salt,
//     which Mosquitto 1.6 and earlier wrote.
//   - `$7$<iterations>$<salt>$<hash>` is PBKDF2-HMAC-SHA512 over the
//     password with that salt and count, 64 bytes out. Mosquitto 2.0
//     defaulted to it for five years, so it is what a file in the wild
//     almost always holds.
//
// Salt and hash are standard base64. argon2id is refused rather than
// guessed at, and refused by name - see Load.
package passwd

import (
	"bufio"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// Iterations is what saguin writes, rather than a modern-looking number.
//
// **A password file is read on every CONNECT**, so the cost is paid by
// every device on every reconnect, and a fleet coming back after a link
// drop pays it at once. Measured on an AMD A10: 1000 iterations is 2.55ms,
// 100,000 is 240ms, and OWASP's 210,000 is half a second. A broker on a
// gateway cannot spend half a second per connection.
//
// What it costs is resistance to offline cracking of a stolen file, and the
// defence there is the file's permissions.
const Iterations = 1000

// saltBytes and keyBytes are what mosquitto_passwd writes: 64 of each.
const (
	saltBytes = 64
	keyBytes  = 64
)

// File is a password file's contents, in the order the file holds them so
// that rewriting one leaves it recognisable to whoever wrote it.
type File struct {
	path  string
	lines []line

	// index maps a user to their position in lines, and first is the position
	// of the file's first user; both are meaningful only while index is not
	// empty. A lookup that scanned every line cost about 50 µs at 21,210
	// users, 3 to 15 percent of one verification. **Every writer of lines
	// keeps them true**: Load and Set's append add to them, Delete rebuilds
	// them, and SetScopes changes a line in place so moves nothing. A name
	// occurs once - Load refuses a second - so the map has no ambiguity.
	index map[string]int
	first int
}

// reindex rebuilds index and first from lines.
func (f *File) reindex() {
	f.index = make(map[string]int, len(f.lines))
	for i, l := range f.lines {
		if l.user == "" {
			continue
		}
		if len(f.index) == 0 {
			f.first = i
		}
		f.index[l.user] = i
	}
}

// find is the line holding a user.
func (f *File) find(user string) (*line, bool) {
	i, ok := f.index[user]
	if !ok {
		return nil, false
	}
	return &f.lines[i], true
}

// line is one user, or something else the file holds. A comment or a blank
// line is kept exactly as it was: saguin is not the only thing that reads
// these files, and reformatting somebody's file is not saguin's to do.
type line struct {
	user string // "" when this line is not a user
	raw  string
	hash hashed

	// scopes is the third colon-delimited field: the route prefixes this
	// user may reach, or nil for every route.
	//
	// **Nil and empty are different**, and the distinction is the whole
	// design. A line with two fields - which is every file written before
	// this existed, and every Mosquitto file - reaches everything, so
	// nothing migrates. A line whose third field is empty reaches nothing,
	// which is a locked-out user and is refused when the file is read.
	scopes []string
}

type hashed struct {
	scheme     string
	iterations int
	salt       []byte
	sum        []byte
}

// Load reads a password file.
//
// **A hash saguin cannot read is an error naming the file, the line and the
// scheme - never a user who fails to authenticate.** An argon2id file is
// exactly what an operator migrating from Mosquitto 2.1 has, and if it
// merely made every login fail, what they would see is every device
// appearing to have the wrong password: a broker that looks broken in the
// one way that sends somebody looking at their fleet instead of at this.
func Load(path string) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := &File{path: path}
	seen := map[string]int{}
	scan := bufio.NewScanner(f)
	for n := 1; scan.Scan(); n++ {
		raw := scan.Text()
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			out.lines = append(out.lines, line{raw: raw})
			continue
		}
		user, rest, ok := strings.Cut(raw, ":")
		if !ok {
			return nil, fmt.Errorf("%s:%d: no colon, so this is neither a user nor a comment", path, n)
		}
		// **A name on two lines is refused, naming both**, as mosquitto
		// refuses it ("Duplicate user"). Every reader here acts on the first
		// match, so the second line's password was invisible until Delete
		// removed the first - and then it was the one that admitted the
		// device an operator had just withdrawn. A file mosquitto_passwd or
		// Set wrote has none; one edited by hand or concatenated can.
		if first, ok := seen[user]; ok {
			return nil, fmt.Errorf("%s:%d: user %q is already on line %d; a name may appear once",
				path, n, user, first)
		}
		seen[user] = n
		// **The colon is what makes a third field parseable at all.** A user
		// name holding one is not a name - `saguin --passwd add` refuses it,
		// and read back, the first colon would end it - and a stored hash is
		// `$`-prefixed base64, whose alphabet has none either. So the first
		// colon ends the name and the second, if there is one, ends the
		// hash, and no realistic value is misread. A delimiter inside the
		// name would not survive `alice@example.com`, which is a name this
		// file accepts today.
		stored, scopeField, scoped := strings.Cut(rest, ":")
		h, err := parse(stored)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: user %q: %w", path, n, user, err)
		}
		l := line{user: user, raw: raw, hash: h}
		if scoped {
			if l.scopes, err = parseScopes(scopeField); err != nil {
				return nil, fmt.Errorf("%s:%d: user %q: %w", path, n, user, err)
			}
		}
		out.lines = append(out.lines, l)
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	out.reindex()
	return out, nil
}

// parseScopes reads the third field: the route prefixes a user may reach.
//
// **Every prefix is checked here for shape, and against the routes saguin
// serves by the caller that knows them.** A scope naming no route is a user
// denied everything by a typo, which is the failure this file exists to
// prevent rather than one to introduce - and it reads, from the file, as
// though it granted something.
func parseScopes(field string) ([]string, error) {
	var out []string
	for _, one := range strings.Split(field, ",") {
		one = strings.TrimSpace(one)
		switch {
		case one == "":
			return nil, fmt.Errorf(
				"an empty route in the scope list, which names nothing: write the routes " +
					"this user may reach, or remove the field to let it reach every one")
		case !strings.HasPrefix(one, "/"):
			return nil, fmt.Errorf(
				"scope %q does not begin with a slash, so it is not a route. Scopes are "+
					"the paths this user may reach, such as /metrics", one)
		}
		out = append(out, one)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf(
			"an empty scope field, which reaches no route at all: remove the field to " +
				"reach every one")
	}
	return out, nil
}

// KnownRoutes is every path the operations listener serves, which is what a
// scope in this file may name.
//
// **It lives here rather than in the broker** so that `--passwd` can refuse
// a typo without loading a configuration - the command takes a file and no
// config on purpose, and a scope naming no route is a user denied
// everything by a slip of the keyboard.
var KnownRoutes = []string{
	"/metrics",
	"/v1/operations",
	"/v1/operations/acl",
	"/v1/operations/config",
	"/v1/operations/consumers",
	"/v1/operations/position-lost",
	"/v1/operations/queues",
	"/v1/operations/refused",
	"/v1/operations/sessions",
	"/v1/operations/users",
}

// CheckScopes reports what is wrong with a user's scopes, naming the routes
// that exist. A scope matching no route reads as though it granted
// something and grants nothing.
func (f *File) CheckScopes() []string {
	var findings []string
	for _, l := range f.lines {
		for _, s := range l.scopes {
			if !slices.Contains(KnownRoutes, s) {
				findings = append(findings, fmt.Sprintf(
					"user %q names route %q, which saguin does not serve. It serves %s",
					l.user, s, strings.Join(KnownRoutes, ", ")))
			}
		}
	}
	return findings
}

// Reaches reports whether this user may reach a path.
//
// **A user with no scopes reaches everything**, which is every file written
// before scopes existed, and is why nothing migrates.
//
// **Matched at a segment boundary, never as a string prefix.** Plain prefix
// matching would let `/v1/operations` also grant `/v1/operationsomething` -
// a route nobody granted, reached by a name that merely starts the same
// way, which is the shape of finding nobody notices until it is one.
func (f *File) Reaches(user, path string) bool {
	l, ok := f.find(user)
	if !ok {
		return false
	}
	if l.scopes == nil {
		return true
	}
	for _, s := range l.scopes {
		if path == s || strings.HasPrefix(path, strings.TrimSuffix(s, "/")+"/") {
			return true
		}
	}
	return false
}

// Known reports whether this file has an entry for a user, whatever its
// password or scopes.
//
// **It is asked of a name the broker did not check a password for** - one a
// client certificate carried, or a proxy asserted - to decide whether the
// file has anything to say about what that name may reach. A name it does
// not hold is not refused; it is simply unscoped, and the caller decides
// what an unscoped principal gets.
func (f *File) Known(user string) bool {
	if f == nil {
		return false
	}
	_, ok := f.find(user)
	return ok
}

// Scopes is what a user may reach, or nil for every route.
func (f *File) Scopes(user string) []string {
	if l, ok := f.find(user); ok {
		return l.scopes
	}
	return nil
}

// AnyScoped reports whether any user in the file names routes at all, which
// is what an MQTT password file must not do - it has no routes - and what
// decides whether `--passwd list` grows a column.
func (f *File) AnyScoped() bool {
	for _, l := range f.lines {
		if l.scopes != nil {
			return true
		}
	}
	return false
}

// parse reads one stored hash.
func parse(s string) (hashed, error) {
	parts := strings.Split(s, "$")
	// A stored hash opens with `$`, so the first field is empty.
	if len(parts) < 2 || parts[0] != "" {
		return hashed{}, fmt.Errorf("the password is not hashed. saguin does not read plain " +
			"text passwords; hash the file with `mosquitto_passwd -U <file>`")
	}
	switch parts[1] {
	case "6":
		if len(parts) != 4 {
			return hashed{}, fmt.Errorf("a $6$ hash is $6$<salt>$<hash>")
		}
		salt, sum, err := decode(parts[2], parts[3])
		return hashed{scheme: "6", salt: salt, sum: sum}, err
	case "7":
		if len(parts) != 5 {
			return hashed{}, fmt.Errorf("a $7$ hash is $7$<iterations>$<salt>$<hash>")
		}
		iter, err := strconv.Atoi(parts[2])
		if err != nil || iter < 1 {
			return hashed{}, fmt.Errorf("iteration count %q is not a positive number", parts[2])
		}
		salt, sum, err := decode(parts[3], parts[4])
		return hashed{scheme: "7", iterations: iter, salt: salt, sum: sum}, err
	case "argon2id", "argon2i", "argon2d":
		// Named rather than lumped in with the unknown, because this is the
		// one an operator will actually meet: Mosquitto 2.1 documents it as
		// the default. The message says what to do about it.
		return hashed{}, fmt.Errorf("saguin does not read %s hashes. Re-hash this user with "+
			"`mosquitto_passwd -H sha512-pbkdf2 <file> <user>`, which needs the password rather "+
			"than the hash", parts[1])
	default:
		return hashed{}, fmt.Errorf("unknown hash scheme $%s$", parts[1])
	}
}

func decode(saltB64, sumB64 string) ([]byte, []byte, error) {
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return nil, nil, fmt.Errorf("the salt is not base64")
	}
	sum, err := base64.StdEncoding.DecodeString(sumB64)
	if err != nil {
		return nil, nil, fmt.Errorf("the hash is not base64")
	}
	return salt, sum, nil
}

// Verify reports whether a password is this user's.
//
// **An unknown user costs what the file's first user costs.** Without that,
// the time a refusal takes says which usernames are real - a $6$ line checks
// in a quarter of a microsecond, a $7$ one in a third of a millisecond - and
// the answer is a list of accounts worth attacking. So an unknown user is
// checked against the first user's own hash, whose result is thrown away.
// That hides every name in a file of one scheme and count, which is every
// file mosquitto_passwd or saguin wrote; in a file mixing them, a line
// unlike the first still takes its own time.
//
// The comparison is constant time for the same reason it always is: a
// comparison that stops at the first wrong byte tells an attacker how much
// of their guess was right.
func (f *File) Verify(user, password string) bool {
	if l, ok := f.find(user); ok {
		return l.hash.matches(password)
	}
	var absent hashed
	if len(f.index) > 0 {
		absent = f.lines[f.first].hash
	} else {
		absent = hashed{scheme: "7", iterations: Iterations, salt: make([]byte, saltBytes), sum: make([]byte, keyBytes)}
	}
	absent.matches(password)
	return false
}

func (h hashed) matches(password string) bool {
	var sum []byte
	switch h.scheme {
	case "6":
		d := sha512.Sum512(append([]byte(password), h.salt...))
		sum = d[:]
	case "7":
		var err error
		sum, err = pbkdf2.Key(sha512.New, password, h.salt, h.iterations, keyBytes)
		if err != nil {
			return false
		}
	default:
		return false
	}
	return subtle.ConstantTimeCompare(sum, h.sum) == 1
}

// Users is every user the file names, in the order it names them.
func (f *File) Users() []string {
	var out []string
	for _, l := range f.lines {
		if l.user != "" {
			out = append(out, l.user)
		}
	}
	return out
}

// Set adds a user or replaces their password, in place, leaving every other
// line - comments included - exactly as it was.
func (f *File) Set(user, password string) error {
	h, err := Hash(password)
	if err != nil {
		return err
	}
	if l, ok := f.find(user); ok {
		// **The scope survives a password change**, which is the whole
		// reason the two are separate commands. Rebuilding the line from
		// the name and the hash alone would silently widen a narrowed
		// user back to every route, at the moment somebody was rotating
		// a credential and thinking about something else.
		l.hash, l.raw = mustParse(h), render(user, h, l.scopes)
		return nil
	}
	if len(f.index) == 0 {
		f.first = len(f.lines)
	}
	if f.index == nil {
		f.index = map[string]int{}
	}
	f.index[user] = len(f.lines)
	f.lines = append(f.lines, line{user: user, hash: mustParse(h), raw: user + ":" + h})
	return nil
}

// SetScopes narrows a user to the routes it may reach, or widens it to
// every route with nil.
//
// **It is a set rather than an edit**, and it has to be: an absent scope
// means every route, so an incremental "add" against a user who currently
// reaches everything would have to *narrow* them - an add that removes, with
// no wording that survives it.
func (f *File) SetScopes(user string, scopes []string) bool {
	l, ok := f.find(user)
	if !ok {
		return false
	}
	l.scopes = scopes
	l.raw = render(user, storedOf(l.raw), scopes)
	return true
}

// render writes one line back. A user with no scopes writes two fields, so
// a file nobody has narrowed is byte for byte the file it always was.
func render(user, stored string, scopes []string) string {
	if len(scopes) == 0 {
		return user + ":" + stored
	}
	return user + ":" + stored + ":" + strings.Join(scopes, ",")
}

// storedOf is the hash as it sits in a line, without the name or the scope.
func storedOf(raw string) string {
	_, rest, _ := strings.Cut(raw, ":")
	stored, _, _ := strings.Cut(rest, ":")
	return stored
}

// mustParse re-reads a hash this package has just written. It cannot fail
// on a value Hash produced, and a hash the file holds but cannot verify is
// worse than a panic here - the line would load and every login would fail.
func mustParse(stored string) hashed {
	h, err := parse(stored)
	if err != nil {
		panic("passwd: a hash this package wrote will not parse: " + err.Error())
	}
	return h
}

// Delete removes a user, and says whether there was one.
func (f *File) Delete(user string) bool {
	i, ok := f.index[user]
	if !ok {
		return false
	}
	f.lines = append(f.lines[:i], f.lines[i+1:]...)
	f.reindex()
	return true
}

// Hash is a new `$7$` entry, in the format mosquitto_passwd writes, so a
// file saguin has touched still opens in Mosquitto's own tools.
func Hash(password string) (string, error) {
	salt := make([]byte, saltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	sum, err := pbkdf2.Key(sha512.New, password, salt, Iterations, keyBytes)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("$7$%d$%s$%s", Iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(sum)), nil
}

// Save writes the file back.
//
// **Through a temporary file and a rename**, so that a crash or a full disk
// leaves the old file rather than half a new one: a password file truncated
// half way through is every user after that point locked out, and the
// operator's copy of it is gone.
//
// The mode is 0600 for a file saguin creates. Whoever can read it can spend
// as long as they like guessing against it, and this is the whole of what
// stands between a stolen copy and the fleet's credentials.
//
// **A file that exists keeps its mode and its owner**, as mosquitto_passwd's
// in-place rewrite keeps them. The rename puts a new inode in place, which
// was 0600 and owned by whoever ran the command: an operator provisioning a
// device as root left a broker running as its own user a file it could not
// read, and the SIGUSR1 meant to admit the device refused every credential
// file instead. If the owner cannot be given back, nothing is replaced.
func (f *File) Save() error {
	mode, uid, gid := os.FileMode(0o600), -1, -1
	if info, err := os.Stat(f.path); err == nil {
		mode = info.Mode().Perm()
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			uid, gid = int(st.Uid), int(st.Gid)
		}
	}
	tmp, err := os.CreateTemp(dirOf(f.path), ".saguin-passwd-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if uid >= 0 {
		if err := tmp.Chown(uid, gid); err != nil {
			tmp.Close()
			return fmt.Errorf("%s is owned by uid %d gid %d, and the rewritten file cannot be "+
				"given back to them (%w); it is left as it was", f.path, uid, gid, err)
		}
	}
	w := bufio.NewWriter(tmp)
	for _, l := range f.lines {
		if _, err := fmt.Fprintln(w, l.raw); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		tmp.Close()
		return err
	}
	// Synced before the rename, or the rename can be on the disk with the
	// contents still in the page cache - which after a power cut is an
	// empty password file where a good one used to be.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), f.path)
}

// New is an empty file at a path, for the first user added to one that does
// not exist yet.
func New(path string) *File { return &File{path: path} }

func dirOf(path string) string {
	if i := strings.LastIndexByte(path, '/'); i > 0 {
		return path[:i]
	}
	return "."
}
