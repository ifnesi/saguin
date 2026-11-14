package passwd

import (
	"encoding/base64"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// The password-file reader, fuzzed: design.md, Testing, "Code that reads
// bytes or text from outside Sagüin is fuzzed, whether the input comes from
// clients, a bridge's peer or the operator's files". The reader this target
// calls, which the broker's start and `saguin --passwd` share:
//
//	// Parse reads the contents of the client password file named name.
//	func Parse(name string, data []byte) (*File, error)
//	// Users returns the file's users in file order.
//	func (f *File) Users() []string
//
// The oracle is config.md, broker.mqtt, password_file: "one user:hash line
// per user: a line empty, of spaces alone, or starting with # after spaces
// is ignored; otherwise the user name is everything before the first colon
// and the hash follows it, a second colon starting a third field; a user
// name is 1 to 65,535 bytes of valid UTF-8 with no control character,
// holds no :, does not start with # and has no leading or trailing space;
// the hash is $7$<iterations>$<salt>$<hash>, PBKDF2-HMAC-SHA512 with a
// 64-byte output, or $6$<salt>$<hash>, SHA-512 of the password then the
// salt; salt and hash in standard base64, their lengths unchecked; a $7$
// iteration count is a positive decimal; any other scheme, a line breaking
// this form (a carriage return or a user name outside those rules
// included), or a user named on two lines, is refused, naming the file and
// the lines"; design.md, Operations, Who may call: "A third field in a
// client password file also stops startup, naming the file and the line".
// Lines are counted from 1 and end at a line feed; a space is U+0020, the
// one config.md's "spaces alone" ignores. A file the format describes is
// accepted, its users in file order; a line it does not describe, or a
// user named twice, refuses the file, and a refusal of one such fault names
// the file and its lines. Where config.md leaves the verdict open (see
// open), the input is read but not judged.

// fileName is the name Parse is given; it holds no digit, so the numbers a
// refusal names are its lines.
const fileName = "/etc/saguin/passwd"

// verdict is the oracle's reading of a file.
type verdict struct {
	judged bool     // false where config.md leaves the verdict open
	users  []string // accepted: the users in file order
	faults [][]int  // refused: each fault's lines
}

// form is the oracle's reading of one part of a line.
type form int

const (
	valid form = iota
	invalid
	open // config.md leaves it open
)

// expected reads data as config.md's format sentence describes it.
func expected(data []byte) verdict {
	v := verdict{judged: true}
	seen := map[string]int{}
	for i, line := range strings.Split(string(data), "\n") {
		n := i + 1
		if rest := strings.TrimLeft(line, " "); rest == "" || rest[0] == '#' {
			continue // empty, spaces alone, or # after spaces: ignored
		}
		user, hash, ok := strings.Cut(line, ":")
		switch f := lineForm(user, hash, ok); {
		case f == open:
			return verdict{}
		case f == invalid:
			v.faults = append(v.faults, []int{n})
		case seen[user] != 0:
			v.faults = append(v.faults, []int{seen[user], n})
		default:
			seen[user] = n
			v.users = append(v.users, user)
		}
	}
	return v
}

// lineForm reads a line that is not ignored: user, then a colon (ok), then
// hash. A carriage return anywhere breaks the line's form, as does a user
// name outside userForm's rules. Whitespace other than a space at either
// end of the name is what config.md leaves open.
func lineForm(user, hash string, ok bool) form {
	if strings.ContainsRune(user+hash, '\r') || !ok || strings.Contains(hash, ":") {
		return invalid // a carriage return, no hash, or a third field
	}
	if f := userForm(user); f != valid {
		return f
	}
	parts := strings.Split(hash, "$")
	switch {
	case len(parts) == 5 && parts[0] == "" && parts[1] == "7":
		return worst(iterations(parts[2]), base64Form(parts[3], false), base64Form(parts[4], true))
	case len(parts) == 4 && parts[0] == "" && parts[1] == "6":
		return worst(base64Form(parts[2], false), base64Form(parts[3], true))
	}
	return invalid // any other scheme, or not the scheme's shape
}

// userForm reads a user name: 1 to 65,535 bytes of valid UTF-8 with no
// control character (U+0000 to U+001F and U+007F to U+009F, Unicode's
// category Cc), no colon, not starting with # and with no space at either
// end; any other whitespace at either end is left open.
func userForm(user string) form {
	switch {
	case user == "" || len(user) > 65535 || !utf8.ValidString(user):
		return invalid
	case strings.ContainsFunc(user, func(r rune) bool { return r < 0x20 || (r >= 0x7f && r <= 0x9f) }):
		return invalid
	case strings.ContainsRune(user, ':') || user[0] == '#' || user[0] == ' ' || user[len(user)-1] == ' ':
		return invalid
	case strings.TrimFunc(user, unicode.IsSpace) != user:
		return open
	}
	return valid
}

// worst is the weakest of forms: invalid before open before valid.
func worst(forms ...form) form {
	switch {
	case slices.Contains(forms, invalid):
		return invalid
	case slices.Contains(forms, open):
		return open
	}
	return valid
}

// iterations reads a $7$ count: decimal digits, a number. A leading zero,
// zero itself, or a count past what a 32-bit int holds is left open.
func iterations(s string) form {
	if s == "" || strings.IndexFunc(s, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return invalid
	}
	if n, err := strconv.ParseInt(s, 10, 64); err != nil || s[0] == '0' || n > math.MaxInt32 {
		return open
	}
	return valid
}

// base64Form reads a salt or, where output, a hash: standard base64. An
// encoding only the lenient decoder takes, an empty salt, or a hash that is
// not the 64 bytes both schemes produce, is left open.
func base64Form(s string, output bool) form {
	decoded, err := base64.StdEncoding.Strict().DecodeString(s)
	switch {
	case err != nil:
		if _, lenient := base64.StdEncoding.DecodeString(s); lenient == nil {
			return open
		}
		return invalid
	case output && len(decoded) != 64:
		return open
	case !output && len(decoded) == 0:
		return open
	}
	return valid
}

// numbers finds the decimal numbers in a refusal.
var numbers = regexp.MustCompile(`\d+`)

// b64 encodes n bytes of b in standard base64, for the seeds.
func b64(n int, b byte) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(rune(b)), n)))
}

func FuzzParse(f *testing.F) {
	seven := "$7$1000$" + b64(64, 'a') + "$" + b64(64, 'h')
	six := "$6$" + b64(12, 's') + "$" + b64(64, 'h')
	for _, seed := range []string{
		"",
		"# only a comment\n",
		"alice:" + seven + "\n",
		"# clients\nalice:" + six + "\n\nbob:" + seven + "\n# end",
		"alice:" + six + "\nbob:" + six + "\nalice:" + seven + "\n",
		"bob:$5$" + b64(12, 's') + "$" + b64(32, 'h') + "\n",
		"alice:" + six + ":/metrics\n",
		"no colon at all\n",
		"alice:$6$!!!$" + b64(64, 'h') + "\n",
		"alice:$7$many$" + b64(64, 'a') + "$" + b64(64, 'h') + "\n",
		"alice:$7$1000$" + b64(64, 'a') + "\n",
		"alice:" + six + "\r\n",
		"  alice : " + six + "\n",
		"alice:" + seven + "\nbob:" + seven,
		"   \n   # an indented comment\n#alice:" + six + "\nbob:" + six + "\n",
		":" + six + "\n",
		"al\x01ice:" + six + "\n",
		"al\x7fice:" + six + "\n",
		"al\u0085ice:" + six + "\n",
		"al\tice:" + six + "\n",
		"\xffalice:" + six + "\n",
		strings.Repeat("u", 65535) + ":" + six + "\n",
		strings.Repeat("u", 65536) + ":" + six + "\n",
		" alice:" + six + "\n",
		"alice :" + six + "\n",
		"Ana María:" + six + "\nJoão:" + seven + "\n用户:" + six + "\n",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		file, err := Parse(fileName, data)
		judge(t, data, file, err, expected(data))
	})
}

// judge fails the test unless file and err, a reader's answer for data,
// are want's: where want accepts data, the users in file order; where it
// refuses it, a refusal naming the file and, for one fault, its lines. It
// reports whether data was judged and accepted.
func judge(t *testing.T, data []byte, file *File, err error, want verdict) bool {
	t.Helper()
	switch {
	case !want.judged:
		return false // read, and left unjudged
	case len(want.faults) == 0 && err != nil:
		t.Fatalf("refused %q: %v; want it accepted, users %q", data, err, want.users)
	case len(want.faults) == 0:
		if got := file.Users(); !slices.Equal(got, want.users) {
			t.Fatalf("accepted %q with users %q; want %q, in file order", data, got, want.users)
		}
		return true
	case err == nil:
		t.Fatalf("accepted %q; want it refused for its lines %v", data, want.faults)
	case !strings.Contains(err.Error(), fileName):
		t.Fatalf("refused %q with %q, which does not name the file %s", data, err, fileName)
	case len(want.faults) == 1:
		named := numbers.FindAllString(strings.ReplaceAll(err.Error(), fileName, ""), -1)
		for _, n := range want.faults[0] {
			if !slices.Contains(named, strconv.Itoa(n)) {
				t.Fatalf("refused %q with %q, which does not name its line %d (lines %v)", data, err, n, want.faults[0])
			}
		}
	}
	return false
}
