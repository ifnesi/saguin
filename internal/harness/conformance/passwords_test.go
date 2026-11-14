package conformance

import (
	"bytes"
	"crypto/sha512"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of password files and the check at CONNECT (item 0018a):
// config.md, broker.mqtt, password_file: "one user:hash line per user
// (blank lines and # comments ignored); the hash is
// $7$<iterations>$<salt>$<hash>, PBKDF2-HMAC-SHA512 with a 64-byte output,
// or $6$<salt>$<hash>, SHA-512 of the password then the salt; salt and hash
// in standard base64; any other scheme, or a user named on two lines, is
// refused, naming the file and the lines"; allow_anonymous, "admits a
// client with no user name", default "true without password_file, false
// with one". design.md, Flows, CONNECT, 2. Check: "The broker checks
// password, access, version, client id and limits. A failure gets a CONNACK
// with its reason, then the connection closes"; the refusal table: 0x86, "a
// wrong or missing credential", MQTT 3.1.1 return code 4. Every hash a
// test writes is made here from config.md's definitions, through Go's own
// crypto/pbkdf2 (harness.SevenHash) and crypto/sha512, never with the
// broker's code.

// b64 is standard base64, as config.md writes salt and hash.
var b64 = base64.StdEncoding

// salt64 and salt12 are salts of 64 and 12 bytes: the first's base64 ends
// in padding, the second's has none.
var (
	salt64 = bytes.Repeat([]byte{0xA5, 0x3C, 0x0F, 0x96}, 16)
	salt12 = []byte("twelve-bytes")
)

// sevenHash is config.md's $7$ hash of password: PBKDF2-HMAC-SHA512 over
// iterations, with salt, its 64-byte output, salt and output in standard
// base64.
func sevenHash(t *testing.T, password string, iterations int, salt []byte) string {
	t.Helper()
	hash, err := harness.SevenHash(password, iterations, salt)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

// sixHash is config.md's $6$ hash of password: SHA-512 of the password then
// the salt, salt and sum in standard base64.
func sixHash(password string, salt []byte) string {
	sum := sha512.Sum512(append([]byte(password), salt...))
	return "$6$" + b64.EncodeToString(salt) + "$" + b64.EncodeToString(sum[:])
}

// passwordFile writes lines, each ended by a line feed, to the file name in
// dir, and returns its path.
func passwordFile(t *testing.T, dir, name string, lines ...string) string {
	t.Helper()
	return testkit.WriteFile(t, dir, name, strings.Join(lines, "\n")+"\n")
}

// credentials is an MQTT 5 CONNECT of a new session for client, carrying
// user and password.
func credentials(client, user, password string) *harness.Connect {
	return &harness.Connect{ClientID: client, CleanStart: true,
		HasUsername: true, Username: user, HasPassword: true, Password: []byte(password)}
}

// sessionState is everything the store holds for client's durable
// session, every row of every table of sessionTables, in a stable
// order: what a CONNECT that touched the session would change.
func sessionState(t *testing.T, db *sql.DB, client string) string {
	t.Helper()
	id := sessionID(t, db, client)
	state := make([]string, 0, len(sessionTables)) // a row a table, at least, for a session with a subscription
	for _, table := range sessionTables {
		column := "session"
		if table == "sessions" {
			column = "id"
		}
		rows, err := db.QueryContext(t.Context(), `SELECT * FROM `+table+` WHERE `+column+` = ?`, id)
		if err != nil {
			t.Fatalf("reading %s of session %d: %v", table, id, err)
		}
		names, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		values := make([]any, len(names))
		into := make([]any, len(names))
		for i := range values {
			into[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(into...); err != nil {
				t.Fatal(err)
			}
			state = append(state, fmt.Sprintf("%s %v %v", table, names, values))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close() // read whole
	}
	slices.Sort(state)
	return strings.Join(state, "\n")
}

// config.md, broker.mqtt: allow_anonymous "admits a client with no user
// name", default "false with one" password_file; design.md, CONNECT's
// refusal table: 0x86 "a wrong or missing credential", MQTT 3.1.1 return
// code 4; Flows, CONNECT: "2. Check. The broker checks password [...]. A
// failure gets a CONNACK with its reason, then the connection closes",
// before "4. Load. Unless the id is connected, the core reads its session
// from the store", "5. Take over" and "6. Decide. Clean start ends an
// existing session"; MQTT 5 3.1.4: the Server "SHOULD perform
// authentication and authorization checks. If any of these checks fail, it
// MUST close the Network Connection [MQTT-3.1.4-2]", and only "If
// validation is successful" does it take over the existing Client's
// connection and process Clean Start; MQTT 3.1.1 3.1.4 likewise; a
// refusing CONNACK has Session Present 0 (MQTT 5 [MQTT-3.2.2-6], 3.1.1
// [MQTT-3.2.2-4]). Item 0018a (261019 054601, from audit 261018-101a):
// "the existing session of that client id is kept, unread and undeleted;
// the check comes before any session is touched".
//
// With a password file and allow_anonymous left to its default, alice's
// durable session subscribes. A CONNECT with no user name for its client
// id, Clean Start 1, is refused 0x86 and alice's connection is not taken
// over: her PINGREQ is still answered. She goes away; a message is
// published to her filter. Three CONNECTs with no user name for her id are
// then refused, none answered with anything but the CONNACK: MQTT 5 Clean
// Start 0, which would resume the session and be sent the message; MQTT
// 3.1.1 Clean Session 1 (return code 4); MQTT 5 Clean Start 1, which would
// end it. The store's rows of her session are as they were, and she
// resumes it, Session Present 1, and is sent the message on her
// subscription.
func TestRefusedConnectLeavesTheSessionUntouched(t *testing.T) {
	dir := t.TempDir()
	file := passwordFile(t, dir, "passwd", "alice:"+sixHash("alice-pw", salt12))
	b := support.Launch(t, &harness.Options{Login: harness.Login{PasswordFile: file}})
	id := clientID(t, "")
	owner := credentials(id, "alice", "alice-pw")
	owner.Props = support.Durable
	c := support.Subscribed(t, b.Addr, harness.V5, owner, []harness.Subscription{{Filter: "owned/t", QoS: 1}})

	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true}, 0x86)
	if err := c.Ping(); err != nil {
		t.Fatalf("the session's own client after a refused CONNECT for its id: %v; want its connection kept", err)
	}
	support.Away(t, c)
	publisher, _ := harness.Connected(t, b.Addr, harness.V5, credentials(id+"-publisher", "alice", "alice-pw"))
	if ack, err := publisher.Publish(&harness.Publish{QoS: 1, Topic: "owned/t", Payload: []byte("while away")}); err != nil || ack.Reason != 0 {
		t.Fatalf("publishing to the away session's filter: %v %+v", err, ack)
	}
	db := support.OpenDB(t, b.DB)
	before := sessionState(t, db, id)

	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, Props: support.Durable}, 0x86)
	connackRefuses(t, b.Addr, harness.V311, &harness.Connect{ClientID: id, CleanStart: true}, 4)
	connackRefuses(t, b.Addr, harness.V5, &harness.Connect{ClientID: id, CleanStart: true}, 0x86)
	if after := sessionState(t, db, id); after != before {
		t.Errorf("refused CONNECTs changed the stored session:\nbefore %s\nafter  %s", before, after)
	}
	r := resumed(t, b.Addr, harness.V5, owner)
	if p := harness.Expect[*harness.Publish](t, r); p.Topic != "owned/t" || string(p.Payload) != "while away" {
		t.Errorf("the resumed session is sent %q %q; want the message published while it was away", p.Topic, p.Payload)
	}
}

// metrics.md, Connections and sessions: saguin_connections_refused_total,
// "the CONNACK's reason (design.md, CONNECT): [...]
// bad_user_name_or_password"; design.md, CONNECT's refusal table: 0x86, "a
// wrong or missing credential". MQTT 5 Table 2-6: 0x86 is "Bad User Name or
// Password".
//
// metrics.md, Rules: "Counters start at 0".
//
// With a password file holding alice, a CONNECT naming alice with another
// password and one naming a user the file does not hold are each refused
// 0x86, and each counts once as bad_user_name_or_password on the new
// broker's /metrics.
func TestBadCredentialsAreRefusedAndCounted(t *testing.T) {
	dir := t.TempDir()
	file := passwordFile(t, dir, "passwd", "alice:"+sixHash("alice-pw", salt12))
	b := support.Launch(t, &harness.Options{Operations: true, Login: harness.Login{PasswordFile: file}})
	connackRefuses(t, b.Addr, harness.V5, credentials(clientID(t, "-wrong"), "alice", "not-alice-pw"), 0x86)
	connackRefuses(t, b.Addr, harness.V5, credentials(clientID(t, "-unknown"), "mallory", "alice-pw"), 0x86)
	support.AwaitReady(t, b)
	awaitSeries(t, b, map[string]float64{refusedSeries("bad_user_name_or_password"): 2})
}

// config.md, broker.mqtt, password_file: "the hash is
// $7$<iterations>$<salt>$<hash>, PBKDF2-HMAC-SHA512 with a 64-byte output,
// or $6$<salt>$<hash>, SHA-512 of the password then the salt; salt and hash
// in standard base64"; storage.md, sessions: user, "the proved user name";
// design.md, Access and reload: "Otherwise the user name is the password
// user".
//
// A file holds a $7$ entry, its iterations 4096 (the count it names, not
// --passwd's 1,000) and its salt 64 bytes, and a $6$ entry, its salt 12
// bytes. Each user connects a durable session with the right password and
// is accepted; the stored session's user is that user name.
func TestRightPasswordIsAcceptedInEitherScheme(t *testing.T) {
	dir := t.TempDir()
	file := passwordFile(t, dir, "passwd",
		"# made by the test from config.md's definitions",
		"seven:"+sevenHash(t, "seven-pw", 4096, salt64),
		"",
		"six:"+sixHash("six-pw", salt12))
	b := support.Launch(t, &harness.Options{Login: harness.Login{PasswordFile: file}})
	db := support.OpenDB(t, b.DB)
	for _, user := range []string{"seven", "six"} {
		t.Run(user, func(t *testing.T) {
			p := credentials(clientID(t, ""), user, user+"-pw")
			p.Props = support.Durable
			harness.Connected(t, b.Addr, harness.V5, p)
			var stored string
			if err := db.QueryRowContext(t.Context(), `SELECT user FROM sessions WHERE client = ?`, p.ClientID).Scan(&stored); err != nil {
				t.Fatalf("reading the session's user: %v", err)
			}
			if stored != user {
				t.Errorf("the session's user is %q; want %q, the user its password proved", stored, user)
			}
		})
	}
}

// config.md, broker.mqtt: allow_anonymous "admits a client with no user
// name"; design.md, CONNECT's refusal table: 0x86, "a wrong or missing
// credential". A user name is a claim the password must prove; admitting
// no user name does not admit a wrong one.
//
// With a password file holding alice and allow_anonymous true, a CONNECT
// with no user name is accepted; one naming alice with another password,
// and one naming a user the file does not hold, are each refused 0x86.
func TestAnonymousAllowedNeverAdmitsAWrongPassword(t *testing.T) {
	dir := t.TempDir()
	file := passwordFile(t, dir, "passwd", "alice:"+sixHash("alice-pw", salt12))
	b := support.Launch(t, &harness.Options{Login: harness.Login{PasswordFile: file, AllowAnonymous: "true"}})
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-anonymous"), CleanStart: true})
	connackRefuses(t, b.Addr, harness.V5, credentials(clientID(t, "-wrong"), "alice", "not-alice-pw"), 0x86)
	connackRefuses(t, b.Addr, harness.V5, credentials(clientID(t, "-unknown"), "mallory", "any"), 0x86)
}

// config.md, A listener: password_file, default "the broker's", "the
// listener's own file"; allow_anonymous, default "the broker's; false with
// the listener's own password_file".
//
// The broker's password file holds carol, with allow_anonymous false.
// Listener tcp1 has its own file holding alice and allow_anonymous true;
// tcp2 its own file holding bob, its allow_anonymous left to its default.
// Alice is accepted on tcp1 and refused 0x86 on tcp2; bob the other way
// round; carol, whom only the broker's file holds, is refused on both; a
// CONNECT with no user name is accepted on tcp1 and refused 0x86 on tcp2.
func TestListenersOwnPasswordFileOverridesTheBrokers(t *testing.T) {
	dir := t.TempDir()
	brokers := passwordFile(t, dir, "broker.passwd", "carol:"+sixHash("carol-pw", salt12))
	first := passwordFile(t, dir, "tcp1.passwd", "alice:"+sixHash("alice-pw", salt12))
	second := passwordFile(t, dir, "tcp2.passwd", "bob:"+sevenHash(t, "bob-pw", 1000, salt64))
	b := support.Launch(t, &harness.Options{TCPListeners: 2,
		Login:         harness.Login{PasswordFile: brokers, AllowAnonymous: "false"},
		ListenerLogin: []harness.Login{{PasswordFile: first, AllowAnonymous: "true"}, {PasswordFile: second}}})
	tcp1, tcp2 := b.Addrs[0], b.Addrs[1]
	for _, c := range []struct {
		name         string
		connect      func(client string) *harness.Connect
		accepted, on string // the listener that accepts it, "" for none
	}{
		{"alice", func(id string) *harness.Connect { return credentials(id, "alice", "alice-pw") }, tcp1, ""},
		{"bob", func(id string) *harness.Connect { return credentials(id, "bob", "bob-pw") }, tcp2, ""},
		{"carol", func(id string) *harness.Connect { return credentials(id, "carol", "carol-pw") }, "", ""},
		{"no user name", func(id string) *harness.Connect { return &harness.Connect{ClientID: id, CleanStart: true} }, tcp1, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			for i, addr := range []string{tcp1, tcp2} {
				p := c.connect(clientID(t, "-tcp"+strconv.Itoa(i+1)))
				if addr == c.accepted {
					harness.Connected(t, addr, harness.V5, p)
				} else {
					connackRefuses(t, addr, harness.V5, p, 0x86)
				}
			}
		})
	}
}

// config.md, broker.mqtt, password_file: "a user name is 1 to 65,535 bytes
// of valid UTF-8 with no control character, holds no :, does not start with
// # and has no leading or trailing space"; "any other scheme, a line
// breaking this form (a carriage return or a user name outside those rules
// included), or a user named on two lines, is refused, naming the file and
// the lines"; design.md,
// Operations, Who may call: "A third field in a client password file also
// stops startup"; Restart: "The exit status is [...] 1 after [...] a
// refused start"; Testing: "An operator observes the command line, exit
// status, log, files on disk".
//
// Each case writes a client password file with one fault, among comments,
// a blank line and good entries, and starts the broker on it: the start is
// refused, exit status 1, and a line of what it prints names the file and
// every line of the fault (a user named on two lines: both).
func TestStartRefusesABadPasswordFile(t *testing.T) {
	good := "alice:" + sixHash("alice-pw", salt12)
	for _, c := range []struct {
		name  string
		lines []string
		fault []int // the lines a refusal names, from 1
	}{
		{"an unknown hash scheme", []string{"# clients", good, "", "# bob next",
			"bob:$5$" + b64.EncodeToString(salt12) + "$" + b64.EncodeToString(make([]byte, 32))}, []int{5}},
		{"a user on two lines", []string{"# clients", "", good, "bob:" + sixHash("bob-pw", salt12), "",
			"# alice again", "alice:" + sevenHash(t, "other-pw", 1000, salt64)}, []int{3, 7}},
		{"a third field", []string{"# clients", "", "# a scope, as an operators' file has", good + ":/metrics"}, []int{4}},
		{"a user name ending in a space", []string{"# clients", good, "", "bob :" + sixHash("bob-pw", salt12)}, []int{4}},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			file := passwordFile(t, dir, "passwd", c.lines...)
			config, err := harness.WriteConfig(&harness.Options{Dir: dir, WriteTimeout: support.WriteTimeout, Login: harness.Login{PasswordFile: file}})
			if err != nil {
				t.Fatalf("writing the configuration: %v", err)
			}
			code, _, stderr := execute(t, support.BrokerBinary, "--config", config)
			if code != 1 {
				t.Errorf("exit %d; want the start refused, exit 1", code)
			}
			if !namesFileAndLines(stderr, file, c.fault) {
				t.Errorf("no line of what the refused start printed names %s and its lines %v:\n%s", file, c.fault, stderr)
			}
		})
	}
}

// numberPattern finds the decimal numbers in a message.
var numberPattern = regexp.MustCompile(`\d+`)

// namesFileAndLines reports whether a line of printed names file and each
// number of lines. The time a log line starts with (design.md, Logging:
// "the time the event happened, in UTC with milliseconds") and the file's
// own name are no part of what it names.
func namesFileAndLines(printed, file string, lines []int) bool {
	for line := range strings.Lines(printed) {
		if !strings.Contains(line, file) {
			continue
		}
		if f := strings.SplitN(line, " ", 2); len(f) == 2 {
			if _, err := time.Parse(time.RFC3339, f[0]); err == nil {
				line = f[1]
			}
		}
		named := numberPattern.FindAllString(strings.ReplaceAll(line, file, ""), -1)
		if !slices.ContainsFunc(lines, func(n int) bool { return !slices.Contains(named, strconv.Itoa(n)) }) {
			return true
		}
	}
	return false
}

// sevenEntry checks line, a password file's line, against config.md's $7$
// form for user and password, as --passwd add writes it (design.md,
// Operations: "writes $7$ with 1,000 iterations and a 64-byte salt"): user,
// then $7$1000$, a salt of 64 bytes and the PBKDF2-HMAC-SHA512 output of
// password over it, 64 bytes, each in standard base64.
func sevenEntry(t *testing.T, line, user, password string) {
	t.Helper()
	name, hash, _ := strings.Cut(line, ":")
	fields := strings.Split(hash, "$")
	if name != user || len(fields) != 5 || fields[0] != "" || fields[1] != "7" || fields[2] != "1000" {
		t.Fatalf("the line %q is not %s:$7$1000$<salt>$<hash>", line, user)
	}
	salt, err := b64.Strict().DecodeString(fields[3])
	if err != nil || len(salt) != 64 {
		t.Fatalf("the line %q has salt %q: %d bytes, %v; want 64 bytes in standard base64", line, fields[3], len(salt), err)
	}
	if want := sevenHash(t, password, 1000, salt); hash != want {
		t.Fatalf("the line %q holds hash %q; want %q, PBKDF2-HMAC-SHA512 of the password over its salt", line, hash, want)
	}
}

// entries returns the lines of the password file at path, each without its
// line feed.
func entries(t *testing.T, path string) []string {
	t.Helper()
	return strings.Split(strings.TrimSuffix(readFile(t, path), "\n"), "\n")
}

// absentFile fails the test unless nothing exists at path: a refused
// command made no file.
func absentFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Errorf("%s exists after a refused command (%v); want no file made", path, err)
	}
	if left, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "*")); len(left) != 0 {
		t.Errorf("the folder holds %v after a refused command; want nothing", left)
	}
}
