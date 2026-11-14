package main

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The run's reloads (design.md, Access and reload): now and then, amid the
// clients' traffic, the operator of a broker the run started changes the
// files of its access step, then sends it SIGUSR1. One user's password
// changes in the password file; and, where no PUBLISH of restricted's may
// still reach its reader's check, restricted's write on $broadcast's topics
// of three levels is taken away from it in the acl_file, or given back.
// Now and then the acl_file written is broken, a user entry naming a role
// that is not defined, and the step is refused: the broker keeps the old
// copy of both files ("Each step is all-or-nothing. An invalid file is
// logged and the old one kept"), which the run writes back. The run waits
// until /metrics has counted each step once, and a refused one has logged
// one ERROR line naming the file, the entry and what is wrong ("Each step
// is logged and counted once"; "A step refused is one ERROR line naming the
// file, the entry and what is wrong"). The model then follows the step
// applied: every later CONNECT is predicted against the new passwords, the
// one replaced now a wrong password, refused with 0x86 ("An MQTT client's
// password is checked against the new password file from its next
// CONNECT"), while connections open across the reload stay; every later
// PUBLISH, CONNECT Will and Will that goes against the new rules ("Every
// check after the swap uses the new acl_file"). A Will whose connection
// ended before the swap, and which the run has not yet seen go, may have
// been checked on either side of it: where the reload changed its topic
// for its user, it may be dropped or published, and is owed to no one.
// Left out: read rules and features a reload changes, whose deliveries
// already handed to writers and held subscriptions the model does not
// follow; the conformance tests check them.

// reloads is what the run needs to reload the broker it started, and what
// its reloads did, for its last lines.
type reloads struct {
	dir    string          // the broker's folder, holding its password file and acl_file
	broker *harness.Broker // signalled, its /metrics and log read
	// formers are the passwords reloads replaced, by user: wrong ones now.
	formers map[string]string
	// applied and refused count the reloads by their access step's result,
	// toggles those applied that changed restricted's rights.
	applied, refused, toggles int
	// logins and rights are the model's before the run's reloads changed
	// it (restore).
	logins []login
	rights []grant
}

// newReloads reloads b, the broker the run started in dir.
func newReloads(dir string, b *harness.Broker) *reloads {
	return &reloads{dir: dir, broker: b, formers: map[string]string{}, logins: slices.Clone(logins), rights: slices.Clone(rights)}
}

// restore gives logins and rights back what they held before the run's
// reloads, so that a later run in the same process, a test's, starts from
// the model its files are written from (writePasswords, writeACL).
func (r *reloads) restore() {
	copy(logins, r.logins)
	copy(rights, r.rights)
}

// former is the password of user's a reload replaced, if any; none where r
// is nil, the run given a broker with -addr.
func (r *reloads) former(user string) (string, bool) {
	if r == nil {
		return "", false
	}
	p, ok := r.formers[user]
	return p, ok
}

// toggled is the index in rights of restricted's grant a reload takes away
// and gives back: write on $broadcast's topics of three levels, which the
// run's PUBLISHes and Wills often name.
var toggled = slices.IndexFunc(rights, func(g grant) bool { return g.channel == "" && g.topic == "+/+/+" })

// brokenEntry is the user entry a broken acl_file ends with: design.md,
// Access and reload, "The broker refuses a file that holds any of the
// mistakes below": "a user entry with no roles, or one naming a role that
// is not defined".
const (
	brokenUser  = "zed"
	brokenRole  = "nowhere"
	brokenEntry = "  " + brokenUser + ": [" + brokenRole + "]\n"
)

// reloadParts are saguin_reloads_total's parts, one per step (metrics.md,
// Process: "part is log_and_origins, access (password files and acl_file)
// or tls").
var reloadParts = []string{"log_and_origins", "access", "tls"}

// reload: the operator changes a password, and, now and then where
// restrictedSettled allows it, restricted's toggled grant, once every
// PUBLISH of restricted's is checked (restrictedChecked); writes the files,
// the acl_file now and then broken; and sends the broker SIGUSR1. The
// model then follows what the broker took.
func (f *fuzzer) reload() (bool, error) {
	r := f.reloads
	if r == nil {
		return false, nil
	}
	i := f.rng.IntN(len(logins))
	was := logins[i]
	logins[i].password = f.newPassword()
	toggle := f.rng.IntN(2) == 0 && f.restrictedSettled()
	if toggle {
		if err := f.restrictedChecked(); err != nil {
			return true, err
		}
	}
	writable := f.restrictedWills()
	if toggle {
		rights[toggled].write = !rights[toggled].write
	}
	broken := f.rng.IntN(8) == 0
	result := "applied"
	if broken {
		result = "refused"
	}
	f.note("operator: %s's password now %q, %s's write on %q %v (changed %v), the acl_file broken %v; SIGUSR1: want each step counted once, access %s",
		was.user, logins[i].password, restricted, rights[toggled].topic, rights[toggled].write, toggle, broken, result)
	if err := r.write(broken); err != nil {
		return true, err
	}
	logged := len(r.broker.Log())
	if err := r.reloaded(f.timeout, result); err != nil {
		return true, err
	}
	if broken {
		// All or nothing: the broker kept both old files, which go back on
		// disk, so a restart reads what the broker holds.
		logins[i] = was
		if toggle {
			rights[toggled].write = !rights[toggled].write
		}
		r.refused++
		return true, errors.Join(r.refusalLogged(logged, f.timeout), r.write(false))
	}
	r.applied++
	r.formers[was.user] = was.password
	if toggle {
		r.toggles++
		f.unsettleWills(writable)
	}
	return true, nil
}

// newPassword draws a password no user of the password file has: a letter
// and a digit, two characters as every login's, so that none is another's
// or a prefix of another's (logins).
func (f *fuzzer) newPassword() string {
	for {
		p := f.word(1, 1) + strconv.Itoa(f.rng.IntN(10))
		if !slices.ContainsFunc(logins, func(l login) bool { return l.password == p }) {
			return p
		}
	}
}

// restrictedSettled reports whether none of restricted's sessions holds a
// QoS 2 exchange, whose PUBLISH a reconnect or a repeat sends again: the
// model predicts a PUBLISH when it is first sent, while one a reader checks
// after the swap is checked against the new rules (design.md, Access and
// reload: "Every check after the swap uses the new acl_file").
func (f *fuzzer) restrictedSettled() bool {
	return !slices.ContainsFunc(f.slots, func(s *slot) bool { return s.bound && len(s.held) > 0 })
}

// restrictedChecked returns once every PUBLISH of restricted's the run has
// sent has been checked against the rules it was predicted from: each of
// its open connections answers a PINGREQ, which its reader reads after
// them (pingPast), and the reader of every connection the run has ended
// has stopped reading (socketsSettled).
func (f *fuzzer) restrictedChecked() error {
	for _, s := range f.slots {
		if s.bound && s.conn != nil {
			if err := pingPast(s); err != nil {
				return fmt.Errorf("%s: before a reload: %w", s.id, err)
			}
		}
	}
	return f.socketsSettled(f.timeout)
}

// socketsSettled waits, for at most limit, until the broker serves no more
// sockets than the run holds open, so that the reader of every connection
// the run has ended has stopped reading, each PUBLISH it read checked
// (design.md, Flows, Closing a connection: the writer closes only once
// "The reader stops reading and says so"; metrics.md,
// saguin_connections_open, "sockets served, from their permit to their
// close"). A PUBLISH the model predicted, sent before its connection
// ended, is then checked against the rules it was predicted from.
func (f *fuzzer) socketsSettled(limit time.Duration) error {
	open := len(f.held)
	for _, s := range f.slots {
		if s.conn != nil {
			open++
		}
	}
	key := testkit.SeriesKey("saguin_connections_open")
	served := -1.0
	if testkit.Within(limit, func() bool {
		e, err := scrape(f.reloads.broker)
		if err != nil {
			return false
		}
		served = e.Values[key]
		return served <= float64(open)
	}) {
		return nil
	}
	return fmt.Errorf("%v before a reload, the broker serves %v sockets; want at most the %d the run holds open, the others closed", limit, served, open)
}

// restrictedWills snapshots the rights and time before a possible swap for
// each Will of restricted's whose check is still unsettled. A certain drop
// or an observed publication can never change with a reload.
func (f *fuzzer) restrictedWills() map[*will]willAccess {
	ws := map[*will]willAccess{}
	now := time.Now()
	for _, w := range f.wills {
		if w.user == restricted && !w.observed() && !w.dropped && w.settledAt.IsZero() && (w.state == willFired || w.state == willWaiting) {
			ws[w] = willAccess{allowed: may(w.user, w.w.Topic, true), from: now}
		}
	}
	return ws
}

// unsettleWills follows an applied reload that changed restricted's
// rights, writable being restrictedWills before it. The old rights may have
// applied until this observation, the new ones since the request. syncWill
// intersects those intervals with the possible check times; a denial wholly
// before a delayed Will is eligible does not make it uncertain.
func (f *fuzzer) unsettleWills(writable map[*will]willAccess) {
	now := time.Now()
	for w, was := range writable {
		allowed := may(w.user, w.w.Topic, true)
		if was.allowed == allowed || w.observed() || w.dropped || !w.settledAt.IsZero() {
			continue
		}
		w.access[len(w.access)-1].until = now
		w.access = append(w.access, willAccess{allowed: allowed, from: was.from})
		f.syncWill(w, w.uncertain, f.willTime(now))
	}
}

// write writes the run's password file and acl_file in the broker's folder
// from the model, the acl_file ending with brokenEntry where broken.
func (r *reloads) write(broken bool) error {
	if err := writePasswords(r.dir); err != nil {
		return err
	}
	if err := writeACL(r.dir); err != nil {
		return err
	}
	if !broken {
		return nil
	}
	file, err := os.OpenFile(filepath.Join(r.dir, aclFileName), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = file.WriteString(brokenEntry)
	return errors.Join(err, file.Close())
}

// reloaded sends the broker SIGUSR1 and waits, for at most limit, until
// its /metrics has counted each step of it once (design.md, Access and
// reload: "Each step is logged and counted once"): the access step as
// access says, applied or refused, and the others, whose files are as they
// were, applied.
func (r *reloads) reloaded(limit time.Duration, access string) error {
	before, err := scrape(r.broker)
	if err != nil {
		return err
	}
	want := make(map[string]float64, 2*len(reloadParts))
	for _, part := range reloadParts {
		for _, result := range []string{"applied", "refused"} {
			key := testkit.SeriesKey("saguin_reloads_total", "part", part, "result", result)
			v, ok := before.Values[key]
			if !ok {
				return fmt.Errorf("/metrics has no %s (metrics.md, Rules: \"Every series exists from start\")", key)
			}
			want[key] = v
		}
		result := "applied"
		if part == "access" {
			result = access
		}
		want[testkit.SeriesKey("saguin_reloads_total", "part", part, "result", result)]++
	}
	if err := r.broker.Signal(syscall.SIGUSR1); err != nil {
		return fmt.Errorf("SIGUSR1: %w", err)
	}
	got := map[string]float64{}
	if testkit.Within(limit, func() bool {
		e, err := scrape(r.broker)
		if err != nil {
			return false
		}
		for key := range want {
			got[key] = e.Values[key]
		}
		return maps.Equal(got, want)
	}) {
		return nil
	}
	return fmt.Errorf("%v after SIGUSR1, /metrics counts %v; want %v: each step counted once, access %s (design.md, Access and reload)", limit, got, want, access)
}

// refusalLogged waits, for at most limit, until the broker's log written
// since offset holds an ERROR line naming the acl_file, and returns an
// error unless it holds one alone, naming the entry brokenEntry adds and
// the role it names (design.md, Access and reload: "A step refused is one
// ERROR line naming the file, the entry and what is wrong").
func (r *reloads) refusalLogged(offset int, limit time.Duration) error {
	path := filepath.Join(r.dir, aclFileName)
	var lines []string
	testkit.Within(limit, func() bool {
		lines = testkit.LinesNaming(r.broker.Log()[offset:], "ERROR", path)
		return len(lines) > 0
	})
	if len(lines) != 1 || !strings.Contains(lines[0], brokenUser) || !strings.Contains(lines[0], brokenRole) {
		return fmt.Errorf("the refused reload's ERROR lines naming %s: %q; want one, naming the entry %s and the role %s", path, lines, brokenUser, brokenRole)
	}
	return nil
}
