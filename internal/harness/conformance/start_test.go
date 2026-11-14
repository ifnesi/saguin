package conformance

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// startLimit bounds a command that ends at its start: --version, or a start
// refused by the checks that come before the store opens or any listener
// starts (design.md, Restart, Start).
const startLimit = 10 * time.Second

// execute runs a command to its end, within startLimit, and returns its exit
// code and what it printed on each stream. One still running then is killed
// and fails the test.
func execute(t *testing.T, name string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), startLimit)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("%s %s still ran after %v:\n%s%s", name, strings.Join(args, " "), startLimit, out.String(), errOut.String())
	}
	if _, exited := errors.AsType[*exec.ExitError](err); err != nil && !exited {
		t.Fatalf("%s: %v", name, err)
	}
	return cmd.ProcessState.ExitCode(), out.String(), errOut.String()
}

// design.md, Operations: "saguin --version | prints [...] the version: the
// version recorded in the binary, else the commit it was built from, else
// (devel)" (decided from item 0008a). The oracle reads the same build
// information from the shipped binary's file, from outside.
func TestVersionIsTheBuilds(t *testing.T) {
	info, err := buildinfo.ReadFile(support.BrokerBinary)
	if err != nil {
		t.Fatal(err)
	}
	want := info.Main.Version
	if want == "" || want == "(devel)" {
		i := slices.IndexFunc(info.Settings, func(s debug.BuildSetting) bool { return s.Key == "vcs.revision" })
		if i < 0 {
			t.Fatalf("the build stamped neither a version nor a commit: %v", info)
		}
		want = info.Settings[i].Value
	}
	code, stdout, stderr := execute(t, support.BrokerBinary, "--version")
	fields := strings.Fields(stdout)
	if code != 0 || stderr != "" || strings.Count(stdout, "\n") != 1 || len(fields) == 0 || fields[len(fields)-1] != want {
		t.Errorf("exit %d, stdout %q, stderr %q; want 0 and one line ending in the build's version %q", code, stdout, stderr, want)
	}
}

// The licences --licenses prints, from this package's directory: Sagüin's
// own, at the repository's root, and the third-party notices the binary
// embeds (0020a.md, 261028 084426 and 084443).
const (
	licenceFile = "../../../LICENSE"
	noticesFile = "../../../cmd/saguin/THIRD-PARTY-NOTICES.md"
)

// endOfModules ends the notices' list of the modules inside, one "- " line
// each, its path and version, a replaced module's followed by " => " and its
// replacement's path and version.
const endOfModules = "<!-- end of module list -->"

// design.md, Operations: "saguin --licenses [...] | prints the licences of
// the code inside"; 0020a.md, 261028 084443: "Sagüin is Apache-2.0 [...]
// --licenses prints it first, before the third-party notices"; 261028
// 084426: "--licenses prints one generated file,
// cmd/saguin/THIRD-PARTY-NOTICES.md, embedded in the binary; a make target
// gathers it from the modules that reach the binary, and a test fails when
// it falls out of date".
//
// 261028 094143 (maintainer): the notices "also carry Go's licence,
// SQLite's public-domain notice and glibc's LGPL (glibc is statically
// linked), with a line that the complete source is available at the
// repository".
//
// The shipped binary's --licenses exits 0 and prints LICENSE, then, after
// any blank lines, the notices file as it is in the repository. The
// notices' module list (endOfModules) names exactly the modules the
// binary's own build information records as linked into it, and each has a
// section of its own, headed "## " and its list line, holding its licence.
// After the list, three more sections, each headed "## " and the name of
// what it covers, alone or followed by a space and more, hold: Go's, its
// toolchain's LICENSE as it is; SQLite's, its public-domain notice; and
// glibc's, the GNU Lesser General Public License, version 2.1. A line of
// the notices says the complete source is at the repository: it names the
// source and the binary's own module path.
func TestLicensesNameEveryModuleInside(t *testing.T) {
	licence, err := os.ReadFile(licenceFile)
	if err != nil {
		t.Fatal(err)
	}
	notices, err := os.ReadFile(noticesFile)
	if err != nil {
		t.Fatalf("the notices the binary embeds: %v", err)
	}
	code, stdout, stderr := execute(t, support.BrokerBinary, "--licenses")
	rest, first := strings.CutPrefix(stdout, string(licence))
	switch {
	case code != 0 || stderr != "":
		t.Errorf("--licenses: exit %d, stderr %q; want 0 and nothing", code, stderr)
	case !first:
		t.Errorf("--licenses does not begin with %s; it begins %.200q", licenceFile, stdout)
	case strings.TrimLeft(rest, "\n") != string(notices):
		t.Errorf("--licenses, after %s, does not print %s as it is: run the notices' make target and rebuild", licenceFile, noticesFile)
	}

	info, err := buildinfo.ReadFile(support.BrokerBinary)
	if err != nil {
		t.Fatal(err)
	}
	inside := make([]string, 0, len(info.Deps))
	for _, m := range info.Deps {
		entry := m.Path + " " + m.Version
		if r := m.Replace; r != nil {
			entry += " => " + r.Path + " " + r.Version
		}
		inside = append(inside, entry)
	}
	head, body, ok := strings.Cut(string(notices), endOfModules)
	if !ok {
		t.Fatalf("%s has no module list ending %q", noticesFile, endOfModules)
	}
	var listed []string
	for line := range strings.Lines(head) {
		if entry, ok := strings.CutPrefix(strings.TrimSpace(line), "- "); ok {
			listed = append(listed, entry)
		}
	}
	slices.Sort(inside)
	slices.Sort(listed)
	if len(inside) == 0 || !slices.Equal(listed, inside) {
		t.Errorf("%s lists the modules %q; the binary holds %q: run the notices' make target", noticesFile, listed, inside)
	}
	for _, entry := range inside {
		section, ok := noticeSection(body, func(heading string) bool { return heading == entry })
		if !ok || strings.TrimSpace(section) == "" {
			t.Errorf("%s has no section \"## %s\" holding its licence", noticesFile, entry)
		}
	}

	code, goroot, stderr := execute(t, "go", "env", "GOROOT")
	if code != 0 {
		t.Fatalf("go env GOROOT: exit %d, %s", code, stderr)
	}
	goLicence, err := os.ReadFile(filepath.Join(strings.TrimSpace(goroot), "LICENSE"))
	if err != nil {
		t.Fatal(err)
	}
	for _, linked := range []struct {
		name, what string
		holds      []string
	}{
		{"Go", "its toolchain's LICENSE", []string{strings.TrimSpace(string(goLicence))}},
		{"SQLite", "its public-domain notice", []string{"The author disclaims copyright to this source code."}},
		{"glibc", "the LGPL, version 2.1", []string{"GNU LESSER GENERAL PUBLIC LICENSE", "Version 2.1, February 1999"}},
	} {
		section, ok := noticeSection(body, func(heading string) bool {
			return heading == linked.name || strings.HasPrefix(heading, linked.name+" ")
		})
		if !ok {
			t.Errorf("%s has no section \"## %s\" after its module list", noticesFile, linked.name)
			continue
		}
		for _, text := range linked.holds {
			if !strings.Contains(section, text) {
				t.Errorf("%s's section \"## %s\" does not hold %s: %.80q", noticesFile, linked.name, linked.what, text)
			}
		}
	}
	if !slices.ContainsFunc(slices.Collect(strings.Lines(string(notices))), func(line string) bool {
		return strings.Contains(strings.ToLower(line), "source") && strings.Contains(line, info.Main.Path)
	}) {
		t.Errorf("%s has no line saying the complete source is at the repository, %s", noticesFile, info.Main.Path)
	}
}

// noticeSection returns the text of the first section of body, the notices
// after their module list, whose "## " heading line matches, up to the
// next heading of that level, and whether there is one.
func noticeSection(body string, match func(heading string) bool) (string, bool) {
	for _, chunk := range strings.Split(body, "\n## ")[1:] {
		heading, section, _ := strings.Cut(chunk, "\n")
		if match(heading) {
			return section, true
		}
	}
	return "", false
}

// design.md, Restart, Start: "startup checks [...] that the process's
// open-file limit covers limits.max_connections, one more for each MQTT
// listener, the operations listener's connections ([Operations, HTTP](#http)),
// and the files Sagüin opens besides,
// including the store's, the listeners' and pid_file. The requirement
// includes simultaneous bridge links and establishment attempts, the working
// database's connections, their files and sidecars, and, while a snapshot is
// written, memory mode's snapshot file and the file it copies from. It also
// includes a reserve of 32 descriptors for runtime and temporary startup or
// reload uses. If the limit is insufficient, startup is refused, reporting
// the calculated requirement and the available limit. Startup then takes the
// storage lock (Store)"; Store: "It runs in WAL mode"; "the running broker
// holds a live exclusive lock on <file_path>.lock beside the database";
// memory mode: the broker "copies its file, byte for byte, to <id>.db in
// memory_dir"; "Startup first takes a live exclusive lock on <id>.db.lock
// beside it"; "Startup closes every connection it opened to the disk
// database and runs on the RAM copy"; Restart: "The exit status is [...] 1
// after [...] a refused start".
//
// The harness's configuration sets max_connections to its default, 10000
// (config.md), one MQTT listener, no operations listener, no bridge and no
// pid_file. The least the list above then requires is least: max_connections,
// one more held for the MQTT listener, the listener's own socket, the
// working database's file with its -wal and -shm sidecars, which the store
// keeps open in WAL mode, the storage lock and the reserve of 32, 10038, in
// full and normal mode alike; in memory mode, memory_dir on a tmpfs of its
// own, the working database is the RAM copy, with as many files, and the
// RAM copy's lock, the snapshot file and the file it copies from come on
// top, 10041. It is a lower bound: SQLite's per-connection files may add
// more. With an open-file limit one below it, set by the shell the binary is
// started from, the start is refused, naming the limit and a requirement no
// lower than least, the largest number it names. Whatever else is counted,
// memory mode's requirement is at least full mode's and those three files.
func TestStartRefusesALowOpenFileLimit(t *testing.T) {
	const fullLeast = 10000 + 1 + 1 + 3 + 1 + 32
	const memoryOnly = 3 // the RAM copy's lock, the snapshot file and the file it copies from
	ram := support.MemoryDir(t, support.DiskSize)
	if ram == "" {
		return
	}
	required := make(map[string]int) // the requirement each mode's refusal names
	for _, tc := range []struct {
		mode  string
		least int
	}{
		{"full", fullLeast},
		{"normal", fullLeast},
		{"memory", fullLeast + memoryOnly},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir(), StorageMode: tc.mode}
			if tc.mode == "memory" {
				o.MemoryDir = ram
			}
			required[tc.mode] = openFileRequirement(t, o, tc.least-1, tc.least, ram)
		})
	}
	if full, memory := required["full"], required["memory"]; full == 0 || memory < full+memoryOnly {
		t.Errorf("the requirement named in full mode is %d, in memory mode %d; want memory mode's at least full mode's and %d more: the RAM copy's lock, the snapshot file and the file it copies from",
			full, memory, memoryOnly)
	}
}

// openFileRequirement starts the broker on o's configuration, written in
// o.Dir, under an open-file limit of limit, set by the shell the binary is
// started from, and returns the requirement its refusal names: the largest
// number it names that is at least least. It fails the test unless the
// start is refused, exit 1, naming limit and such a requirement, and then
// returns 0. The names of o.Dir and of each folder of others may hold
// digits; the numbers named are the rest.
func openFileRequirement(t *testing.T, o *harness.Options, limit, least int, others ...string) int {
	t.Helper()
	config, err := harness.WriteConfig(o)
	if err != nil {
		t.Fatalf("writing the configuration: %v", err)
	}
	// ulimit -n sets the hard limit too, so Go's start-up, which raises the
	// soft limit to the hard one, cannot lift it again.
	code, _, stderr := execute(t, "sh", "-c", `ulimit -n "$0" && exec "$1" --config "$2"`, strconv.Itoa(limit), support.BrokerBinary, config)
	if code != 1 {
		t.Errorf("exit %d with an open-file limit of %d; want the start refused, exit 1", code, limit)
	}
	rest := strings.ReplaceAll(stderr, o.Dir, "")
	for _, dir := range others {
		rest = strings.ReplaceAll(rest, dir, "")
	}
	numbers := regexp.MustCompile(`\d+`).FindAllString(rest, -1)
	named, required := false, 0
	for _, s := range numbers {
		n, err := strconv.Atoi(s)
		named = named || (err == nil && n == limit)
		if err == nil && n >= least {
			required = max(required, n)
		}
	}
	if !named || required == 0 {
		t.Errorf("the refusal names %v; want the limit, %d, and the requirement, at least %d:\n%s", numbers, limit, least, stderr)
		return 0
	}
	return required
}

// TestStartCountsTheBridgesInTheOpenFileLimit: design.md, Restart, Start:
// "It checks that the process's open-file limit covers
// limits.max_connections, one more for each MQTT listener, the operations
// listener's connections ([Operations, HTTP](#http)), and the files Sagüin
// opens besides, including the store's,
// the listeners' and pid_file. The requirement includes simultaneous bridge
// links and establishment attempts, the working database's connections,
// their files and sidecars [...]. If the limit is insufficient, startup is
// refused, reporting the calculated requirement and the available limit."
//
// The configuration of TestStartRefusesALowOpenFileLimit in full mode, its
// least 10038, is started under a limit one below it, once with no bridge
// and once with three, each to a peer on port 1883, below 10038, so that no
// number the refusal names comes from it: each bridge has at least one
// link or establishment attempt at a time, so the requirement named with
// the three is at least three above the one named without.
func TestStartCountsTheBridgesInTheOpenFileLimit(t *testing.T) {
	const least = 10000 + 1 + 1 + 3 + 1 + 32
	without := openFileRequirement(t, &harness.Options{Dir: t.TempDir()}, least-1, least)
	names := []string{"one", "two", "three"}
	bridges := make([]harness.Bridge, 0, len(names))
	for _, name := range names {
		bridges = append(bridges, harness.Bridge{Name: name, Peer: "tcp://127.0.0.1:1883", ClientID: "bridge-" + name,
			Topics: []harness.BridgeRule{support.In("a/#", 1)}})
	}
	with := openFileRequirement(t, &harness.Options{Dir: t.TempDir(), Bridges: bridges}, least-1, least)
	if without == 0 || with < without+len(bridges) {
		t.Errorf("the requirement named is %d with no bridge and %d with %d; want at least %d more with them: one link or establishment attempt each",
			without, with, len(bridges), len(bridges))
	}
}

// design.md, Store: "the running broker holds a live exclusive lock on
// <file_path>.lock beside the database. The file's existence does not hold
// the lock. [...] A broker that finds the lock held by a live process refuses
// to start, naming the lock"; Restart, Start: "Startup then takes the storage
// lock (Store). It writes pid_file where set, then opens the store"; Restart:
// "The exit status is [...] 1 after [...] a refused start".
//
// A broker serves a durable session's client on a database file, with a
// pid_file. A second broker on the same file_path and pid_file, but on a
// listener address of its own, so that only the lock can refuse it, exits
// with status 1, naming <file_path>.lock. The first broker still answers
// its client's PINGREQ; the database still holds that session as
// connected (storage.md, sessions: left "0 while connected"), which a start
// opening the store would have made away ("A session left connected by a
// crash is away from the start that follows"); and the pid_file still holds
// the first broker's process id.
func TestASecondBrokerIsRefusedByTheLock(t *testing.T) {
	dir := t.TempDir()
	db, pidFile := filepath.Join(dir, "saguin.db"), filepath.Join(dir, "saguin.pid")
	first := support.Launch(t, &harness.Options{Dir: dir, FilePath: db, PIDFile: pidFile})
	c := support.Stored(t, first, "held")

	err := support.Started(t, &harness.Options{FilePath: db, PIDFile: pidFile}).Listening()
	switch exit, ok := errors.AsType[*exec.ExitError](err); {
	case err == nil:
		t.Errorf("a second broker started on %s, which a running broker holds", db)
	case !ok || exit.ExitCode() != 1:
		t.Errorf("the second broker's start: %v; want it refused, exit status 1", err)
	case !strings.Contains(err.Error(), db+".lock"):
		t.Errorf("the refusal does not name the lock %s.lock:\n%v", db, err)
	}

	harness.SendBytes(t, c, pingreq)
	harness.Expect[*harness.Pingresp](t, c)
	var left int64
	if err := support.OpenDB(t, db).QueryRow(`SELECT left FROM sessions WHERE client = 'held'`).Scan(&left); err != nil {
		t.Fatalf("reading the session of the first broker's client: %v", err)
	}
	if left != 0 {
		t.Errorf("the session of the first broker's client, connected, is stored as away since %d", left)
	}
	checkPIDFile(t, pidFile, first)
}

// design.md, Store, The lock: "the running broker holds a live exclusive lock
// on <file_path>.lock beside the database. The file's existence does not hold
// the lock"; "a lock file left by a crash is an ordinary file". A broker is
// killed, and a <file_path>.lock lies beside its database, held by no
// process, as a crash leaves it: a broker starting on that file serves.
func TestAStaleLockFileIsNoLock(t *testing.T) {
	dir := t.TempDir()
	support.Crash(t, support.Launch(t, &harness.Options{Dir: dir}))
	testkit.WriteFile(t, dir, "saguin.db.lock", "") // beside Launch's file_path, Dir/saguin.db
	b := support.Launch(t, &harness.Options{Dir: dir})
	harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "after-the-crash", CleanStart: true})
}

// design.md, Store, The lock: "the running broker holds a live exclusive
// lock on <file_path>.lock beside the database"; "A missing database file
// is created 0600, readable and writable by the broker's user alone,
// whatever the umask, and so is the lock file. SQLite gives the database's
// -wal and -shm the database file's mode. The broker never changes the
// mode, group or ACL of a file that exists, so an operator grants others
// read access through the file's own permissions or ACLs. A memory-mode
// snapshot replaces file_path with a new file that keeps all three, as
// memory mode above describes". Store: "It runs in WAL mode"; "At a stop in
// full or normal mode with no store error, SQLite folds the log into the
// database as the last connection closes. No -wal or -shm file remains";
// memory mode: "This RAM copy is readable and writable by the broker's user
// alone (0600)"; "Every snapshot_interval and at a graceful stop, a
// snapshot copies the RAM copy's database file, byte for byte, to
// file_path.new, beside file_path. It gives that file file_path's mode,
// owning group and access control list (ACL) [...]. It then syncs that
// file, renames it over file_path"; "A snapshot, the final one included, is
// skipped when nothing was committed since the last successful one".
//
// In each mode the design names, full, normal and memory (memory_dir on a
// tmpfs), a broker runs under umask 022, which leaves a new file readable
// by everyone, makes a new database, and acknowledges a QoS 1 publish, so
// memory mode's final snapshot is taken: while it serves, the database and
// its lock file are each a file of mode 0600, and so are the database's
// -wal and -shm in full and normal mode, and memory mode's RAM copy. It
// stops, exit status 0, and the database is still 0600. The operator makes
// the database 0640, and a broker in the same mode starting on it under the
// same umask acknowledges a QoS 1 publish: while it serves, the database is
// 0640, its -wal and -shm are 0640 too in full and normal mode, the lock
// file, which exists, stays 0600, and memory mode's RAM copy is 0600; after
// the stop, exit status 0, the database is 0640 still, in memory mode the
// final snapshot having been renamed over it.
func TestDatabaseFilesKeepTheirModes(t *testing.T) {
	for _, mode := range []string{"full", "normal", "memory"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			db := filepath.Join(dir, "saguin.db")
			o := &harness.Options{Dir: dir, FilePath: db, StorageMode: mode, Under: []string{"/bin/sh", "-c", `umask 022 && exec "$@"`, "sh"}}
			if mode == "memory" {
				if o.MemoryDir = support.MemoryDir(t, support.DiskSize); o.MemoryDir == "" {
					return
				}
			}
			type file struct {
				path string
				want os.FileMode
			}
			modes := func(when string, files ...file) {
				t.Helper()
				for _, f := range files {
					switch info, err := os.Stat(f.path); {
					case err != nil:
						t.Errorf("%s: %v", when, err)
					case !info.Mode().IsRegular() || info.Mode().Perm() != f.want:
						t.Errorf("%s, %s has mode %v; want a file of mode %#o", when, f.path, info.Mode(), f.want)
					}
				}
			}
			// served runs a broker on db, acknowledges a QoS 1 publish, and
			// checks the files' modes while it serves, the database's being
			// database, and the database's again after its stop.
			served := func(when string, database os.FileMode) {
				t.Helper()
				b := support.Launch(t, o)
				pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "modes", CleanStart: true})
				support.Acknowledged(t, pub, support.Sized("modes/record", 64))
				files := []file{{db, database}, {db + ".lock", 0o600}}
				if mode == "memory" {
					files = append(files, file{b.RAM, 0o600})
				} else {
					files = append(files, file{db + "-wal", database}, file{db + "-shm", database})
				}
				modes(when+", serving", files...)
				stopped(t, b)
				modes(when+", after the stop", file{db, database})
			}
			served("with the database new", 0o600)
			if err := os.Chmod(db, 0o640); err != nil {
				t.Fatal(err)
			}
			served("with the database the operator made 0640", 0o640)
		})
	}
}

// design.md, Store, memory mode: "A process can give a file only a group
// its user belongs to. Startup in memory mode is therefore refused when
// file_path's group is not one of the broker's user's groups. The refusal
// names file_path, its group, and the two ways to grant the access
// instead: add the broker's user to that group, or name the group in
// file_path's ACL"; "Example: file_path is in group backup, and the broker
// runs as saguin, which is not a member: the broker does not start";
// Restart: "The exit status is [...] 1 after [...] a refused start".
//
// A broker in full mode makes its database at file_path and stops. The
// file is given a group the test's user does not belong to
// (support.GiveForeignGroup). A broker in memory mode on it, memory_dir on
// the user's runtime tmpfs (support.RuntimeMemoryDir: no user namespace, so
// the group keeps its id), does not start: exit status 1, and the refusal
// names file_path, the group, the broker's user, to be added to it, and the
// ACL, to name it in.
func TestMemoryModeRefusesAGroupItCannotGive(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "saguin.db")
	if err := support.Launch(t, &harness.Options{Dir: dir, FilePath: db}).Stop(); err != nil {
		t.Fatal(err)
	}
	foreign := support.GiveForeignGroup(t, db)
	broker, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	b, err := harness.Launch(support.BrokerBinary, &harness.Options{Dir: dir, FilePath: db, StorageMode: "memory", MemoryDir: support.RuntimeMemoryDir(t)})
	if err == nil {
		_ = b.Signal(os.Kill) // a broker that started is the failure; it ends here
		_, _ = b.Wait(support.StopLimit)
		t.Fatalf("the broker started in memory mode on %s, whose group %v its user %s does not belong to", db, foreign.Names(), broker.Username)
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
		t.Errorf("the refused start: %v; want exit status 1", err)
	}
	refusal := err.Error()
	if !namesAll(refusal, db, broker.Username, "ACL") || !slices.ContainsFunc(foreign.Names(), func(name string) bool { return strings.Contains(refusal, name) }) {
		t.Errorf("the refusal %q does not name each of %s, its group %v, the broker's user %s and the ACL", refusal, db, foreign.Names(), broker.Username)
	}
}

// config.md, Values: "A path Sagüin writes must differ from every other path
// in the configuration. These paths are compared as files at start. They
// include pid_file, file_path with its -wal, -shm and -journal files and its
// lock file, memory_dir, memory mode's RAM copy with its -wal, -shm,
// -journal and lock files, its file_path.new snapshot file"; design.md,
// Store, memory mode: the broker "copies its file, byte for byte, to <id>.db
// in memory_dir, named after broker.id"; "Startup first takes a live
// exclusive lock on <id>.db.lock beside it"; "a snapshot copies the RAM
// copy's database file [...] to file_path.new, beside file_path"; item 0008a
// (261013 110314): "a link to another path [...] naming the same file refuses
// to start, naming both"; design.md, Restart: "The exit status is [...] 1
// after [...] a refused start".
//
// pid_file is a link to a file that one of these paths names, which exists
// before the start, as the database does after an earlier run: file_path;
// and, in memory mode with memory_dir on a tmpfs of its own, the snapshot
// file beside file_path, saguin.db.new, as a crash during a snapshot leaves
// it, and the RAM copy of broker.id harness (harness.WriteConfig),
// harness.db, its -wal, its -shm, its -journal and its lock file,
// harness.db.lock. The start is refused, with exit status 1, naming pid_file
// and the file it links to.
func TestStartRefusesTwoWrittenPathsOneFile(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		memory bool   // the broker in memory mode, memory_dir on a tmpfs of its own
		inRAM  bool   // the file in memory_dir; else in the broker's folder
		file   string // the file pid_file links to, by its name in that folder
	}{
		{"file_path", false, false, "saguin.db"}, // Launch's file_path, Dir/saguin.db
		{"the snapshot file", true, false, "saguin.db.new"},
		{"the RAM copy", true, true, "harness.db"},
		{"the RAM copy's -wal", true, true, "harness.db-wal"},
		{"the RAM copy's -shm", true, true, "harness.db-shm"},
		{"the RAM copy's -journal", true, true, "harness.db-journal"},
		{"the RAM copy's lock file", true, true, "harness.db.lock"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			o := &harness.Options{Dir: dir, PIDFile: filepath.Join(dir, "saguin.pid")}
			folder := dir
			if tc.memory {
				ram := support.MemoryDir(t, support.DiskSize)
				if ram == "" {
					return
				}
				o.StorageMode, o.MemoryDir = "memory", ram
				if tc.inRAM {
					folder = ram
				}
			}
			target := testkit.WriteFile(t, folder, tc.file, "")
			if err := os.Symlink(target, o.PIDFile); err != nil {
				t.Fatal(err)
			}
			b, err := harness.Launch(support.BrokerBinary, o)
			if err == nil {
				_ = b.Signal(os.Kill) // a broker that started is the failure; it ends here
				_, _ = b.Wait(support.StopLimit)
				t.Fatalf("the broker started with pid_file %s a link to %s", o.PIDFile, target)
			}
			if exit, ok := errors.AsType[*exec.ExitError](err); !ok || exit.ExitCode() != 1 {
				t.Errorf("the refused start: %v; want exit status 1", err)
			}
			if !strings.Contains(err.Error(), o.PIDFile) || !strings.Contains(err.Error(), target) {
				t.Errorf("the refusal %q does not name both %s and %s", err, o.PIDFile, target)
			}
		})
	}
}

// design.md, Words, Offset: "The stored counter is raised with every saved
// record and every saved live-start boundary. Startup resumes from that
// counter. A stored counter below a saved record or boundary is a damaged
// file, and the broker refuses to start"; storage.md, channels, next:
// "raised, never lowered, with every saved record and live-start boundary";
// design.md, Store: "A damaged file_path stops the broker. If the damage is
// in the header, schema, version or state the core loads, it stops the broker
// at startup"; Restart: "The exit status is [...] 1 after [...] a refused
// start".
//
// A broker stores a message at offset 1 on $broadcast and a durable
// session subscribing to it, whose start is saved at 2, and stops. Each
// row then damages the file as another program may: lowering $broadcast's
// next to 1, below the record; or deleting the record as well, so that
// next 1 is below the session's saved start with no record above it. A
// broker starting on the file refuses to start, exit status 1, naming the
// file.
func TestStartRefusesACounterBelowSavedState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, damage string }{
		{"below a saved record", `UPDATE channels SET next = 1 WHERE id = 1`},
		{"below a saved subscription start", `DELETE FROM records; UPDATE channels SET next = 1, bytes = 0 WHERE id = 1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := &harness.Options{Dir: t.TempDir()}
			b := support.Launch(t, o)
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
			support.Acknowledged(t, pub, &harness.Publish{QoS: 1, Topic: "counted", Payload: []byte("1")})
			support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: "saved", CleanStart: true, Props: support.Durable}, support.At(1, "counted"))
			stopped(t, b)
			db := support.OpenDB(t, b.DB)
			if starts := storedSubscriptions(t, db, "saved").starts; !slices.Equal(starts, []startRow{{"counted", 1, 2}}) {
				t.Fatalf("subscription_starts holds %+v; want counted on $broadcast (1) at 2, past the record", starts)
			}
			if _, err := db.Exec(tc.damage); err != nil {
				t.Fatalf("damaging the file: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			err := support.Started(t, o).Listening()
			switch exit, ok := errors.AsType[*exec.ExitError](err); {
			case err == nil:
				t.Errorf("the broker started on %s, whose $broadcast next is %s", b.DB, tc.name)
			case !ok || exit.ExitCode() != 1:
				t.Errorf("the start: %v; want it refused, exit status 1", err)
			case !strings.Contains(err.Error(), b.DB):
				t.Errorf("the refusal does not name the file %s:\n%v", b.DB, err)
			}
		})
	}
}

// design.md, Restart, A changed configuration: "A stored channel that is no
// longer configured, or is configured with another type, stops startup,
// naming it. Startup remains refused until the drop-channel command deletes
// it (saguin --drop-channel)"; Restart: "The exit status is [...] 1 after
// [...] a refused start"; Store: "A start that refuses the database file
// leaves it [...] as [it] found [it]".
//
// A broker with two append channels, kept (kept/#) and metering
// (meters/#), stores a record in metering and stops. A broker started on
// the same file without metering, or with metering as latest, or as a
// queue, exits with status 1 before it listens, naming metering. Started
// again with the first configuration, the broker serves, and after its
// stop the record is still in metering.
func TestStartRefusesAChannelRemovedOrRetyped(t *testing.T) {
	dir := t.TempDir()
	kept := harness.Channel{Name: "kept", Type: "append", Filter: "kept/#"}
	metering := harness.Channel{Name: "metering", Type: "append", Filter: "meters/#"}
	b := support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{kept, metering}})
	c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "metered", CleanStart: true})
	support.Acknowledged(t, c, &harness.Publish{QoS: 1, Topic: "meters/1", Payload: []byte("1")})
	stopped(t, b)

	for _, tc := range []struct {
		name     string
		channels []harness.Channel
	}{
		{"removed", []harness.Channel{kept}},
		{"configured as latest", []harness.Channel{kept, {Name: "metering", Type: "latest", Filter: "meters/#"}}},
		{"configured as a queue", []harness.Channel{kept, {Name: "metering", Type: "queue", Filter: "meters/#"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := support.Started(t, &harness.Options{Dir: dir, Channels: tc.channels}).Listening()
			switch exit, ok := errors.AsType[*exec.ExitError](err); {
			case err == nil:
				t.Errorf("the broker started with the stored channel metering %s", tc.name)
			case !ok || exit.ExitCode() != 1:
				t.Errorf("the start: %v; want it refused, exit status 1", err)
			case !strings.Contains(err.Error(), "metering"):
				t.Errorf("the refusal does not name the channel metering:\n%v", err)
			}
		})
	}

	again := support.Launch(t, &harness.Options{Dir: dir, Channels: []harness.Channel{kept, metering}})
	stopped(t, again)
	if rs := records(t, again.DB); len(rs) != 1 || rs[0].name != "metering" || string(rs[0].payload) != "1" {
		t.Errorf("after the refused starts the database holds %+v; want the one record in metering, as stored", rs)
	}
}

// routeFlag is the flag that asks saguin --route for JSON: design.md,
// Operations, "--json | with --acl or --route: one compact JSON object per
// line"; written last, saguin --route <config> <subject> --json, the format
// the maintainer chose for it (0014a, 261027).
const routeFlag = "--json"

// reach is one entry of --route's reaches, nil standing for null.
type reach struct{ channel, typ, why *string }

// routeRow is one row of --route's routing_table.
type routeRow struct{ filter, channel, typ string }

// routeAnswer is the one JSON object saguin --route --json prints.
type routeAnswer struct {
	subject, config, kind string
	reaches               []reach
	table                 []routeRow
}

// routeJSON reads stdout as design.md, Operations, says --route --json
// prints it: one compact JSON object on one line, "{"subject":…,
// "config":…,"kind":…,"reaches":[{"channel":…,"type":…,"why":…}],
// "routing_table":[{"filter":…,"channel":…,"type":…}]}", those keys and
// no others. It fails the test where stdout is not that.
func routeJSON(t *testing.T, stdout string) routeAnswer {
	t.Helper()
	line := compactLine(t, "--route --json", stdout)
	var top map[string]json.RawMessage
	var reaches []map[string]*string
	var rows []map[string]string
	var a routeAnswer
	err := json.Unmarshal([]byte(line), &top)
	keysAre := func(m any, want ...string) bool {
		var got []string
		switch m := m.(type) {
		case map[string]json.RawMessage:
			got = slices.Collect(maps.Keys(m))
		case map[string]*string:
			got = slices.Collect(maps.Keys(m))
		case map[string]string:
			got = slices.Collect(maps.Keys(m))
		}
		slices.Sort(got)
		slices.Sort(want)
		return slices.Equal(got, want)
	}
	for _, field := range []struct {
		key  string
		into any
	}{{"subject", &a.subject}, {"config", &a.config}, {"kind", &a.kind}, {"reaches", &reaches}, {"routing_table", &rows}} {
		if err == nil {
			err = json.Unmarshal(top[field.key], field.into)
		}
	}
	if err != nil || !keysAre(top, "subject", "config", "kind", "reaches", "routing_table") {
		t.Fatalf("--route --json printed %s: %v; want the keys subject, config, kind, reaches and routing_table", line, err)
	}
	for _, r := range reaches {
		if !keysAre(r, "channel", "type", "why") {
			t.Fatalf("--route --json printed a reach %v; want the keys channel, type and why", r)
		}
		a.reaches = append(a.reaches, reach{r["channel"], r["type"], r["why"]})
	}
	for _, r := range rows {
		if !keysAre(r, "filter", "channel", "type") {
			t.Fatalf("--route --json printed a routing_table row %v; want the keys filter, channel and type", r)
		}
		a.table = append(a.table, routeRow{r["filter"], r["channel"], r["type"]})
	}
	return a
}

// compactLine returns the one line stdout holds, failing the test unless
// it is one compact JSON object on one line, ended by a line feed:
// design.md, Operations, "--json | with --acl or --route: one compact JSON
// object per line". command names what printed it, for the message.
func compactLine(t *testing.T, command, stdout string) string {
	t.Helper()
	line, ok := strings.CutSuffix(stdout, "\n")
	var compact bytes.Buffer
	if !ok || !strings.HasPrefix(line, "{") || strings.Contains(line, "\n") ||
		json.Compact(&compact, []byte(line)) != nil || compact.String() != line {
		t.Fatalf("%s printed %q; want one compact JSON object on one line", command, stdout)
	}
	return line
}

// show prints p's string, or null.
func show(p *string) string {
	if p == nil {
		return "null"
	}
	return strconv.Quote(*p)
}

// design.md, Operations: "saguin --route <config> <topic or filter> | says
// which channel a topic lands in, or which channels a filter reaches";
// "--json | with --acl or --route: one compact JSON object per line.
// --route's is {"subject":…,"config":…,"kind":…,"reaches":[{"channel":…,
// "type":…,"why":…}],"routing_table":[{"filter":…,"channel":…,"type":…}]}:
// kind topic or filter (one holding + or #, or under $share/ or
// $saguin/queue/); the routing table every channel filter, {a,b}
// expanded, in the order topics are resolved, most exact first. A topic
// reaches one channel, why that channel's filter as configured, null on
// $broadcast. A filter reaches a line per channel (Words: Channel), why
// replayed on an append channel, current on a latest one, excluded on a
// queue and live on $broadcast, or live-shared under $share/; a worker's
// $saguin/queue/<channel> gives worker, or refused-0x8F where the
// subscription is refused, the channel null when no queue answers to it".
// Words: Channel, the rule landings follows, "A {a,b} level is expanded
// first, with each alternative treated as a written level. A subscription's
// filter reaches the channels its topics land in. Each topic belongs to its
// own channel under that rule. The filter also reaches $broadcast where a
// matching topic lands in no configured channel". config.md, channels.<name>,
// filter: "{a,b} [...] offering alternatives", "{a,b} expanded"; Channels: a
// queue's dead-letter channel, "the append channel <channel>__dlq", its
// filter "the queue's filter with the same prefix", the prefix __dlq/;
// $broadcast, "the reserved append
// channel", which --route shows as the channel $broadcast of type append
// (0014a). SUBSCRIBE: "0x8F for a filter of the wrong shape
// (such as a narrower $saguin/queue filter)".
//
// The configuration holds routedChannels, and channels whose filters are
// written with {a,b} levels, one each of append, latest and queue among
// them (the cases of TestFilterExpansions' old channel rows). With no
// broker running, saguin --route is run, as a process, on each subject,
// as text and as JSON:
//   - JSON: subject and config as given; kind; the routing table holding
//     exactly each channel's filter expanded, each alternative once with its
//     channel and type, and jobs__dlq's __dlq/work/+, of type append; no
//     row before another the rule makes more exact; and the same table for
//     every subject;
//   - a topic of landings, or one of the expanded filters: one reach, its
//     channel, the channel's type, and why the channel's filter as
//     configured, {a,b} unexpanded; a topic no channel claims, $broadcast
//     of type append, why null;
//   - a filter: one reach per channel it reaches, each why as above;
//   - text: exit 0, nothing on standard error, and every channel the JSON
//     reaches named (its format is not the design's to say).
func TestRouteAnswersAsTheBrokerRoutes(t *testing.T) {
	more := []harness.Channel{
		{Name: "sensors", Type: "append", Filter: "sensors/+/temp"},
		{Name: "ships", Type: "append", Filter: "ships/{north,south}/#"},
		{Name: "kv", Type: "latest", Filter: "kv/{a,b,c}"},
		{Name: "grid", Type: "append", Filter: "x/{a,b}/{c,d}"},
		{Name: "back", Type: "append", Filter: "x/{b,a}/+"},
		{Name: "pair", Type: "latest", Filter: "{a,b}"},
		{Name: "three", Type: "append", Filter: "ab/{cd,ef,gh}"},
		{Name: "mid", Type: "append", Filter: "a/{b,c}/d"},
		{Name: "xall", Type: "append", Filter: "x/{a,b}/#"},
		{Name: "lv", Type: "latest", Filter: "lv/{a,b}"},
		{Name: "jobs", Type: "queue", Filter: "work/+"},
	}
	// expanded is each channel's filter with its {a,b} levels expanded.
	expanded := map[string][]string{
		"fleet": {"fleet/#"}, "devices": {"fleet/+"}, "temps": {"fleet/+/temp"}, "north": {"fleet/north/#"},
		"northtemp": {"fleet/north/temp"}, "northroot": {"fleet/north"}, "sides": {"fleet/east/#", "fleet/west/#"},
		"books": {"orders/#", "invoices/#"}, "sensors": {"sensors/+/temp"}, "ships": {"ships/north/#", "ships/south/#"},
		"kv": {"kv/a", "kv/b", "kv/c"}, "grid": {"x/a/c", "x/a/d", "x/b/c", "x/b/d"}, "back": {"x/b/+", "x/a/+"},
		"pair": {"a", "b"}, "three": {"ab/cd", "ab/ef", "ab/gh"}, "mid": {"a/b/d", "a/c/d"},
		"xall": {"x/a/#", "x/b/#"}, "lv": {"lv/a", "lv/b"}, "jobs": {"work/+"},
	}
	channels := slices.Concat(routedChannels, more)
	byName := make(map[string]harness.Channel)
	for _, c := range channels {
		byName[c.Name] = c
	}
	config, err := harness.WriteConfig(&harness.Options{Dir: t.TempDir(), Channels: channels})
	if err != nil {
		t.Fatalf("writing the configuration: %v", err)
	}

	type subject struct {
		subject, kind string
		reaches       map[string]string // why by channel, "" for null; nil for a refused worker's
		exits         bool              // its exit status is the design's to say: 0
	}
	var subjects []subject
	for _, l := range landings {
		why := ""
		if l.channel != "$broadcast" {
			why = "won"
		}
		subjects = append(subjects, subject{l.topic, "topic", map[string]string{l.channel: why}, true})
	}
	for _, c := range more {
		for _, f := range expanded[c.Name] {
			if !strings.ContainsAny(f, "+#") {
				subjects = append(subjects, subject{f, "topic", map[string]string{c.Name: "won"}, true})
			}
		}
	}
	for _, s := range []struct{ topic, channel string }{
		{"sensors/7/temp", "sensors"}, {"sensors/7/hum", "$broadcast"}, {"ships/north/1", "ships"}, {"ships/south", "ships"},
		{"kv/d", "$broadcast"}, {"x/a/q", "back"}, {"x/b/q", "back"}, {"x/a", "xall"}, {"x/b/c/d", "xall"}, {"c", "$broadcast"},
		{"work/1", "jobs"},
	} {
		why := "won"
		if s.channel == "$broadcast" {
			why = ""
		}
		subjects = append(subjects, subject{s.topic, "topic", map[string]string{s.channel: why}, true})
	}
	subjects = append(subjects,
		subject{"fleet/north/#", "filter", map[string]string{"north": "replayed", "northtemp": "replayed", "northroot": "replayed"}, true},
		subject{"fleet/+/temp", "filter", map[string]string{"northtemp": "replayed", "sides": "replayed", "temps": "replayed"}, true},
		subject{"weather/#", "filter", map[string]string{"$broadcast": "live"}, true},
		subject{"kv/+", "filter", map[string]string{"kv": "current", "$broadcast": "live"}, true},
		subject{"x/#", "filter", map[string]string{"grid": "replayed", "back": "replayed", "xall": "replayed", "$broadcast": "live"}, true},
		subject{"work/+", "filter", map[string]string{"jobs": "excluded"}, true},
		subject{"$share/g/fleet/north/#", "filter", map[string]string{"north": "live-shared", "northtemp": "live-shared", "northroot": "live-shared"}, true},
		subject{"$saguin/queue/jobs", "filter", map[string]string{"jobs": "worker"}, true},
		subject{"$saguin/queue/jobs/extra", "filter", nil, false},
		subject{"$saguin/queue/nosuch", "filter", nil, false},
	)

	var table []routeRow
	for _, s := range subjects {
		t.Run(s.subject, func(t *testing.T) {
			code, stdout, stderr := execute(t, support.BrokerBinary, "--route", config, s.subject, routeFlag)
			if s.exits && (code != 0 || stderr != "") {
				t.Fatalf("--route %s: exit %d, stderr %q; want 0 and nothing", routeFlag, code, stderr)
			}
			a := routeJSON(t, stdout)
			if a.subject != s.subject || a.config != config || a.kind != s.kind {
				t.Errorf("subject %q, config %q, kind %q; want %q, %q and %q", a.subject, a.config, a.kind, s.subject, config, s.kind)
			}
			routingTable(t, a.table, byName, expanded)
			if table == nil {
				table = a.table
			} else if !slices.Equal(a.table, table) {
				t.Errorf("the routing table differs from one subject to another:\n%v\n%v", a.table, table)
			}
			switch {
			case s.reaches != nil:
				reaches(t, a.reaches, s.reaches, byName)
			case len(a.reaches) != 1 || a.reaches[0].why == nil || *a.reaches[0].why != "refused-0x8F":
				t.Errorf("reaches %v; want one, why \"refused-0x8F\"", a.reaches)
			case strings.HasSuffix(s.subject, "nosuch") && a.reaches[0].channel != nil:
				t.Errorf("reaches channel %s; want null, no queue answering to it", show(a.reaches[0].channel))
			}

			code, stdout, stderr = execute(t, support.BrokerBinary, "--route", config, s.subject)
			if s.exits && (code != 0 || stderr != "") {
				t.Fatalf("--route: exit %d, stderr %q; want 0 and nothing", code, stderr)
			}
			for channel := range s.reaches {
				if !strings.Contains(stdout, channel) {
					t.Errorf("--route printed %q, which does not name %q, which the subject reaches", stdout, channel)
				}
			}
		})
	}
}

// routingTable fails the test unless table holds exactly each channel of
// byName's expanded filters, each once with its channel and type, and the
// dead-letter channel jobs__dlq's filter, and no row comes after one it is
// more exact than.
func routingTable(t *testing.T, table []routeRow, byName map[string]harness.Channel, expanded map[string][]string) {
	t.Helper()
	want := map[routeRow]bool{{"__dlq/work/+", "jobs__dlq", "append"}: true}
	for name, filters := range expanded {
		for _, f := range filters {
			want[routeRow{f, name, byName[name].Type}] = true
		}
	}
	seen := make(map[routeRow]bool)
	for _, row := range table {
		switch {
		case seen[row]:
			t.Errorf("the routing table holds %+v twice", row)
		case !want[row]:
			t.Errorf("the routing table holds %+v, no channel's filter expanded", row)
		}
		seen[row] = true
	}
	for row := range want {
		if !seen[row] {
			t.Errorf("the routing table lacks %+v", row)
		}
	}
	for i := range table {
		for j := i + 1; j < len(table); j++ {
			if more, _ := harness.MoreExact(table[j].filter, table[i].filter); more {
				t.Errorf("the routing table holds %q (row %d) after %q (row %d), which it spells out more exactly", table[j].filter, j+1, table[i].filter, i+1)
			}
		}
	}
}

// reaches fails the test unless got are want, why by channel, in any
// order, each with its channel's type, $broadcast's append: why "won"
// standing for the channel's filter as configured, and "" for null.
func reaches(t *testing.T, got []reach, want map[string]string, byName map[string]harness.Channel) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("reaches %d channels; want %d: %v", len(got), len(want), want)
	}
	for _, r := range got {
		if r.channel == nil {
			t.Errorf("a reach of channel null, type %s, why %s; want one of %v", show(r.typ), show(r.why), want)
			continue
		}
		why, ok := want[*r.channel]
		c := byName[*r.channel]
		if *r.channel == "$broadcast" {
			c.Type = "append"
		}
		switch {
		case !ok:
			t.Errorf("reaches %q; want only %v", *r.channel, want)
		case r.typ == nil || *r.typ != c.Type:
			t.Errorf("reaches %q of type %s; want %q", *r.channel, show(r.typ), c.Type)
		case why == "" && r.why != nil:
			t.Errorf("reaches %q, why %s; want null", *r.channel, show(r.why))
		case why == "won" && (r.why == nil || *r.why != c.Filter):
			t.Errorf("reaches %q, why %s; want its filter as configured, %q", *r.channel, show(r.why), c.Filter)
		case why != "" && why != "won" && (r.why == nil || *r.why != why):
			t.Errorf("reaches %q, why %s; want %q", *r.channel, show(r.why), why)
		}
	}
}
