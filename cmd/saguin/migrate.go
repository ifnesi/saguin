package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// A migration reads and writes the paths it is given and consults no
// configuration at all.
//
// That is deliberate, and it is what makes it useful when something has
// gone wrong. A configuration is loaded whole or not at all - one unknown
// key is a hard error - so requiring one would mean an operator whose
// configuration had drifted could not run the command that would let them
// fix it. It also lets a directory be converted on another machine, from a
// backup, with no broker anywhere near it.
//
// Nothing is lost by it, because both formats describe themselves. A
// snapshot carries its channel's name and kind, and a queue's file carries
// its dead-letter channel beside it.

// migrateToSQLite converts a directory of snapshots into a new sqlite
// database, and returns the process's exit status.
//
// Which flags were given arrives as arguments rather than being read back
// out of the global flag set, so that the rules below can be tested without
// a test binary's own flags standing in for a broker's.
func migrateToSQLite(args []string, withConfig, withCheck bool) int {
	if withConfig {
		return refuse("--config cannot be given with a migration. A migration reads and writes\n" +
			"the paths you name and consults no configuration; accepting one would imply\n" +
			"that it respects it.")
	}
	if withCheck {
		return refuse("--check-config and --snapshots-to-sqlite cannot be given together: one\n" +
			"validates a configuration and the other writes storage.")
	}
	if len(args) != 2 {
		return refuse(fmt.Sprintf(
			"--snapshots-to-sqlite takes two paths, the snapshot directory to read and the\n"+
				"database to create:\n\n    saguin --snapshots-to-sqlite <dir> <file>\n\nGot %d.", len(args)))
	}
	from, to := args[0], args[1]

	// Before anything is created or locked. Any file at all, not merely a
	// database saguin recognises: writing into something that is already
	// there is how two histories end up under one name, with one offset
	// meaning two different records, and a migration only ever writes a new
	// database.
	if _, err := os.Lstat(to); err == nil {
		return refuse(fmt.Sprintf(
			"%s already exists, and a migration only ever writes a database that does not.\n\n"+
				"Writing into a file that is already there is how two histories end up under one\n"+
				"name, with one offset meaning two different records. Name a path that does not\n"+
				"exist, or move that file aside.", to))
	} else if !os.IsNotExist(err) {
		return refuse(fmt.Sprintf("%s: %v", to, err))
	}

	if st, err := os.Stat(from); err != nil {
		return refuse(fmt.Sprintf("%s: %v", from, err))
	} else if !st.IsDir() {
		return refuse(fmt.Sprintf("%s is not a directory. --snapshots-to-sqlite reads a snapshot\n"+
			"directory, the one a memory provider names as its snapshot_dir.", from))
	}

	// The same lock a broker takes, so a directory that is being served is
	// not read out from underneath the shutdown that is about to rewrite it.
	lock, err := sqlite.Hold(filepath.Join(from, snapshotLockName), from)
	if err != nil {
		return refuse(fmt.Sprintf("cannot read %s while a broker is using it:\n%v\n\n"+
			"Stop saguin before migrating. The migration takes both for itself.", from, err))
	}
	defer func() { _ = lock.Release() }()

	snaps, err := store.NewDir(from, version()).LoadAll()
	if err != nil {
		return refuse(err.Error())
	}
	if len(snaps) == 0 {
		// Nothing is created. An empty database would count as "already
		// exists" and lock the operator out of ever retrying into that path.
		fmt.Printf("%s holds no snapshots. No database was written.\n", from)
		return 0
	}

	fmt.Printf("migrating snapshots in %s\n              into %s\n\n", from, to)

	db, err := sqlite.Open(to, version())
	if err != nil {
		return refuse(err.Error())
	}

	started := time.Now()
	moved, failed, dropped := importAll(db, snaps)
	took := time.Since(started)

	if err := db.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		failed++
	}

	if moved == 0 {
		// Nothing landed, so the file holds an empty schema and no records.
		// Removing it is what lets the operator fix the cause and run the
		// same command again.
		_ = os.Remove(to)
		_ = os.Remove(to + ".lock")
		fmt.Fprintf(os.Stderr, "\nnothing could be migrated; %s was removed so this can be run again\n", to)
		return 1
	}

	fmt.Printf("\nmigrated %d channel(s) from %d snapshot(s) in %s\n", moved, len(snaps)-failed, took.Round(time.Millisecond))
	if dropped > 0 {
		fmt.Printf("%d consumer position(s) were dropped: the sessions holding them had expired\n", dropped)
	}
	fmt.Printf("%s\n", writtenWhen(snaps))
	fmt.Printf("\nThe snapshots in %s are unchanged. Nothing reads them once a configuration\n"+
		"names the database instead; removing them is yours to do.\n", from)

	if failed > 0 {
		fmt.Fprintf(os.Stderr, "\n%d snapshot(s) could not be migrated, named above. %s holds what did:\n"+
			"remove it and run this again once the cause is fixed, or the two will disagree\n"+
			"about what the channels hold.\n", failed, to)
		return 1
	}
	return 0
}

// importAll converts one snapshot file at a time and carries on past a
// failure, because the failures after the first are exactly what stopping
// at the first would hide. Each file is one transaction, so a queue and its
// dead-letter channel arrive together or not at all (invariant 5).
func importAll(db *sqlite.DB, snaps []*store.Snapshot) (moved, failed, dropped int) {
	for _, s := range snaps {
		// A position whose session would have expired while nobody was
		// running belongs to nobody, and carrying it across is how the table
		// grows for ever (invariant 13). The same rule a starting broker
		// applies to a snapshot it loads.
		dropped += s.DropExpiredPositions(time.Now())

		if err := db.Import(s.Channels); err != nil {
			fmt.Printf("  %-24s FAILED\n", snapshotName(s))
			fmt.Fprintf(os.Stderr, "  %v\n", err)
			failed++
			continue
		}
		for i := range s.Channels {
			c := &s.Channels[i]
			fmt.Printf("  %-24s %-7s %s\n", c.Name, c.Kind, describe(c))
			moved++
		}
	}
	return moved, failed, dropped
}

// describe says what came across, in the numbers that matter: the offsets,
// which are the whole reason a migration exists rather than a fresh start.
func describe(c *store.ChannelState) string {
	switch c.Kind {
	case store.KindLatest:
		return fmt.Sprintf("%d topic(s), next %d", len(c.Records), c.Next)

	case store.KindQueue:
		offsets := make([]uint64, len(c.Items))
		for i := range c.Items {
			offsets[i] = c.Items[i].Offset
		}
		return fmt.Sprintf("%s, next %d", span("unresolved record", offsets), c.Next)

	default:
		offsets := make([]uint64, len(c.Records))
		for i := range c.Records {
			offsets[i] = c.Records[i].Offset
		}
		out := fmt.Sprintf("%s, next %d, floor %d", span("record", offsets), c.Next, c.Floor)
		if n := len(c.Positions); n > 0 {
			out += fmt.Sprintf(", %d position(s)", n)
		}
		return out
	}
}

// span reports how many of something there are and the offsets they run
// between, which is what tells an operator the numbering was carried over
// rather than started again.
func span(noun string, offsets []uint64) string {
	if len(offsets) == 0 {
		return fmt.Sprintf("no %ss", noun)
	}
	return fmt.Sprintf("%d %s(s), offsets %d..%d",
		len(offsets), noun, offsets[0], offsets[len(offsets)-1])
}

// snapshotName is the file a snapshot came out of. The first channel names
// it, which for a queue is the queue rather than its dead-letter channel.
func snapshotName(s *store.Snapshot) string {
	if len(s.Channels) == 0 {
		return "(a snapshot holding no channels)"
	}
	name, err := store.SnapshotFileName(s.Channels[0].Name)
	if err != nil {
		return s.Channels[0].Name
	}
	return name
}

// writtenWhen says how old what was migrated is, because a snapshot is
// written at a shutdown and an operator converting one months later should
// see that rather than work it out.
func writtenWhen(snaps []*store.Snapshot) string {
	var oldest, newest time.Time
	for _, s := range snaps {
		if s.WrittenAt.IsZero() {
			continue
		}
		if oldest.IsZero() || s.WrittenAt.Before(oldest) {
			oldest = s.WrittenAt
		}
		if s.WrittenAt.After(newest) {
			newest = s.WrittenAt
		}
	}
	switch {
	case oldest.IsZero():
		return "the snapshots record no time of writing"
	case oldest.Equal(newest):
		return "written at " + oldest.UTC().Format(time.RFC3339)
	default:
		return fmt.Sprintf("written between %s and %s - a channel older than the rest did not finish its shutdown",
			oldest.UTC().Format(time.RFC3339), newest.UTC().Format(time.RFC3339))
	}
}

// flagGiven says whether a flag was actually typed, rather than left at its
// default. --config defaults to empty and --check-config to false, so
// comparing values would let a default silently count as absent and the
// rules above would never fire.
func flagGiven(name string) bool {
	given := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			given = true
		}
	})
	return given
}

// operands pulls the leading non-flag arguments off the command line and
// re-parses whatever follows them as flags.
//
// **Go's flag package stops parsing at the first non-flag argument**, so
// once these commands took the configuration as an operand rather than
// through `--config`, every flag written after it silently became an
// operand too: `saguin --check-config saguin.yaml --output` reached the
// dispatch as two operands, and answered with a usage message about a file
// it had been given. The flag before the file worked, which is the worst
// version of a rule an operator cannot see.
//
// Parsing the tail again is safe because flag.Parse accumulates rather than
// resets what has been set, so `flagGiven` still answers about the whole
// command line. An operand that genuinely begins with `-` is written after
// a `--`, which the flag package already stops at.
func operands() ([]string, error) {
	var out []string
	rest := flag.Args()
	for len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		out = append(out, rest[0])
		rest = rest[1:]
	}
	if len(rest) > 0 {
		if err := flag.CommandLine.Parse(rest); err != nil {
			return nil, err
		}
		out = append(out, flag.Args()...)
	}
	return out, nil
}

// pipedIn reports whether standard input is something other than a
// terminal, which is how a batch run is told from a command somebody typed
// wrong.
//
// Without it, `saguin --route saguin.yaml` with the subject forgotten sits
// there reading a terminal and looks like a hang. A command that appears to
// have frozen is the worst answer to a typo, because the operator's next
// move is to kill it and then wonder what it did.
func pipedIn() bool {
	st, err := os.Stdin.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice == 0
}

func refuse(msg string) int {
	fmt.Fprintln(os.Stderr, strings.TrimRight(msg, "\n"))
	return 1
}

// migrateToSnapshots converts a sqlite database into a directory of
// snapshots, and returns the process's exit status.
//
// The cheaper direction: the channels come out of the database as the state
// a snapshot holds, and Dir.Save writes them the way a shutdown does -
// temporary file, sync, rename, directory sync, manifest first. None of
// that is reimplemented here, so a snapshot this writes is a snapshot a
// broker wrote.
func migrateToSnapshots(args []string, withConfig, withCheck bool) int {
	if withConfig {
		return refuse("--config cannot be given with a migration. A migration reads and writes\n" +
			"the paths you name and consults no configuration; accepting one would imply\n" +
			"that it respects it.")
	}
	if withCheck {
		return refuse("--check-config and --sqlite-to-snapshots cannot be given together: one\n" +
			"validates a configuration and the other writes storage.")
	}
	if len(args) != 2 {
		return refuse(fmt.Sprintf(
			"--sqlite-to-snapshots takes two paths, the database to read and the snapshot\n"+
				"directory to write:\n\n    saguin --sqlite-to-snapshots <file> <dir>\n\nGot %d.", len(args)))
	}
	from, to := args[0], args[1]

	if err := emptyOfSnapshots(to); err != nil {
		return refuse(err.Error())
	}

	db, err := sqlite.OpenForExport(from)
	if err != nil {
		return refuse(fmt.Sprintf("%v\n\nStop saguin before migrating if it is serving this database.\n"+
			"The migration takes both for itself.", err))
	}
	defer func() { _ = db.Close() }()

	channels, err := db.Export()
	if err != nil {
		return refuse(err.Error())
	}
	if len(channels) == 0 {
		// Nothing is created, for the reason the other direction creates
		// nothing: a manifest alone would make the directory ineligible and
		// lock the operator out of retrying into it.
		fmt.Printf("%s holds no channels. No snapshots were written.\n", from)
		return 0
	}

	fmt.Printf("migrating %s\n     into snapshots in %s\n\n", from, to)

	snaps, dropped := group(channels)
	for _, s := range snaps {
		for i := range s.Channels {
			c := &s.Channels[i]
			fmt.Printf("  %-24s %-7s %s\n", c.Name, c.Kind, describe(c))
		}
	}

	// **The destination's lock, as a broker takes it**, and the other
	// direction takes its source's. A directory a broker is serving is one its
	// next shutdown rewrites: migrated into, it lost every record at that
	// broker's SIGTERM, after this had said it was done. Taken here, once
	// there is something to write, so a refused or empty migration creates
	// no directory; and the check that it holds no snapshots is asked again
	// under the lock, so nothing can arrive between the two.
	if err := os.MkdirAll(to, 0o700); err != nil {
		return refuse(fmt.Sprintf("%s: %v", to, err))
	}
	lock, err := sqlite.Hold(filepath.Join(to, snapshotLockName), to)
	if err != nil {
		return refuse(fmt.Sprintf("cannot write %s while a broker is using it:\n%v\n\n"+
			"Stop saguin before migrating. The migration takes both for itself.", to, err))
	}
	defer func() { _ = lock.Release() }()
	if err := emptyOfSnapshots(to); err != nil {
		return refuse(err.Error())
	}

	started := time.Now()
	if err := store.NewDir(to, version()).Save(snaps); err != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n\n%s now holds part of a conversion. Clear the snapshots and the\n"+
			"manifest it wrote before running this again, or the two will disagree about\n"+
			"what the channels hold.\n", err, to)
		return 1
	}
	took := time.Since(started)

	fmt.Printf("\nmigrated %d channel(s) into %d snapshot(s) in %s\n",
		len(channels), len(snaps), took.Round(time.Millisecond))
	if dropped > 0 {
		fmt.Printf("%d consumer position(s) were left behind: the sessions holding them had expired\n", dropped)
	}
	fmt.Printf("\n%s is unchanged. Nothing reads it once a configuration names the directory\n"+
		"instead; removing it is yours to do.\n", from)
	return 0
}

// group puts each channel in the snapshot file it belongs to, and drops the
// positions whose sessions have already expired.
//
// A queue and its dead-letter channel share a file so that a record leaving
// one and arriving in the other cannot be recorded by half (invariant 5).
// Which channels those are is the __dlq suffix and nothing else - a derived
// name is part of the topic contract, which is why the store cannot answer
// this and the command can.
func group(channels []store.ChannelState) ([]*store.Snapshot, int) {
	byName := map[string]*store.Snapshot{}
	var out []*store.Snapshot

	// Queues first, so a dead-letter channel always finds its queue's file
	// already made. Export orders by name, and "jobs" sorts before
	// "jobs__dlq" - but that is an accident of this suffix rather than a rule
	// to lean on.
	for _, c := range channels {
		if c.Kind != store.KindQueue {
			continue
		}
		s := &store.Snapshot{Channels: []store.ChannelState{c}}
		byName[c.Name] = s
		out = append(out, s)
	}

	for _, c := range channels {
		if c.Kind == store.KindQueue {
			continue
		}
		if queue, ok := strings.CutSuffix(c.Name, channel.DLQSuffix); ok {
			if s := byName[queue]; s != nil {
				s.Channels = append(s.Channels, c)
				continue
			}
			// A dead-letter channel whose queue is not in this database is
			// not a companion any more: it is a channel of its own, and a
			// file of its own is the only place it can go.
		}
		out = append(out, &store.Snapshot{Channels: []store.ChannelState{c}})
	}

	dropped := 0
	for _, s := range out {
		dropped += s.DropExpiredPositions(time.Now())
	}
	return out, dropped
}

// emptyOfSnapshots reports whether a directory may be written into. It may
// exist and hold other things; what it may not hold is a conversion or a
// broker's own state, since either would end up merged with this one.
//
// A lock file left by a broker that has stopped does not count. The kernel
// releases the lock when a process ends but the file stays, so treating it
// as state would refuse every directory a broker had ever used.
func emptyOfSnapshots(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s: %v", dir, err)
	}

	var found []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".snapshot") || e.Name() == "saguin.manifest" {
			found = append(found, e.Name())
		}
	}
	if len(found) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s already holds snapshots: %s\n\n"+
			"A migration only ever writes into a directory that holds none. Writing beside\n"+
			"channels that are already there is how two histories end up under one name,\n"+
			"with one offset meaning two different records. Name an empty directory, or\n"+
			"move those files aside.",
		dir, strings.Join(found, ", "))
}
