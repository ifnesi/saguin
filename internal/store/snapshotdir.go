package store

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	snapshotExt = ".snapshot"
	tempExt     = ".tmp"

	// manifestName cannot collide with any channel's file. A channel name
	// reaches a file name with every character outside a small safe set
	// percent-encoded, so no channel can produce a literal dot before the
	// extension, and no channel can produce this.
	manifestName = "saguin.manifest"

	// sessionsName is the provider's persistent sessions. Like the manifest
	// it cannot collide with a channel's file, and it does not end in the
	// channel extension, so LoadAll never reads it as a channel.
	sessionsName = "saguin.sessions"

	// broadcastName is the provider's broadcast log, written with the
	// sessions file because a session's cursor is worth nothing without the
	// log it points into. It cannot collide with a channel's file for the
	// same two reasons, so LoadAll never reads it as a channel.
	broadcastName = "saguin.broadcast"

	// maxFileName is what a Linux filesystem accepts for one path
	// component. Change three checks it during configuration validation,
	// so an operator learns that a channel name cannot be stored before
	// the broker opens a listener, rather than at the shutdown where the
	// data would be lost.
	maxFileName = 255

	// writesAtOnce bounds how many files are being written and synced
	// together. Fifteen channels must not mean fifteen concurrent fsyncs
	// on an edge box. It is not configuration: the only reason to change
	// it is to make it worse.
	writesAtOnce = 4

	// writeBuffer keeps Encode's small writes off the syscall path. Every
	// integer in the format is four or eight bytes.
	writeBuffer = 64 << 10
)

// Dir is a directory of snapshot files: one per channel, plus a manifest
// naming what was written and when.
//
// One file per channel rather than one for the broker, so that a channel
// dropped from the configuration is a file nobody opens rather than a
// startup error, and a damaged file costs one channel instead of all of
// them. What that trades away is the one thing a single file gives for
// free: a crash partway through a shutdown leaves some channels from this
// run and some from the last, with nothing to say so. The manifest is
// what says so.
type Dir struct {
	path   string
	writer string // the saguin version stamped into every file it writes
}

func NewDir(path, writer string) *Dir { return &Dir{path: path, writer: writer} }

// Path is the directory the snapshots live in.
func (d *Dir) Path() string { return d.path }

// SnapshotFileName is where a channel's records are stored.
//
// A channel name cannot be a file name as it stands: "a.b" is a legal
// channel and "." and ".." are two directories. Everything outside a small
// safe set is percent-encoded, including the percent itself, so two channel
// names can never reach one file.
//
// The safe set is narrower than the set of characters a name may hold. A
// dot is legal in a name - "a.b", "...", ".hidden" - and is encoded here
// anyway, so that nothing this function is handed can come out as "." or
// ".." whatever the configuration admitted that day. Those two names are
// refused during validation as well, and this is the half that does not
// depend on it: a directory of snapshots is also read by the migration
// commands and by a broker whose name rule has changed under it.
//
// Encoding a dot costs three characters, and that is the reason a name
// inside the 128-byte channel limit can still be too long to store.
//
// It is exported because configuration validation calls it: a channel
// whose name is too long to store must be refused at startup, where it
// costs a restart, and not at the shutdown that would discover it by
// losing the channel.
func SnapshotFileName(channel string) (string, error) {
	var b strings.Builder
	for i := range len(channel) {
		c := channel[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	name := b.String() + snapshotExt
	if len(name)+len(tempExt) > maxFileName {
		return "", fmt.Errorf(
			"channel %q cannot be stored: its name becomes the %d-character file %q, and a file name holds at most %d",
			channel, len(name), name, maxFileName-len(tempExt))
	}
	return name, nil
}

// Save writes every snapshot, each to its own file, all stamped with one
// moment so that a later start can tell which of them came from this
// shutdown and which are older.
//
// The first channel of each snapshot names its file. For a queue that is
// the queue, and its dead-letter channel rides in the same file.
func (d *Dir) Save(snaps []*Snapshot) error { return d.save(time.Now(), snaps) }

func (d *Dir) save(now time.Time, snaps []*Snapshot) error {
	files := make([]string, len(snaps))
	for i, s := range snaps {
		if len(s.Channels) == 0 {
			return fmt.Errorf("a snapshot holding no channels has nothing to name its file")
		}
		name, err := SnapshotFileName(s.Channels[0].Name)
		if err != nil {
			return err
		}
		files[i] = name
	}

	if err := os.MkdirAll(d.path, 0o700); err != nil {
		return fmt.Errorf("snapshot directory %s: %w", d.path, err)
	}

	// The manifest goes first, and is made durable before any channel is
	// written. Written last, a crash before it would leave channel files
	// stamped later than the manifest - a state nothing can interpret.
	// Written first, every crash reads cleanly: the manifest says when
	// this shutdown began, and a channel older than that did not finish.
	//
	// Moving it below the loop passes every other test in this package, for
	// the reason it costs nothing until the power goes:
	// TestTheSnapshotWriteSyncsInTheOrderItsCommentsClaim reads the syscalls
	// and is what notices.
	m := &manifest{writer: d.writer, writtenAt: now, files: files}
	if err := d.replace(manifestName, m.encode); err != nil {
		return err
	}
	if err := syncDir(d.path); err != nil {
		return err
	}

	// Serialising costs nothing beside the fsync each file ends with, and
	// there is no reason to wait for one before starting the next.
	var wg sync.WaitGroup
	at := make(chan struct{}, writesAtOnce)
	errs := make([]error, len(snaps))
	for i, s := range snaps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			at <- struct{}{}
			defer func() { <-at }()

			s.Writer, s.WrittenAt = d.writer, now
			errs[i] = d.replace(files[i], s.Encode)
		}()
	}
	wg.Wait()

	// Every failure, not only the first. An operator whose disk filled
	// needs to know which channels did not make it, and the ones after
	// the first are exactly what stopping at the first would hide.
	if err := errors.Join(errs...); err != nil {
		return err
	}
	return syncDir(d.path)
}

// replace writes a file and puts it in place atomically. A crash at any
// point leaves either the file that was there before, whole, or the new
// one, whole, and never half of either - which is what invariant 14 means
// by never overwriting the known-good snapshot before its replacement is
// fully written and synced.
func (d *Dir) replace(name string, encode func(io.Writer) error) error {
	// The temporary file sits in the same directory because a rename is
	// atomic only within one filesystem. The same syscall check guards it.
	final := filepath.Join(d.path, name)
	tmp := final + tempExt

	// 0600 from the moment it exists, not chmod'd afterwards: a snapshot
	// holds whatever the application published.
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("%s: %w", tmp, err)
	}

	w := bufio.NewWriterSize(f, writeBuffer)
	err = encode(w)
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		// Synced before the rename, never after. The rename is what makes
		// this file the one that loads, and renaming over the last
		// known-good snapshot while the new one's bytes are still only in
		// the page cache is how a power cut destroys both at once.
		//
		// Deleting this line breaks nothing a normal shutdown can show. The
		// same syscall check guards it.
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: %w", tmp, err)
	}

	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("%s: %w", final, err)
	}
	return nil
}

// syncDir makes a rename durable. Without it a file's contents survive a
// power cut but the directory entry naming them need not, which leaves
// the new snapshot written, synced, and invisible.
//
// Both of its calls are load-bearing and neither is visible to a test that
// only reads the files back. The same syscall check guards them.
func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("snapshot directory %s: %w", path, err)
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("snapshot directory %s: %w", path, err)
	}
	return nil
}

// Loaded is what a snapshot directory held.
type Loaded struct {
	// Snapshots by the channel that names the file. A channel with no
	// file is simply absent - a channel that is new and a channel whose
	// file was never written look the same here, and the manifest is what
	// tells them apart, in Warnings.
	Snapshots map[string]*Snapshot

	// Warnings are what an operator has to be told about a directory the
	// broker is going to start from anyway.
	Warnings []string

	// Stale names the snapshots written before the shutdown the manifest
	// records began: a channel whose file that shutdown did not rewrite -
	// its write never landed, or the channel was not configured then.
	Stale map[string]bool
}

func (l *Loaded) warn(format string, args ...any) {
	l.Warnings = append(l.Warnings, fmt.Sprintf(format, args...))
}

// LoadAll reads every snapshot in the directory, in file-name order, and
// is how a directory is read by something that has no configuration to
// tell it what to look for.
//
// Load takes the channels a configuration names. This takes whatever is
// there, which is what the migration needs: a snapshot file carries its
// channel's name and kind, and a queue's file carries its dead-letter
// channel beside it, so a directory describes itself completely.
//
// A damaged file is an error and nothing is returned, for the same reason
// Load refuses one: half a directory converted is worse than none of it,
// and an operator who is told which file is wrong can act.
//
// Nothing here consults the manifest. It records which files a shutdown
// meant to write, which is what tells a starting broker that a channel did
// not finish - but each snapshot carries the version that wrote it and
// when, so a caller reporting what it read says the same thing more
// directly and does not have to survive a missing manifest to do it.
func (d *Dir) LoadAll() ([]*Snapshot, error) {
	entries, err := os.ReadDir(d.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("snapshot directory %s: %w", d.path, err)
	}

	var out []*Snapshot
	for _, e := range entries {
		// The manifest, the lock, and a temporary file left by a write that
		// did not finish are all not channels. Only the extension decides,
		// so a file nobody recognises is skipped rather than guessed at.
		if e.IsDir() || !strings.HasSuffix(e.Name(), snapshotExt) {
			continue
		}
		path := filepath.Join(d.path, e.Name())
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", path, err)
		}
		s, err := DecodeSnapshot(b)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", path, err)
		}
		out = append(out, s)
	}
	return out, nil
}

// Load reads the snapshot for each channel named and cross-checks what it
// found against the manifest.
//
// The channels named are the ones that name files, not every channel a
// broker holds: a queue's dead-letter channel rides in the queue's file
// and is reached through it, so naming it here would look for a file that
// was never written.
//
// A channel file that is damaged, truncated, or in a format this broker
// does not read is an error, and the broker does not start: an empty
// start is indistinguishable from a fresh install and destroys the
// evidence (invariant 14). The manifest's own failures are warnings
// instead - it holds no records, so refusing to start over a file that
// carries nothing would be the worst trade in the design.
func (d *Dir) Load(channels []string) (*Loaded, error) {
	out := &Loaded{Snapshots: map[string]*Snapshot{}}

	m, mErr := d.loadManifest()

	read := map[string]bool{}
	for _, ch := range channels {
		name, err := SnapshotFileName(ch)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(d.path, name)

		b, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", path, err)
		}
		s, err := DecodeSnapshot(b)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", path, err)
		}
		out.Snapshots[ch] = s
		read[name] = true
	}

	// A directory with neither a manifest nor a single snapshot is a
	// fresh install, and has nothing to say for itself.
	if m == nil && len(out.Snapshots) == 0 {
		return out, nil
	}
	if m == nil {
		out.warn("the snapshot directory %s has no usable manifest (%v), so how old each snapshot is cannot be checked", d.path, mErr)
		return out, nil
	}

	for _, name := range m.files {
		if read[name] {
			continue
		}
		// A file the manifest listed and which is not there is a channel
		// whose write never landed. A file that is there but was not read
		// belongs to a channel the configuration no longer names, which
		// is the operator's own doing and needs no comment.
		if _, err := os.Stat(filepath.Join(d.path, name)); errors.Is(err, os.ErrNotExist) {
			out.warn("%s was being written at %s and is not there: that channel's records are gone",
				name, stamp(m.writtenAt))
		}
	}

	for _, ch := range channels {
		s, ok := out.Snapshots[ch]
		if !ok {
			continue
		}
		switch {
		case s.WrittenAt.Before(m.writtenAt):
			if out.Stale == nil {
				out.Stale = map[string]bool{}
			}
			out.Stale[ch] = true
			out.warn("channel %q was last written at %s, before the shutdown that began at %s: everything published after that is gone",
				ch, stamp(s.WrittenAt), stamp(m.writtenAt))
		case s.WrittenAt.After(m.writtenAt):
			out.warn("channel %q was written at %s, after the manifest at %s: the channels finished but the manifest did not",
				ch, stamp(s.WrittenAt), stamp(m.writtenAt))
		}
	}

	return out, nil
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return "an unrecorded time"
	}
	return t.UTC().Format(time.RFC3339)
}

// manifest is what the directory says it wrote, and when. It carries no
// records, so every one of its failures is survivable.
type manifest struct {
	writer    string
	writtenAt time.Time
	files     []string
}

func (m *manifest) encode(w io.Writer) error {
	sum := crc32.New(crcTable)
	e := &encoder{w: io.MultiWriter(w, sum)}

	writeHeader(e, kindManifest, m.writer, m.writtenAt)
	e.u32(uint32(len(m.files)))
	for _, f := range m.files {
		e.str(f)
	}
	return seal(e, w, sum)
}

func (d *Dir) loadManifest() (*manifest, error) {
	b, err := os.ReadFile(filepath.Join(d.path, manifestName))
	if err != nil {
		return nil, err
	}
	h, body, err := readHeader(b, kindManifest)
	if err != nil {
		return nil, err
	}

	m := &manifest{writer: h.writer, writtenAt: h.writtenAt}
	dec := &decoder{b: body}
	n := dec.count(minFileNameSize)
	m.files = make([]string, 0, n)
	for range n {
		m.files = append(m.files, dec.str())
	}
	if dec.err != nil {
		return nil, fmt.Errorf("%s: %w", h.describe(), dec.err)
	}
	if dec.i != len(body) {
		return nil, fmt.Errorf("%s: %d bytes remain after the last file name", h.describe(), len(body)-dec.i)
	}
	return m, nil
}

// minFileNameSize is the length prefix a file name cannot be shorter than.
const minFileNameSize = 4

// SaveSessions writes the provider's persistent sessions and their broadcast
// log, each to its own file, replacing the last of each only once the new one
// is whole and synced (invariant 14).
//
// **The log goes first, and is made durable before the sessions file is
// written.** The sessions' cursors are positions on the log, so the log is the
// half the other depends on: a stop that crashes between the two leaves this
// run's log beside the last run's sessions, which the start reads as sessions
// whose cursors are where they were and cursors of sessions it does not hold
// (RestoreSessions drops those). Written the other way round, the same crash
// would leave this run's sessions over the last run's log.
//
// **A snapshot without its log is refused rather than written.** Writing the
// sessions alone would leave whatever log the directory held before beside
// them, and the two would no longer be one pair.
func (d *Dir) SaveSessions(snap *SessionsSnapshot) error {
	if snap.Log == nil {
		return fmt.Errorf("snapshot directory %s: the sessions come with their broadcast log, and none was given", d.path)
	}
	if err := d.SaveBroadcast(snap.Log); err != nil {
		return err
	}
	if snap.Writer == "" {
		snap.Writer = d.writer
	}
	if snap.WrittenAt.IsZero() {
		snap.WrittenAt = time.Now()
	}
	if err := d.replace(sessionsName, snap.Encode); err != nil {
		return err
	}
	return syncDir(d.path)
}

// LoadSessions reads the provider's persistent sessions and their broadcast
// log, the pair SaveSessions writes. Neither file is no sessions; a log with
// no sessions file was left by a first stop that crashed between the two, and
// reads with no sessions. A file that is there and cannot be read is an
// error, and the broker does not start on it (invariant 14).
//
// **Two pairs cannot be true, and are refused rather than started on.** The
// log is always written before the sessions, so a sessions file with no log
// beside it has lost its log: its in-flight tables would name offsets in a
// log that starts again at 1 (invariant 9). And an in-flight entry at or past
// the log's next offset names a message the log never had.
func (d *Dir) LoadSessions() (*SessionsSnapshot, error) {
	lg, err := d.LoadBroadcast()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(d.path, sessionsName)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if lg == nil {
			return nil, nil
		}
		return &SessionsSnapshot{Writer: lg.Writer, WrittenAt: lg.WrittenAt, Log: lg}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", path, err)
	}
	if lg == nil {
		return nil, fmt.Errorf("snapshot %s: the sessions are written after their broadcast log, and %s is not there",
			path, filepath.Join(d.path, broadcastName))
	}
	s, err := DecodeSessions(b)
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", path, err)
	}
	for _, st := range s.Sessions {
		for _, f := range st.InFlight {
			if f.Offset >= lg.Next {
				return nil, fmt.Errorf("snapshot %s: client %q has offset %d in flight, and the broadcast log's next is %d",
					path, st.Session.Client, f.Offset, lg.Next)
			}
		}
	}
	s.Log = lg
	return s, nil
}

// SaveBroadcast writes the provider's broadcast log to its own file,
// replacing the last one only once the new one is whole and synced
// (invariant 14). SaveSessions calls it, first: the log is written with the
// sessions file, never instead of it.
func (d *Dir) SaveBroadcast(snap *BroadcastSnapshot) error {
	if err := os.MkdirAll(d.path, 0o700); err != nil {
		return fmt.Errorf("snapshot directory %s: %w", d.path, err)
	}
	if snap.Writer == "" {
		snap.Writer = d.writer
	}
	if snap.WrittenAt.IsZero() {
		snap.WrittenAt = time.Now()
	}
	if err := d.replace(broadcastName, snap.Encode); err != nil {
		return err
	}
	return syncDir(d.path)
}

// LoadBroadcast reads the provider's broadcast log. No file is no log. A file
// that is there and cannot be read is an error, and the broker does not start
// on it (invariant 14).
func (d *Dir) LoadBroadcast() (*BroadcastSnapshot, error) {
	path := filepath.Join(d.path, broadcastName)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", path, err)
	}
	s, err := DecodeBroadcast(b)
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", path, err)
	}
	return s, nil
}
