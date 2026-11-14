package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// sysBlock is where Linux lists block devices, one directory per drive and
// partition; a partition's directory holds a file named partition.
const sysBlock = "/sys/class/block"

// TestDiskMonitor checks -disk-monitor, 0028j's plan [261107 065639]: "a
// disk monitor beside each run labelling every second with the drive's
// phase", "the disk monitor's samples, each second with its latency and
// phase", read from /proc/diskstats [261107 121359]. R7's results are
// bimodal with its drive's state (targets.md, Assumptions, Disk phases:
// the drive "alternates between a fast and a slow phase under sustained
// synced writes"); the monitor says which phase each second of a run was
// in.
//
// The interface:
//
//   - -disk-monitor turns the monitor on (the package's monitorDisk), and
//     -disk-slow, a duration, is its threshold (slowWrite). The results
//     name the monitor as the other options are named, with the drive it
//     watches and the threshold, in a form such as "disk monitor on
//     nvme0n1, slow above 2ms (outside the recipe)".
//   - driveOf(dir) is the drive holding dir, as /proc/diskstats names it:
//     the whole drive, not its partition (nvme0n1, not nvme0n1p2), as
//     the drive is what changes phase. The monitor watches the drive of
//     the store's directory, the run's os.TempDir().
//   - newDiskMonitor(drive, slow) takes a first reading of the drive's line
//     in diskstatsFile ("/proc/diskstats"), and fails where the file has
//     none: a monitor that watches nothing would measure nothing
//     (design.md, Testing: "A run that tested nothing never passes").
//   - m.sample(), which the run calls at the end of each second, reads the
//     drive's line again and records in m.seconds one diskSecond for the
//     time since the previous reading: writes, the writes the drive
//     completed in it (diskstats' 5th statistic, writes completed);
//     latency, their mean write latency, as iostat's w_await: the time
//     the drive spent on them (the 8th, milliseconds spent writing) over
//     their number, zero where none completed; and slow, its phase.
//
// The phase, as an operator reads it: a second is slow when its mean write
// latency is above the threshold, a latency at the threshold being fast;
// or when the drive completed no write in it while requests were in
// progress at its end (the 9th statistic, I/Os in progress): the drive
// stalled. Every other second is fast, one in which the drive wrote
// nothing and had nothing in progress included.
//
// The real drive cannot give a known latency, so it is checked for what
// it can show: the drive of the test's directory is a whole drive that
// /proc/diskstats names, a write synced in that directory is counted on
// it, and the monitor counts it. Latencies and phases are then checked
// against a scripted diskstats, which the tool reads as it reads the real
// one, whose lines give known answers. In it, the drive's partition sdz1
// comes before the drive sdz, which a match on the name's prefix would
// take for it, and the other drive sdy is always slow.
//
// The threshold has no default: each machine's drive has its own (the A10's
// ordinary synced write takes about 4 ms, R7's 0.16 ms: targets.md, Drive
// only), so -disk-monitor without -disk-slow, or with -disk-slow 0, is
// refused, naming -disk-slow.
//
// Every path that accepts -disk-monitor honours it, or refuses it at
// parse; it never runs unwatched. The recipe's runs and the reconnect
// experiment sample it each second of their measured window (outcome.window;
// TestKeepingUp runs one). The others are run here, short, with -window 1s:
//
//   - publish-only and publish-mixed (one run of the same report path)
//     against Sagüin, the test peer keeping no files on the drive the
//     monitor samples (reaches): the results file it saves holds the drive's
//     seconds, a table with a "Drive phase" column, a row for the
//     window's one second;
//   - drain-only against Sagüin, with diskstatsFile naming no file: the run
//     fails, naming that file, as a monitor that cannot read its drive must
//     (newDiskMonitor), rather than drain unwatched.
func TestDiskMonitor(t *testing.T) {
	dir := t.TempDir()
	drive, err := driveOf(dir)
	if err != nil {
		t.Fatalf("the drive of %s: %v", dir, err)
	}
	if _, err := os.Stat(filepath.Join(sysBlock, drive)); err != nil {
		t.Fatalf("the drive of %s is %q, which is no block device: %v", dir, drive, err)
	}
	if _, err := os.Stat(filepath.Join(sysBlock, drive, "partition")); err == nil {
		t.Errorf("the drive of %s is %q, a partition; want the whole drive holding it", dir, drive)
	}
	before := writesOn(t, drive)
	watched, err := newDiskMonitor(drive, time.Millisecond)
	if err != nil {
		t.Fatalf("a monitor of %s: %v", drive, err)
	}
	writeSynced(t, dir)
	if after := writesOn(t, drive); after <= before {
		t.Errorf("a write synced in %s left %s's writes completed at %d, from %d; want it counted on the drive holding it", dir, drive, after, before)
	}
	if err := watched.sample(); err != nil {
		t.Fatalf("sampling %s: %v", drive, err)
	}
	if len(watched.seconds) != 1 || watched.seconds[0].writes < 1 {
		t.Errorf("the monitor of %s recorded %+v over a synced write; want one second, with at least that write", drive, watched.seconds)
	}

	on, slow := monitorDisk, slowWrite
	t.Cleanup(func() { monitorDisk, slowWrite = on, slow })
	store, err := driveOf(os.TempDir())
	if err != nil {
		t.Fatalf("the drive of %s, the store's directory: %v", os.TempDir(), err)
	}
	o := asked(t, "-disk-monitor", "-disk-slow", "2ms")
	named := func(s string) bool {
		return strings.Contains(s, "disk monitor") && strings.Contains(s, store) && strings.Contains(s, "2ms")
	}
	if !slices.ContainsFunc(o.partial, named) {
		t.Errorf("with -disk-monitor -disk-slow 2ms, the results' setup names %q; want the disk monitor, on %s, slow above 2ms", o.partial, store)
	}

	for _, args := range [][]string{{"-disk-monitor"}, {"-disk-monitor", "-disk-slow", "0"}} {
		if _, err := parse(args, io.Discard); err == nil || !strings.Contains(err.Error(), "-disk-slow") {
			t.Errorf("load %q: %v; want it refused, naming -disk-slow, the threshold every monitored run states", args, err)
		}
	}
	monitoredPaths(t)

	scripted := filepath.Join(t.TempDir(), "diskstats")
	was := diskstatsFile
	diskstatsFile = scripted
	t.Cleanup(func() { diskstatsFile = was })
	// script writes the scripted diskstats with sdz's totals: writes
	// completed, milliseconds spent writing and I/Os in progress.
	script := func(writes, writing, inProgress int64) {
		t.Helper()
		text := diskstatsLine(1, "sdz1", 3*writes, writes/10, 0) + diskstatsLine(0, "sdz", writes, writing, inProgress) +
			diskstatsLine(16, "sdy", writes+1, 100*writing+100, 0)
		if err := os.WriteFile(scripted, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	script(1000, 500, 0)
	if _, err := newDiskMonitor("sdx", 2*time.Millisecond); err == nil {
		t.Error("a monitor of sdx, a drive the diskstats does not name, started; want it refused")
	}
	m, err := newDiskMonitor("sdz", 2*time.Millisecond)
	if err != nil {
		t.Fatalf("a monitor of sdz, threshold 2ms: %v", err)
	}
	steps := []struct {
		writes, writing, inProgress int64 // sdz's totals at the second's end
		want                        diskSecond
		why                         string
	}{
		{3000, 900, 0, diskSecond{writes: 2000, latency: 200 * time.Microsecond}, "2,000 writes in 400 ms: 0.2 ms each, below 2 ms"},
		{3500, 3400, 1, diskSecond{writes: 500, latency: 5 * time.Millisecond, slow: true}, "500 writes in 2,500 ms: 5 ms each, above 2 ms"},
		{3600, 3600, 0, diskSecond{writes: 100, latency: 2 * time.Millisecond}, "100 writes in 200 ms: 2 ms each, at the threshold"},
		{3600, 3600, 3, diskSecond{slow: true}, "no write completed, 3 requests in progress: stalled"},
		{3600, 3600, 0, diskSecond{}, "no write completed, none in progress: idle"},
	}
	for _, s := range steps {
		script(s.writes, s.writing, s.inProgress)
		if err := m.sample(); err != nil {
			t.Fatalf("sampling sdz: %v", err)
		}
	}
	if len(m.seconds) != len(steps) {
		t.Fatalf("the monitor recorded %d seconds, %+v; want %d, one per sample", len(m.seconds), m.seconds, len(steps))
	}
	for i, s := range steps {
		if got := m.seconds[i]; got.writes != s.want.writes || got.latency != s.want.latency || got.slow != s.want.slow {
			t.Errorf("second %d, %s: %d writes, latency %v, slow %v; want %d writes, latency %v, slow %v",
				i+1, s.why, got.writes, got.latency, got.slow, s.want.writes, s.want.latency, s.want.slow)
		}
	}
}

// diskstatsLine is one drive's line of a scripted diskstats, as the kernel
// writes it, with the drive's totals of writes completed, milliseconds
// spent writing and I/Os in progress.
func diskstatsLine(minor int, name string, writes, writing, inProgress int64) string {
	return fmt.Sprintf("%4d %7d %s 4000 10 32000 900 %d 20 %d %d %d 5000 6000 0 0 0 0 30 40\n", 8, minor, name, writes, 8*writes, writing, inProgress)
}

// writesOn is the writes completed on drive so far, from its line in the
// real /proc/diskstats (its 5th statistic, after the major and minor
// numbers and the name).
func writesOn(t *testing.T, drive string) int64 {
	t.Helper()
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(data)) {
		if f := strings.Fields(line); len(f) >= 14 && f[2] == drive {
			n, err := strconv.ParseInt(f[7], 10, 64)
			if err != nil {
				t.Fatalf("/proc/diskstats: %q: %v", line, err)
			}
			return n
		}
	}
	t.Fatalf("/proc/diskstats has no line for %s", drive)
	return 0
}

// writeSynced writes a file in dir and syncs it to its drive.
func writeSynced(t *testing.T, dir string) {
	t.Helper()
	f, err := os.CreateTemp(dir, "synced-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }() // closed below; again if the test ends early
	if _, err := f.Write(make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// monitoredPaths runs, short, each experiment that accepts -disk-monitor
// and does not take its measured window from outcome.window, as
// TestDiskMonitor says; an experiment parse refuses it for is not run.
func monitoredPaths(t *testing.T) {
	t.Helper()
	on, slow, window, file := monitorDisk, slowWrite, measuredWindow, diskstatsFile
	t.Cleanup(func() { monitorDisk, slowWrite, measuredWindow, diskstatsFile = on, slow, window, file })
	accepts := func(experiment string) bool {
		_, err := parse([]string{"-experiment", experiment, "-disk-monitor", "-disk-slow", "2ms"}, io.Discard)
		return err == nil
	}
	if accepts("publish-only") || accepts("publish-mixed") {
		results := t.TempDir()
		args := []string{"-experiment", "publish-mixed", "-disk-monitor", "-disk-slow", "1h", "-window", "1s", "-binary", brokerBinary, "-results", results}
		var stdout, stderr bytes.Buffer
		if code := load(args, &stdout, &stderr); code != 0 {
			t.Errorf("load %q: exit %d; stdout:\n%s\nstderr:\n%s", args, code, stdout.String(), stderr.String())
		} else if file := savedFile(t, results); tableWith(file, "Drive phase") == "" {
			t.Errorf("load %q saved no table with a Drive phase column, the drive's seconds it accepted to watch:\n%s", args, file)
		} else if columns, rows := savedTable(file, tableWith(file, "Drive phase")); len(rows) != 1 {
			t.Errorf("load %q saved the drive's seconds as columns %q, rows %q; want one row, the 1 s window's one second", args, columns, rows)
		}
	}
	if accepts("drain-only") {
		missing := filepath.Join(t.TempDir(), "no-diskstats")
		args := []string{"-experiment", "drain-only", "-disk-monitor", "-disk-slow", "1h", "-window", "1s", "-binary", brokerBinary, "-results", t.TempDir()}
		diskstatsFile = missing // read when the monitor starts, after parse
		var stdout, stderr bytes.Buffer
		if code := load(args, &stdout, &stderr); code == 0 || !strings.Contains(stdout.String()+stderr.String(), missing) {
			t.Errorf("load %q with diskstats at %s, which does not exist: exit %d; stdout:\n%s\nstderr:\n%s\nwant a failure naming %s: the drain ran unwatched",
				args, missing, code, stdout.String(), stderr.String(), missing)
		}
		diskstatsFile = file
	}
}

// savedFile is the one results file load saved in dir.
func savedFile(t *testing.T, dir string) string {
	t.Helper()
	saved, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil || len(saved) != 1 {
		t.Fatalf("%s holds results files %q (%v); want one", dir, saved, err)
	}
	data, err := os.ReadFile(saved[0])
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// tableWith is the heading of a results file's first table with column.
func tableWith(file, column string) string {
	for line := range strings.Lines(file) {
		if after, ok := strings.CutPrefix(line, "## "); ok {
			heading := strings.TrimSpace(after)
			if columns, _ := savedTable(file, heading); slices.Contains(columns, column) {
				return heading
			}
		}
	}
	return ""
}
