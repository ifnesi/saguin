package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Disk monitoring is outside the recipe. The threshold has no default:
// each machine must supply its own measured boundary between phases.
var (
	monitorDisk bool
	slowWrite   time.Duration
	// monitoredDrive is the drive the monitor samples, the one holding the
	// tool's temporary folder and so the brokers' files, found by validate.
	monitoredDrive string
	diskstatsFile  = "/proc/diskstats"
)

// diskSecond describes writes completed since the previous sample.
type diskSecond struct {
	writes  int64
	latency time.Duration
	slow    bool
}

type diskCounters struct {
	writes, writing, inProgress int64
}

type diskMonitor struct {
	drive   string
	slow    time.Duration
	before  diskCounters
	seconds []diskSecond
}

// newDiskMonitor verifies that the drive exists in diskstats and fixes
// the baseline. A run calls this at the measured window's start.
func newDiskMonitor(drive string, slow time.Duration) (*diskMonitor, error) {
	m := &diskMonitor{drive: drive, slow: slow}
	var err error
	m.before, err = m.read()
	return m, err
}

func (m *diskMonitor) read() (diskCounters, error) {
	f, err := os.Open(diskstatsFile)
	if err != nil {
		return diskCounters{}, err
	}
	defer func() { _ = f.Close() }() // read-only
	lines := bufio.NewScanner(f)
	for lines.Scan() {
		fields := strings.Fields(lines.Text())
		if len(fields) < 3 || fields[2] != m.drive {
			continue
		}
		if len(fields) < 14 {
			return diskCounters{}, fmt.Errorf("%s: incomplete counters for %s", diskstatsFile, m.drive)
		}
		var c diskCounters
		for _, field := range []struct {
			index int
			into  *int64
		}{{7, &c.writes}, {10, &c.writing}, {11, &c.inProgress}} {
			n, err := strconv.ParseInt(fields[field.index], 10, 64)
			if err != nil || n < 0 {
				return diskCounters{}, fmt.Errorf("%s: invalid counter %q for %s", diskstatsFile, fields[field.index], m.drive)
			}
			*field.into = n
		}
		return c, nil
	}
	if err := lines.Err(); err != nil {
		return diskCounters{}, err
	}
	return diskCounters{}, fmt.Errorf("%s has no drive %s", diskstatsFile, m.drive)
}

// sample uses diskstats' write-time delta divided by completed writes,
// the same mean as w_await. No completions with I/O pending is a stall.
func (m *diskMonitor) sample() error {
	after, err := m.read()
	if err != nil {
		return err
	}
	writes, writing := after.writes-m.before.writes, after.writing-m.before.writing
	if writes < 0 || writing < 0 {
		return fmt.Errorf("%s: counters for %s went backwards", diskstatsFile, m.drive)
	}
	s := diskSecond{writes: writes}
	if writes > 0 {
		s.latency = time.Duration(float64(writing) / float64(writes) * float64(time.Millisecond))
	}
	s.slow = s.latency > m.slow || (writes == 0 && after.inProgress > 0)
	m.seconds = append(m.seconds, s)
	m.before = after
	return nil
}

// mount describes the longest matching mount point in mountinfo.
type mount struct{ point, fstype, source, device string }

func mountOf(dir string) (mount, error) {
	dir, err := filepath.Abs(dir)
	if err == nil {
		dir, err = filepath.EvalSymlinks(dir)
	}
	if err != nil {
		return mount{}, err
	}
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return mount{}, err
	}
	// mountinfo escapes whitespace and backslashes in path fields.
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	var found mount
	for line := range strings.Lines(string(data)) {
		pre, post, ok := strings.Cut(line, " - ")
		fields, rest := strings.Fields(pre), strings.Fields(post)
		if !ok || len(fields) < 5 || len(rest) < 2 {
			continue
		}
		point := unescape.Replace(fields[4])
		if inside(dir, point) && len(point) >= len(found.point) {
			found = mount{point, rest[0], unescape.Replace(rest[1]), fields[2]}
		}
	}
	if found.point == "" {
		return mount{}, fmt.Errorf("no mount holds %s", dir)
	}
	return found, nil
}

// driveOf follows the filesystem's device in sysfs, then moves from a
// partition to its parent drive. Btrfs can use a virtual mount device
// number; its mount source identifies the actual block device instead.
func driveOf(dir string) (string, error) {
	m, err := mountOf(dir)
	if err != nil {
		return "", err
	}
	dev, err := filepath.EvalSymlinks("/sys/dev/block/" + m.device)
	if err != nil {
		dev, err = filepath.EvalSymlinks("/sys/class/block/" + filepath.Base(m.source))
	}
	if err != nil {
		return "", fmt.Errorf("drive holding %s (%s): %w", dir, m.source, err)
	}
	if _, err := os.Stat(filepath.Join(dev, "partition")); err == nil {
		dev = filepath.Dir(dev)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return filepath.Base(dev), nil
}
