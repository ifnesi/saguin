package main

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
)

// clockTicks is USER_HZ, the unit of the CPU times in /proc/<pid>/stat and
// /proc/stat (proc(5)): fixed at 100 by Linux's user-space interface on
// every architecture it runs Sagüin on.
const clockTicks = 100

// A sample is the resources read at one instant.
type sample struct {
	brokerCPU, clientCPU time.Duration
	hostBusy, hostTotal  uint64 // /proc/stat's ticks, all CPUs
	ioStall, cpuStall    int64  // pressure stall totals in µs; -1 if unreadable
	memStall             int64  // the host's memory pressure stall total
	// The memory pressure stall totals of the broker's and the tool's own
	// cgroups.
	brokerMemStall, toolMemStall int64
	gcCPU                        float64
	gcCycles                     uint64
	sched                        *metrics.Float64Histogram
}

// The runtime metrics a sample reads of the load tool itself.
var runtimeMetrics = []string{"/cpu/classes/gc/total:cpu-seconds", "/gc/cycles/total:gc-cycles", "/sched/latencies:seconds"}

// take samples the broker, pid (none for zero), the load tool and the
// host.
func take(pid int) (sample, error) {
	var s sample
	var err error
	if pid != 0 {
		if s.brokerCPU, err = cpuTime(pid); err != nil {
			return s, err
		}
	}
	if s.clientCPU, err = cpuTime(os.Getpid()); err != nil {
		return s, err
	}
	if s.hostBusy, s.hostTotal, err = hostCPU(); err != nil {
		return s, err
	}
	s.ioStall, s.cpuStall, s.memStall = stall("/proc/pressure/io"), stall("/proc/pressure/cpu"), stall("/proc/pressure/memory")
	s.brokerMemStall, s.toolMemStall = -1, stall(cgroupDir(os.Getpid())+"/memory.pressure")
	if pid != 0 {
		s.brokerMemStall = stall(cgroupDir(pid) + "/memory.pressure")
	}
	m := make([]metrics.Sample, len(runtimeMetrics))
	for i, name := range runtimeMetrics {
		m[i].Name = name
	}
	metrics.Read(m)
	s.gcCPU, s.gcCycles, s.sched = m[0].Value.Float64(), m[1].Value.Uint64(), m[2].Value.Float64Histogram()
	return s, nil
}

// resources are what the broker, the load tool and the host used between
// two samples.
type resources struct {
	length               time.Duration
	brokerCPU, clientCPU time.Duration
	hostBusy             float64 // the share of all CPUs' time busy
	ioStall, cpuStall    float64 // the share of the time some task stalled; NaN if unreadable
	memStall             float64 // the same, on memory, for the host
	// The same, on memory, within the broker's and the tool's cgroups.
	brokerMemStall, toolMemStall float64
	gcCPU                        time.Duration
	gcCycles                     uint64
	sched99                      time.Duration // the tool's goroutine scheduling latency, 99th percentile
}

// since is what was used from a to s.
func (s *sample) since(a *sample) resources {
	share := func(after, before int64) float64 {
		if after < 0 || before < 0 {
			return math.NaN()
		}
		return float64(after-before) / (float64(measuredWindow) / float64(time.Microsecond))
	}
	return resources{
		length:         measuredWindow,
		brokerCPU:      s.brokerCPU - a.brokerCPU,
		clientCPU:      s.clientCPU - a.clientCPU,
		hostBusy:       float64(s.hostBusy-a.hostBusy) / float64(max(s.hostTotal-a.hostTotal, 1)),
		ioStall:        share(s.ioStall, a.ioStall),
		cpuStall:       share(s.cpuStall, a.cpuStall),
		memStall:       share(s.memStall, a.memStall),
		brokerMemStall: share(s.brokerMemStall, a.brokerMemStall),
		toolMemStall:   share(s.toolMemStall, a.toolMemStall),
		gcCPU:          time.Duration((s.gcCPU - a.gcCPU) * float64(time.Second)),
		gcCycles:       s.gcCycles - a.gcCycles,
		sched99:        histogramDelta(s.sched, a.sched, 0.99),
	}
}

// histogramDelta is quantile q of what runtime histogram after counted
// since before, read as its bucket's upper bound.
func histogramDelta(after, before *metrics.Float64Histogram, q float64) time.Duration {
	counts := make([]uint64, len(after.Counts))
	var total uint64
	for i, n := range after.Counts {
		counts[i] = n - before.Counts[i]
		total += counts[i]
	}
	rank := max(uint64(math.Ceil(q*float64(total))), 1)
	var seen uint64
	for i, n := range counts {
		if seen += n; seen >= rank {
			return time.Duration(min(after.Buckets[i+1], 3600) * float64(time.Second))
		}
	}
	return 0
}

// peaks reads the peak resident memory of the broker, pid (none for
// zero), and of the load tool, over their lifetime so far.
func (o *outcome) peaks(pid int) error {
	var err error
	if pid != 0 {
		if o.peakRSS, err = peakRSS(pid); err != nil {
			return err
		}
	}
	o.clientRSS, err = peakRSS(os.Getpid())
	if pid != 0 {
		o.brokerMemory = cgroupMemory(pid)
	}
	o.toolMemory = cgroupMemory(os.Getpid())
	return err
}

// cpuTime is the CPU time process pid has used so far, user and system,
// all its threads together: /proc/<pid>/stat's utime and stime.
func cpuTime(pid int) (time.Duration, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	// Field 2, the command name, is in parentheses and may hold spaces,
	// so the fields are counted from after its closing one: state, field
	// 3, is the first, utime (14) the 12th and stime (15) the 13th.
	fields := strings.Fields(string(data[bytes.LastIndexByte(data, ')')+1:]))
	if len(fields) < 13 {
		return 0, fmt.Errorf("/proc/%d/stat has %d fields after the command name", pid, len(fields))
	}
	ticks, err := sum(fields[11:13])
	return time.Duration(ticks) * time.Second / clockTicks, err
}

// hostCPU is the host's CPU time in ticks, busy and in all, from
// /proc/stat's first line: busy is all but idle and iowait.
func hostCPU() (busy, total uint64, err error) {
	line, err := procField("/proc/stat", "cpu ")
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(line)
	if len(fields) < 8 {
		return 0, 0, fmt.Errorf("/proc/stat: %q", line)
	}
	total, err = sum(fields[:8]) // user to steal; guest time is inside user
	if err != nil {
		return 0, 0, err
	}
	idle, err := sum(fields[3:5])
	return total - idle, total, err
}

// sum adds decimal counters.
func sum(fields []string) (uint64, error) {
	var n uint64
	for _, f := range fields {
		v, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			return 0, err
		}
		n += v
	}
	return n, nil
}

// stall is the total time, in µs, some task stalled since the count
// began, from a pressure file's "some" line (/proc/pressure/<resource>, or
// a cgroup's <resource>.pressure); -1 where it cannot be read.
func stall(path string) int64 {
	line, err := procField(path, "some ")
	if err != nil {
		return -1
	}
	_, total, ok := strings.Cut(line, "total=")
	if !ok {
		return -1
	}
	n, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// cgroupDir is the cgroup (v2) directory of process pid; "" where unknown.
func cgroupDir(pid int) string {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	if err != nil {
		return ""
	}
	_, path, ok := strings.Cut(strings.TrimSpace(string(data)), "::")
	if !ok {
		return ""
	}
	return "/sys/fs/cgroup" + path
}

// cgroupMemory describes process pid's cgroup memory: its peak, its own
// limits and each enclosing limit set, and its out-of-memory kills.
func cgroupMemory(pid int) string {
	dir := cgroupDir(pid)
	if dir == "" {
		return "unknown"
	}
	read := func(d, name string) string {
		v, err := os.ReadFile(filepath.Join(d, name))
		if err != nil {
			return "unreadable"
		}
		return strings.TrimSpace(string(v))
	}
	peak, err := strconv.ParseUint(read(dir, "memory.peak"), 10, 64)
	peakText := read(dir, "memory.peak")
	if err == nil {
		peakText = fmt.Sprintf("%.0f MiB", mib(peak))
	}
	oom, _ := procField(filepath.Join(dir, "memory.events"), "oom_kill ") // reported as found
	var text strings.Builder
	fmt.Fprintf(&text, "%s: peak %s, memory.max %s, memory.swap.max %s, oom_kill %s",
		strings.TrimPrefix(dir, "/sys/fs/cgroup"), peakText, read(dir, "memory.max"), read(dir, "memory.swap.max"), cmp.Or(oom, "unreadable"))
	for d := filepath.Dir(dir); strings.HasPrefix(d, "/sys/fs/cgroup/"); d = filepath.Dir(d) {
		if limit := read(d, "memory.max"); limit != "max" && limit != "unreadable" {
			fmt.Fprintf(&text, "; enclosing %s memory.max %s", strings.TrimPrefix(d, "/sys/fs/cgroup"), limit)
		}
	}
	return text.String()
}

// peakRSS is process pid's peak resident memory, in bytes, since it
// started: /proc/<pid>/status's VmHWM.
func peakRSS(pid int) (uint64, error) {
	kib, err := procField("/proc/"+strconv.Itoa(pid)+"/status", "VmHWM:")
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(kib, " kB"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("VmHWM %q: %w", kib, err)
	}
	return n * 1024, nil
}

// procField is the value of the first line of the file at path that
// starts with key, trimmed.
func procField(path, key string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }() // read-only: nothing to lose
	lines := bufio.NewScanner(f)
	for lines.Scan() {
		if value, ok := strings.CutPrefix(lines.Text(), key); ok {
			return strings.TrimSpace(value), nil
		}
	}
	if err := lines.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%s has no %s line", path, key)
}

// cpuInfoPath is the host's processor and board information.
var cpuInfoPath = "/proc/cpuinfo"

// environment describes what a run is reproduced from: the commit, the
// target it drives (describeTarget), the tool's Go, the machine, the disk
// under the database, and the CPUs and memory the processes may use.
func environment(target, binary string) (full string, setup [][2]string, err error) {
	var b strings.Builder
	c, err := commit()
	if err != nil {
		return "", nil, err
	}
	fmt.Fprintf(&b, "commit:   %s\n", c)
	bin, err := describeTarget(target, binary)
	if err != nil {
		return "", nil, err
	}
	fmt.Fprintf(&b, "broker:   %s\n", bin)
	tool, err := describeTool()
	if err != nil {
		return "", nil, err
	}
	fmt.Fprintf(&b, "tool:     %s\n", tool)
	// x86 names its processor in /proc/cpuinfo; ARM boards (a Raspberry Pi)
	// name the board in /proc/cpuinfo's Model line instead. Some machines
	// supply neither; their model is reported as unknown.
	model, err := procField(cpuInfoPath, "model name")
	if err != nil {
		model, _ = procField(cpuInfoPath, "Model")
	}
	model = cmp.Or(strings.TrimSpace(strings.TrimPrefix(model, ":")), "model unknown")
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return "", nil, err
	}
	memory, _ := procField("/proc/meminfo", "MemTotal:") // reported as found
	fmt.Fprintf(&b, "machine:  %s, %d logical cores usable, memory %s, Linux %s\n",
		model, runtime.NumCPU(), memory, strings.TrimSpace(string(kernel)))
	fmt.Fprintf(&b, "disk:     %s\n", disk(os.TempDir()))
	fmt.Fprintf(&b, "memory:   the broker runs in its own systemd scope, MemoryMax %s, swap off; the tool's cgroup %s\n",
		brokerMemory, cgroupMemory(os.Getpid()))
	sources := strings.TrimPrefix(tool, "sources sha256 ")
	kib, _ := strconv.ParseFloat(strings.TrimSuffix(memory, " kB"), 64)
	short, dirty, _ := strings.Cut(c, " ")
	setup = [][2]string{
		{"Machine", fmt.Sprintf("%s, %d logical cores, %.0f GiB RAM, Linux %s", model,
			runtime.NumCPU(), kib/(1<<20), strings.TrimSpace(string(kernel)))},
		{"Disk", disk(os.TempDir())},
		{"Commit", strings.TrimSpace(short[:7] + " " + dirty)},
		{"Tool", "`make load` (internal/harness/cmd/load), sources sha256 " + sources[:12] + "; clients on the same host, loopback TCP"},
		{"Bounds", "the broker in its own systemd scope: " + brokerMemory + ", no swap; the tool under make's bound"},
	}
	return b.String(), setup, nil
}

// toolSources are the load tool's source files, by pattern, from the
// repository's root: the command and the harness it builds on.
var toolSources = []string{"internal/harness/cmd/load/*.go", "internal/harness/*.go"}

// describeTool is the load tool's identity: the SHA-256 of its sources and
// of its executable, its Go version and build settings, and its garbage
// collector's settings.
func describeTool() (string, error) {
	h := sha256.New()
	n := 0
	for _, pattern := range toolSources {
		files, err := filepath.Glob(pattern)
		if err != nil {
			return "", err
		}
		for _, f := range files { // Glob sorts its matches
			data, err := os.ReadFile(f)
			if err != nil {
				return "", err
			}
			_, _ = fmt.Fprintf(h, "%s %d\n", f, len(data)) // a hash never fails to write
			h.Write(data)
			n++
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	exeSum, err := fileSum(exe)
	if err != nil {
		return "", err
	}
	var settings []string
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			settings = append(settings, s.Key+"="+s.Value)
		}
	}
	gcPercent := debug.SetGCPercent(-1)
	debug.SetGCPercent(gcPercent) // read, and put back as it was
	return fmt.Sprintf("sources sha256 %s (%d files: %s), executable sha256 %s, %s, build %s, GOMAXPROCS %d, GOGC %d, memory limit %d bytes (environment GOGC=%q GOMEMLIMIT=%q, which the broker inherits)",
		hex.EncodeToString(h.Sum(nil)), n, strings.Join(toolSources, " "), exeSum, runtime.Version(), strings.Join(settings, " "),
		runtime.GOMAXPROCS(0), gcPercent, debug.SetMemoryLimit(-1), os.Getenv("GOGC"), os.Getenv("GOMEMLIMIT")), nil
}

// fileSum is the SHA-256 of the file at path, in hexadecimal.
func fileSum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }() // read-only: nothing to lose
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// describeTarget is what a run drives: Sagüin, started from binary
// (describeBinary); Mosquitto, by its binary and version line; or the
// in-process test peer, which starts no broker.
func describeTarget(target, binary string) (string, error) {
	switch target {
	case "mosquitto":
		return mosquittoBinary + ", " + mosquittoVersion(), nil
	case "peer":
		return "none: the in-process test peer", nil
	}
	return describeBinary(binary)
}

// describeBinary is the binary's path, SHA-256 and build: its Go version
// and the commit it was built from, as the build recorded them.
func describeBinary(binary string) (string, error) {
	sum, err := fileSum(binary)
	if err != nil {
		return "", err
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		return "", fmt.Errorf("reading %s's build: %w", binary, err)
	}
	vcs := map[string]string{}
	for _, s := range info.Settings {
		vcs[s.Key] = s.Value
	}
	return fmt.Sprintf("%s, sha256 %s, %s, built from %s (modified %s)", binary, sum,
		info.GoVersion, cmp.Or(vcs["vcs.revision"], "unrecorded"), cmp.Or(vcs["vcs.modified"], "unrecorded")), nil
}

// disk names the file system holding dir, its device and the device's
// model, from /proc/self/mountinfo and /sys.
func disk(dir string) string {
	m, err := mountOf(dir)
	if err != nil {
		return "unknown: " + err.Error()
	}
	model := "model unknown"
	if dev, err := filepath.EvalSymlinks("/sys/class/block/" + filepath.Base(m.source)); err == nil {
		for _, d := range []string{dev, filepath.Dir(dev)} {
			if m, err := os.ReadFile(filepath.Join(d, "device", "model")); err == nil {
				model = strings.TrimSpace(string(m))
				break
			}
		}
	}
	return fmt.Sprintf("%s, %s on %s (%s), mounted at %s", dir, m.fstype, m.source, model, m.point)
}

// inside reports whether path is mount point or below it.
func inside(path, point string) bool {
	return point == "/" || path == point || strings.HasPrefix(path, point+"/")
}

// commit is the commit measured, git rev-parse HEAD, marked when the
// working tree holds changes the commit does not.
func commit() (string, error) {
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	status, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return "", fmt.Errorf("git status: %w", err)
	}
	c := strings.TrimSpace(string(head))
	if len(bytes.TrimSpace(status)) > 0 {
		c += " (with uncommitted changes)"
	}
	return c, nil
}
