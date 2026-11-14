// Package logger serializes Sagüin's log output through one goroutine.
package logger

import (
	"io"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/metrics"
)

// Capacity bounds outstanding lines, until their write results are processed.
const Capacity = 1024

// Level is the severity of a log line.
type Level uint8

// Log levels, from least to most severe.
const (
	Debug Level = iota
	Info
	Warn
	Error
)

const (
	timestampLayout  = "2006-01-02T15:04:05.000Z07:00"
	maxRetainedBytes = 64 << 10
)

var levelNames = [...]string{"DEBUG", "INFO", "WARN", "ERROR"}

// Line records an event's time, severity and message. Its sender admits it.
type Line struct {
	At    time.Time
	Level Level
	Msg   string
}

// Result says only that one line's output write is done, returning its credit
// when processed. The logger counts failed or short writes as refused.
type Result struct{}

// Credits owns log admission. Main hands it to the core while the core runs,
// taking it back only after the core ends. Its owners never overlap (R1).
type Credits struct {
	level Level
	free  int
	full  *metrics.Counter
}

// NewCredits creates the log credits and registers admission's loss counter.
func NewCredits(level Level, reg *metrics.Registry) *Credits {
	if level > Error {
		panic("logger: invalid log level")
	}
	return &Credits{level: level, free: Capacity,
		full: reg.Counter("saguin_log_lines_dropped_total", "cause", "full")}
}

// Admit takes one credit for an eligible line, counting it as full if none
// remains. A line below the configured level takes nothing and counts nothing.
func (c *Credits) Admit(level Level) bool {
	return c.AdmitBatch(level, 1) != 0
}

// AdmitBatch reserves credits for one event's eligible lines. It returns the
// admitted prefix and counts the rest as full without retaining their text.
// No output results are processed between members of the batch.
func (c *Credits) AdmitBatch(level Level, count uint64) int {
	if level > Error {
		panic("logger: invalid log level")
	}
	if level < c.level {
		return 0
	}
	n := min(count, uint64(c.free))
	if count != n {
		c.full.Add(count - n)
	}
	c.free -= int(n)
	return int(n)
}

// SetLevel changes admission on its current owner, without touching output.
func (c *Credits) SetLevel(level Level) { c.level = level }

// Return processes a result, releasing its line's credit without counting.
func (c *Credits) Return(Result) { c.free++ }

// Logger owns the output and reports one result per admitted line, in order.
type Logger struct {
	out     io.Writer
	id      string
	lines   chan Line
	results chan Result
	stop    chan struct{}
	done    chan struct{}
	until   time.Time // final drain's deadline, published by closing stop
	refused *metrics.Counter
	running *metrics.Gauge
}

// Start creates a logger and starts the one goroutine which owns out.
func Start(out io.Writer, id string, reg *metrics.Registry) *Logger {
	l := &Logger{
		out:     out,
		id:      id,
		lines:   make(chan Line, Capacity),
		results: make(chan Result, Capacity),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		refused: reg.Counter("saguin_log_lines_dropped_total", "cause", "refused"),
		running: reg.Goroutine("logger"),
	}
	go l.run()
	return l
}

// Lines has room for every line whose sender holds a credit (R2).
func (l *Logger) Lines() chan<- Line { return l.lines }

// Results returns one completion per line, in line order. It has room for every
// outstanding line, so output never waits for admission's owner (R2, R3).
func (l *Logger) Results() <-chan Result { return l.results }

// StopWithin bounds the exit's wait for queued log lines. The output belongs
// to the logger: main may finish even if that output remains stuck in Write.
// This is the only way to stop the logger and is called once, after all other
// broker goroutines end.
func (l *Logger) StopWithin(timeout time.Duration) {
	l.until = time.Now().Add(timeout)
	timer := time.NewTimer(time.Until(l.until))
	close(l.stop)
	defer timer.Stop()
	select {
	case <-l.done:
	case <-timer.C:
	}
}

func (l *Logger) run() {
	l.running.Add(1)
	defer l.running.Add(-1)
	defer close(l.done)
	var buf []byte
	write := func(entry Line) {
		buf = l.writeLine(buf[:0], entry)
		if cap(buf) > maxRetainedBytes {
			// One exceptional message must not make the process retain its large
			// backing array for every later, ordinary line.
			buf = nil
		}
	}
	drain := func() {
		// Snapshotting the queue writes exactly what was waiting when stop was
		// observed; draining until empty could chase active senders forever.
		for range len(l.lines) {
			if !l.until.IsZero() && !time.Now().Before(l.until) {
				return
			}
			write(<-l.lines)
		}
	}
	for {
		// Prefer an already-requested stop instead of continuing to consume a
		// continuously busy line channel.
		select {
		case <-l.stop:
			drain()
			return
		default:
		}
		select {
		case <-l.stop:
			drain()
			return
		case entry := <-l.lines:
			write(entry)
		}
	}
}

func (l *Logger) writeLine(buf []byte, entry Line) []byte {
	buf = entry.At.UTC().AppendFormat(buf, timestampLayout)
	buf = append(buf, ' ')
	buf = append(buf, levelNames[entry.Level]...)
	buf = append(buf, ' ')
	buf = append(buf, l.id...)
	buf = append(buf, ' ')
	buf = appendMessage(buf, entry.Msg)
	buf = append(buf, '\n')

	// Keep a line to one Write. The io.Writer contract requires a short write
	// to return an error, but count either signal in case an output breaks it.
	n, err := l.out.Write(buf)
	if err != nil || n < len(buf) {
		l.refused.Inc()
	}
	l.results <- Result{}
	return buf
}

func appendMessage(dst []byte, msg string) []byte {
	first := strings.IndexAny(msg, "\n\r")
	if first < 0 {
		return append(dst, msg...)
	}
	start := 0
	for i := first; i < len(msg); i++ {
		var escaped byte
		switch msg[i] {
		case '\n':
			escaped = 'n'
		case '\r':
			escaped = 'r'
		default:
			continue
		}
		dst = append(dst, msg[start:i]...)
		dst = append(dst, '\\', escaped)
		start = i + 1
	}
	return append(dst, msg[start:]...)
}
