package testkit

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ScrapeOption is a check a scrape or a series lookup adds.
type ScrapeOption func(*scrapeChecks)

type scrapeChecks struct {
	count    bool          // WriteTo's count is the bytes it wrote
	backstop time.Duration // the scrape runs on a goroutine of its own, bounded so
	typed    string        // the series' family is declared of this TYPE first
}

// CountChecked has the scrape check that WriteTo returns the number of
// bytes it wrote (io.WriterTo).
func CountChecked() ScrapeOption {
	return func(c *scrapeChecks) { c.count = true }
}

// Backstop has the scrape run on a goroutine of its own and fail the test
// if it takes longer than limit, so a hung scrape cannot hang the test.
func Backstop(limit time.Duration) ScrapeOption {
	return func(c *scrapeChecks) { c.backstop = limit }
}

// Typed has a series lookup check that the series' metric is declared with
// a "# TYPE <name> <kind>" line before the series.
func Typed(kind string) ScrapeOption {
	return func(c *scrapeChecks) { c.typed = kind }
}

// Scrape returns what w writes: a metric registry's whole scrape.
func Scrape(t testing.TB, w io.WriterTo, opts ...ScrapeOption) string {
	t.Helper()
	var c scrapeChecks
	for _, o := range opts {
		o(&c)
	}
	var buf bytes.Buffer
	var n int64
	var err error
	write := func() { n, err = w.WriteTo(&buf) }
	if c.backstop == 0 {
		write()
	} else if !ReturnsWithin(c.backstop, write) {
		t.Fatalf("the scrape did not return within %v", c.backstop)
	}
	if err != nil {
		t.Fatalf("rendering the registry: %v", err)
	}
	if c.count && n != int64(buf.Len()) {
		t.Fatalf("WriteTo returned %d bytes written, wrote %d", n, buf.Len())
	}
	return buf.String()
}

// Sample returns the value of one series, written as in the scrape (its
// name and labels), from a scrape of w.
func Sample(t testing.TB, w io.WriterTo, series string, opts ...ScrapeOption) float64 {
	t.Helper()
	var c scrapeChecks
	for _, o := range opts {
		o(&c)
	}
	text := Scrape(t, w, opts...)
	name, _, _ := strings.Cut(series, "{")
	typed := false
	for line := range strings.SplitSeq(text, "\n") {
		if c.typed != "" && line == "# TYPE "+name+" "+c.typed {
			typed = true
		}
		value, ok := strings.CutPrefix(line, series+" ")
		if !ok {
			continue
		}
		if c.typed != "" && !typed {
			t.Fatalf("%s is rendered without its TYPE %s line before it:\n%s", series, c.typed, text)
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			t.Fatalf("%s has value %q: %v", series, value, err)
		}
		return v
	}
	t.Fatalf("the scrape has no series %s:\n%s", series, text)
	return 0
}

// AwaitSample waits until one series of w reads want, failing the test
// where it does not within limit. A metric moved on a goroutine the test
// cannot wait on (a connection's, the core's) is seen only by polling the
// scrape.
func AwaitSample(t testing.TB, w io.WriterTo, series string, want float64, limit time.Duration) {
	t.Helper()
	if !Within(limit, func() bool { return Sample(t, w, series) == want }) {
		t.Fatalf("%s = %v after %v, want %v", series, Sample(t, w, series), limit, want)
	}
}

// Exposition is one scrape read by the rules of the Prometheus text format.
type Exposition struct {
	Types   map[string]string  // metric name to its TYPE
	Values  map[string]float64 // series, as SeriesKey names it, to its value
	Samples []Scraped          // every sample, in the scrape's order
}

// Scraped is one sample line of a scrape: its metric name, its labels by
// name, and its value.
type Scraped struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// SeriesKey names one series for Exposition.Values: its metric name and
// its labels, name and value pairs in any order.
func SeriesKey(name string, labels ...string) string {
	var pairs []string
	for i := 0; i+1 < len(labels); i += 2 {
		pairs = append(pairs, labels[i]+"="+strconv.Quote(labels[i+1]))
	}
	slices.Sort(pairs)
	return name + "{" + strings.Join(pairs, ",") + "}"
}

// MissedSeries is saguin_missed_total of channel, cause retention
// (metrics.md, Delivery).
func MissedSeries(channel string) string {
	return fmt.Sprintf(`saguin_missed_total{channel=%q,cause="retention"}`, channel)
}

// ParseExposition reads a scrape, refusing one that breaks the Prometheus
// text format: each metric name has one TYPE line, before its samples,
// which form one group; no series appears twice; the text ends with a line
// break. metrics.md gives every series a type, so a sample without a TYPE
// line is refused too.
func ParseExposition(text string) (Exposition, error) {
	e := Exposition{Types: map[string]string{}, Values: map[string]float64{}}
	if !strings.HasSuffix(text, "\n") {
		return e, errors.New("the scrape does not end with a line break")
	}
	current := "" // the metric whose TYPE line was read last
	for i, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		at := fmt.Sprintf("line %d %q", i+1, line)
		switch {
		case line == "":
		case strings.HasPrefix(line, "# TYPE "):
			f := strings.Fields(line)
			if len(f) != 4 || !metricName(f[2]) {
				return e, fmt.Errorf("%s: malformed TYPE line", at)
			}
			if _, ok := e.Types[f[2]]; ok {
				return e, fmt.Errorf("%s: a second TYPE line for %s", at, f[2])
			}
			switch f[3] {
			case "counter", "gauge", "histogram", "summary", "untyped":
			default:
				return e, fmt.Errorf("%s: unknown type %s", at, f[3])
			}
			e.Types[f[2]] = f[3]
			current = f[2]
		case strings.HasPrefix(line, "#"):
			// HELP lines and comments.
		default:
			name, labels, value, err := sampleLine(line)
			if err != nil {
				return e, fmt.Errorf("%s: %w", at, err)
			}
			if _, ok := e.Types[name]; !ok {
				return e, fmt.Errorf("%s: no TYPE line before the first sample of %s", at, name)
			}
			if name != current {
				return e, fmt.Errorf("%s: the samples of %s are not one group after its TYPE line", at, name)
			}
			key := SeriesKey(name, labels...)
			if _, ok := e.Values[key]; ok {
				return e, fmt.Errorf("%s: series %s appears twice", at, key)
			}
			e.Values[key] = value
			byName := make(map[string]string, len(labels)/2)
			for j := 0; j+1 < len(labels); j += 2 {
				byName[labels[j]] = labels[j+1]
			}
			e.Samples = append(e.Samples, Scraped{Name: name, Labels: byName, Value: value})
		}
	}
	return e, nil
}

// ParseScrape is ParseExposition, failing the test where the scrape breaks
// the format.
func ParseScrape(t testing.TB, text string) Exposition {
	t.Helper()
	e, err := ParseExposition(text)
	if err != nil {
		t.Fatalf("%v:\n%s", err, text)
	}
	return e
}

// Value returns a series' value, failing the test if the scrape lacks it.
func (e Exposition) Value(t testing.TB, name string, labels ...string) float64 {
	t.Helper()
	v, ok := e.Values[SeriesKey(name, labels...)]
	if !ok {
		t.Fatalf("the scrape has no series %s", SeriesKey(name, labels...))
	}
	return v
}

// metricName says whether s is a valid Prometheus metric name.
func metricName(s string) bool {
	for i, c := range s {
		letter := c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return s != ""
}

// sampleLine reads one sample line: a name, optional labels, a value and an
// optional timestamp. A label value's escapes are \\, \" and \n.
func sampleLine(line string) (name string, labels []string, value float64, err error) {
	end := strings.IndexAny(line, "{ ")
	if end <= 0 || !metricName(line[:end]) {
		return "", nil, 0, errors.New("no metric name")
	}
	name, rest := line[:end], line[end:]
	if strings.HasPrefix(rest, "{") {
		rest = rest[1:]
		for !strings.HasPrefix(rest, "}") {
			eq := strings.Index(rest, `="`)
			if eq <= 0 || !metricName(rest[:eq]) || strings.Contains(rest[:eq], ":") {
				return "", nil, 0, errors.New("malformed label name")
			}
			label := rest[:eq]
			rest = rest[eq+2:]
			var v strings.Builder
			for {
				if rest == "" {
					return "", nil, 0, errors.New("unterminated label value")
				}
				c := rest[0]
				rest = rest[1:]
				if c == '"' {
					break
				}
				if c == '\n' {
					return "", nil, 0, errors.New("raw line break in a label value")
				}
				if c == '\\' {
					if rest == "" {
						return "", nil, 0, errors.New("unterminated escape")
					}
					switch rest[0] {
					case '\\', '"':
						v.WriteByte(rest[0])
					case 'n':
						v.WriteByte('\n')
					default:
						return "", nil, 0, fmt.Errorf("unknown escape \\%c", rest[0])
					}
					rest = rest[1:]
					continue
				}
				v.WriteByte(c)
			}
			labels = append(labels, label, v.String())
			rest = strings.TrimPrefix(rest, ",")
		}
		rest = rest[1:]
	}
	f := strings.Fields(rest)
	if !strings.HasPrefix(rest, " ") || len(f) < 1 || len(f) > 2 {
		return "", nil, 0, errors.New("want one space, a value and an optional timestamp")
	}
	value, err = strconv.ParseFloat(f[0], 64)
	if err != nil {
		return "", nil, 0, fmt.Errorf("value: %w", err)
	}
	if len(f) == 2 {
		if _, err := strconv.ParseInt(f[1], 10, 64); err != nil {
			return "", nil, 0, fmt.Errorf("timestamp: %w", err)
		}
	}
	return name, labels, value, nil
}
