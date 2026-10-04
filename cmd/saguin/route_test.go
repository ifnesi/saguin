package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// routeConfig writes a configuration whose channels are placed by filter
// and overlap on purpose, which is the only shape that tests anything: two
// filters matching one topic is what the exactness rule exists to settle,
// and a broker whose channels are disjoint would answer the same however
// that rule were written.
func routeConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(path, []byte(`
broker:
  id: t
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: none
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
channels:
  - location:
      type: latest
      filter: iot/+/location/+
  - readings:
      type: append
      filter: iot/+/+/+
  - work:
      type: queue
      filter: iot/tasks/+/+
      visibility_timeout: 30s
      retry:
        max_attempts: 3
`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// **Batch mode: topics and filters in, four tab-separated columns out.**
//
// The whole line is asserted rather than a substring, because the columns
// are the contract: `awk -F'\t' '$3 == "broadcast"'` is how an operator
// finds every topic nothing claims, and a column inserted before that one
// breaks the command silently. A test matching "broadcast" anywhere in the
// output would not notice.
func TestRouteBatchAnswersAListOfTopics(t *testing.T) {
	cfg := routeConfig(t)

	// A comment, a blank line, and a bare `#` - which is a legal filter and
	// not a comment, and the two are told apart by there being nothing else
	// on the line. Getting that backwards would silently drop the widest
	// question anybody asks.
	in := strings.NewReader(strings.Join([]string{
		"# my fleet",
		"iot/depot/location/dev-1",
		"",
		"iot/depot/flow/dev-1",
		"iot/depot/location/dev-1/raw",
		"iot/tasks/settle/7dbec6ce",
		"$saguin/queue/work",
		"iot/tasks/+/+",
		"$saguin/queue/work/settle",
		"$share/g/iot/depot/+/+",
	}, "\n"))

	var out, bad bytes.Buffer
	if code := routeTopic([]string{cfg}, in, &out, &bad, true, asColumns); code != 0 {
		t.Fatalf("batch exited %d: %s", code, bad.String())
	}
	if bad.Len() != 0 {
		t.Errorf("a batch run that succeeded wrote to the refusal stream: %q", bad.String())
	}

	want := []string{
		// The header, first and always, for the reason --acl's is.
		"# topic-or-filter\tchannel\ttype\twhy",
		// Two filters match this one and the more exact takes it, which is
		// the row that would pass whatever the ordering rule said if the
		// two channels did not overlap.
		"iot/depot/location/dev-1\tlocation\tlatest\tiot/+/location/+",
		"iot/depot/flow/dev-1\treadings\tappend\tiot/+/+/+",
		// Five levels against filters of four: nothing claims it.
		"iot/depot/location/dev-1/raw\t-\tbroadcast\t-",
		"iot/tasks/settle/7dbec6ce\twork\tqueue\tiot/tasks/+/+",
		// A queue's form is its channel's name, and it is a worker. The two
		// rows under it are what a reason code has to tell apart: the
		// channel's *filter*, which lies inside the queue and is not its
		// form; and a name with a level after it, which no channel can have.
		// The last is a shared subscription, which is refused nowhere and is
		// served live records from every channel it reaches.
		"$saguin/queue/work\twork\tqueue\tworker",
		"iot/tasks/+/+\twork\tqueue\trefused-0x8F",
		"$saguin/queue/work/settle\t-\t-\trefused-0x8F",
		"$share/g/iot/depot/+/+\tlocation\tlatest\tlive-shared",
		"$share/g/iot/depot/+/+\treadings\tappend\tlive-shared",
	}
	got := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d:\n%s", len(got), len(want), out.String())
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d:\n  got  %q\n  want %q", i+1, got[i], want[i])
		}
	}
}

// A filter reaching several channels is several lines, the input repeated
// in the first column on each. One line would have to invent a summary of
// three different delivery semantics, and the thing an operator wants to
// see is exactly that there are three.
func TestRouteBatchGivesAWideFilterALinePerChannel(t *testing.T) {
	var out, bad bytes.Buffer
	if code := routeTopic([]string{routeConfig(t)},
		strings.NewReader("#\n"), &out, &bad, true, asColumns); code != 0 {
		t.Fatalf("batch exited %d: %s", code, bad.String())
	}

	want := map[string]string{
		"location": "#\tlocation\tlatest\tcurrent",
		"readings": "#\treadings\tappend\treplayed",
		// Crossed and left out of it: a wildcard never reaches a queue,
		// and saying so is more use than leaving the row off.
		"work": "#\twork\tqueue\texcluded",
		// **The channel nobody configured.** A queue derives a dead-letter
		// channel, so `#` reaches one more channel than the file lists -
		// which is exactly the sort of thing an operator finds out from
		// this command rather than by reading, and the reason the row is
		// asserted rather than allowed to be missing.
		"work__dlq": "#\twork__dlq\tappend\treplayed",
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	// The header is the first line and is not an answer. Asserting it here
	// as well as in the table above is what stops it being counted as one:
	// a header that stopped being printed would otherwise leave this test
	// one line short and looking like a missing channel.
	if header := "# topic-or-filter\tchannel\ttype\twhy"; lines[0] != header {
		t.Fatalf("first line:\n  got  %q\n  want %q", lines[0], header)
	}
	lines = lines[1:]
	if len(lines) != len(want) {
		t.Fatalf("`#` produced %d answer lines, want %d:\n%s", len(lines), len(want), out.String())
	}
	seen := map[string]bool{}
	for _, line := range lines {
		f := strings.Split(line, "\t")
		if len(f) != 4 {
			t.Fatalf("line %q has %d columns, want 4", line, len(f))
		}
		if w, ok := want[f[1]]; !ok || w != line {
			t.Errorf("line for %q:\n  got  %q\n  want %q", f[1], line, w)
		}
		seen[f[1]] = true
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("`#` never mentioned channel %q, so a subscriber would meet it unwarned", name)
		}
	}
}

// The same guard --acl has: a forgotten argument must refuse rather than
// read a terminal and look like a hang.
func TestRouteWithNoSubjectAndNoPipeRefusesRatherThanWaiting(t *testing.T) {
	var out, bad bytes.Buffer
	if code := routeTopic([]string{routeConfig(t)}, nil, &out, &bad, false, asColumns); code == 0 {
		t.Fatal("a batch run with nothing piped in was accepted, so it would have read a terminal")
	}
	if !strings.Contains(bad.String(), "nothing piped in") {
		t.Errorf("the refusal does not say why:\n%s", bad.String())
	}
	if out.Len() != 0 {
		t.Errorf("a refusal wrote to standard output: %q", out.String())
	}
}

// **A comment is a comment whether or not it holds a slash.**
//
// A bare `#` is a legal filter, so a line starting with `#` cannot simply
// be skipped - and the rule used to be "skip it unless it holds a `/`",
// which let through exactly the comments a file of topics is most likely
// to carry. `# my/fleet topics` was answered as though somebody had asked
// where it lands, and reported as broadcast, in the middle of the answers.
//
// Nothing else beginning with `#` can be a question: MQTT allows the
// multi-level wildcard only as a whole filter on its own, and a topic a
// client publishes to may not hold a wildcard at all. So the rule is the
// simpler one, and this test holds it to both halves at once - the comment
// with a slash must vanish, and the bare `#` must still be answered.
func TestACommentWithASlashIsStillAComment(t *testing.T) {
	in := strings.NewReader(strings.Join([]string{
		"# my/fleet topics",
		"# plain note",
		"#",
		"iot/depot/location/dev-1",
	}, "\n"))

	var out, bad bytes.Buffer
	if code := routeTopic([]string{routeConfig(t)}, in, &out, &bad, true, asColumns); code != 0 {
		t.Fatalf("batch exited %d: %s", code, bad.String())
	}
	got := out.String()
	if strings.Contains(got, "my/fleet") {
		t.Errorf("a comment holding a slash was answered as a topic:\n%s", got)
	}
	if strings.Contains(got, "plain note") {
		t.Errorf("a comment was answered:\n%s", got)
	}
	// The half that stops the fix going too far: `#` on its own is the
	// widest question anybody asks and must still be answered.
	if !strings.Contains(got, "#\tlocation\tlatest\tcurrent") {
		t.Errorf("the bare `#` filter was skipped as a comment:\n%s", got)
	}
	if !strings.Contains(got, "iot/depot/location/dev-1\tlocation\tlatest\tiot/+/location/+") {
		t.Errorf("the topic after the comments was not answered:\n%s", got)
	}
}

// **--json is the same answers in another shape, not another answer.**
//
// The two renderings share one computation for that reason, and this holds
// them to it: every column of the list form must appear as a field, with
// the `-` that means "nothing here" as null rather than as the string "-",
// which a consumer testing for absence would not recognise.
func TestJSONCarriesTheSameAnswersAsTheColumns(t *testing.T) {
	cfg := routeConfig(t)
	subjects := "iot/depot/location/dev-1\niot/depot/location/dev-1/raw\n$share/g/iot/depot/+/+\n"

	var cols, bad bytes.Buffer
	if code := routeTopic([]string{cfg}, strings.NewReader(subjects), &cols, &bad, true, asColumns); code != 0 {
		t.Fatalf("columns exited %d: %s", code, bad.String())
	}
	var js bytes.Buffer
	if code := routeTopic([]string{cfg}, strings.NewReader(subjects), &js, &bad, true, asJSON); code != 0 {
		t.Fatalf("json exited %d: %s", code, bad.String())
	}

	// No header in JSON: the keys are the header, and a `#` line is not
	// valid JSON, so a consumer would have to strip it before parsing.
	if strings.HasPrefix(js.String(), "#") {
		t.Errorf("the JSON form printed the column header:\n%s", js.String())
	}

	// **Against the literal text, before comparing the two forms.** The
	// check below holds the JSON to the columns, which is what it is for -
	// but on its own it passes when both are wrong the same way. Making
	// `-` a non-empty string would put "-" in the JSON and leave the
	// columns unchanged, and every field would still agree with its
	// column. So one known-absent answer is asserted as it is written.
	if !strings.Contains(js.String(), `"channel":null`) {
		t.Errorf("no answer wrote a null channel, so a topic no channel claims is "+
			"being given one - a consumer testing for absence would not see it:\n%s",
			js.String())
	}

	colLines := strings.Split(strings.TrimRight(cols.String(), "\n"), "\n")[1:]
	jsLines := strings.Split(strings.TrimRight(js.String(), "\n"), "\n")
	if len(colLines) != len(jsLines) {
		t.Fatalf("%d column lines and %d JSON lines:\n%s\n%s",
			len(colLines), len(jsLines), cols.String(), js.String())
	}
	for i := range colLines {
		f := strings.Split(colLines[i], "\t")
		var got routeAnswer
		if err := json.Unmarshal([]byte(jsLines[i]), &got); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", i+1, err, jsLines[i])
		}
		// Each field against its column, with `-` read as null. Asserting
		// the pair rather than the JSON alone is what makes this a test
		// that the two agree, rather than two records of what each says.
		for _, c := range []struct {
			what   string
			column string
			field  *string
		}{
			{"subject", f[0], &got.Subject},
			{"channel", f[1], got.Channel},
			{"type", f[2], got.Type},
			{"why", f[3], got.Why},
		} {
			if c.column == "-" {
				if c.field != nil {
					t.Errorf("line %d: the %s column is %q and the field is %q, want null",
						i+1, c.what, c.column, *c.field)
				}
				continue
			}
			if c.field == nil {
				t.Errorf("line %d: the %s column is %q and the field is null",
					i+1, c.what, c.column)
				continue
			}
			if *c.field != c.column {
				t.Errorf("line %d, %s:\n  column %q\n  field  %q",
					i+1, c.what, c.column, *c.field)
			}
		}
	}
}
