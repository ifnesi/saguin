package testkit

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// MetricsRow is one series a row of metrics.md's tables names.
type MetricsRow struct {
	Name    string
	Labels  []string // the label names the row gives it, in its order
	Kind    string   // its Type column: gauge or counter
	Section string   // the heading it is listed under
	Meaning string   // its Meaning column, as metrics.md writes it
}

// SeriesCell matches one series a row's Series column names, with its
// label names.
var SeriesCell = regexp.MustCompile("`(saguin_[a-z0-9_]+)(?:\\{([a-z_,]+)\\})?`")

// MetricsDoc reads the tables of metrics.md at path, and its Rules
// section's text, so that what they list is what a test checks.
func MetricsDoc(t testing.TB, path string) (rows []MetricsRow, rules string) {
	t.Helper()
	var rulesText strings.Builder
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	section := ""
	for line := range strings.Lines(string(text)) {
		if h, ok := strings.CutPrefix(line, "## "); ok {
			section = strings.TrimSpace(h)
			continue
		}
		if section == "Rules" {
			rulesText.WriteString(line)
		}
		if !strings.HasPrefix(line, "| `saguin_") {
			continue
		}
		cells := strings.Split(strings.TrimSpace(line), "|")
		if len(cells) < 5 {
			t.Fatalf("metrics.md: a row of fewer than three columns: %q", line)
		}
		meaning := strings.Join(cells[3:len(cells)-1], "|")
		for _, m := range SeriesCell.FindAllStringSubmatch(cells[1], -1) {
			r := MetricsRow{Name: m[1], Kind: strings.TrimSpace(cells[2]), Section: section, Meaning: meaning}
			if m[2] != "" {
				r.Labels = strings.Split(m[2], ",")
			}
			rows = append(rows, r)
		}
	}
	rules = rulesText.String()
	if len(rows) < 80 || rules == "" {
		t.Fatalf("metrics.md read as %d series and %d bytes of rules: too few to be the document", len(rows), len(rules))
	}
	return rows, rules
}

// markdownLink matches a Markdown link, its text first.
var markdownLink = regexp.MustCompile(`\[([^\]]+)\]\([^)]*\)`)

// PlainMeaning is a Meaning as plain text (metrics.md: "Each series carries
// its Meaning below as its HELP line, as plain text"): a link becomes its
// text and a code span its content.
func PlainMeaning(meaning string) string {
	return strings.TrimSpace(strings.ReplaceAll(markdownLink.ReplaceAllString(meaning, "$1"), "`", ""))
}
