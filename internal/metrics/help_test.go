package metrics

import (
	"testing"

	"github.com/ifnesi/saguin/internal/testkit"
)

// TestHelpIsMetricsMeaning: metrics.md, "Each series carries its Meaning
// below as its HELP line, as plain text." Every series metrics.md lists has
// that text, and nothing else has any, so a series the broker serves
// carries its document's words whether or not a test's broker serves it.
func TestHelpIsMetricsMeaning(t *testing.T) {
	rows, _ := testkit.MetricsDoc(t, "../../docs/metrics.md")
	documented := map[string]bool{}
	for _, r := range rows {
		documented[r.Name] = true
		if got, want := help[r.Name], testkit.PlainMeaning(r.Meaning); got != want {
			t.Errorf("%s's HELP is %q; metrics.md's Meaning is %q", r.Name, got, want)
		}
	}
	for name := range help {
		if !documented[name] {
			t.Errorf("%s has a HELP text but no row in metrics.md", name)
		}
	}
}
