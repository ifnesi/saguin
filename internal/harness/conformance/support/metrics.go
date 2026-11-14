package support

import (
	"maps"
	"slices"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Sampled returns each series' value on b's /metrics now, each named as
// testkit.SeriesKey names it.
func Sampled(t *testing.T, b *harness.Broker, series ...string) map[string]float64 {
	t.Helper()
	e := testkit.ParseScrape(t, testkit.Scrape(t, b.Metrics()))
	got := make(map[string]float64, len(series))
	for _, s := range series {
		v, ok := e.Values[s]
		if !ok {
			t.Fatalf("the scrape has no series %s", s)
		}
		got[s] = v
	}
	return got
}

// AwaitSampled waits until each series of want reads its value on b's
// /metrics, each named as testkit.SeriesKey names it, failing the test,
// what naming the moment, where they do not within harness.DefaultTimeout:
// a value moved on a goroutine the test cannot wait on is seen only by
// polling the scrape.
func AwaitSampled(t *testing.T, b *harness.Broker, what string, want map[string]float64) {
	t.Helper()
	var got map[string]float64
	if !testkit.Within(harness.DefaultTimeout, func() bool {
		got = Sampled(t, b, slices.Collect(maps.Keys(want))...)
		return maps.Equal(got, want)
	}) {
		t.Fatalf("%s: /metrics reads %v; want %v", what, got, want)
	}
}
