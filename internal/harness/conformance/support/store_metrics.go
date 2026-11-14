package support

import (
	"github.com/ifnesi/saguin/internal/testkit"
)

// ChannelSeries is the series of metric for channel, as testkit.SeriesKey
// names it.
func ChannelSeries(metric, channel string, labels ...string) string {
	return testkit.SeriesKey(metric, append([]string{"channel", channel}, labels...)...)
}
