package main

import (
	"math"
	"math/bits"
	"time"
)

// histogram counts latencies in a fixed number of buckets, so its memory
// does not grow with the deliveries: one bucket per microsecond below
// histSub microseconds, then histSub buckets per power of two, each about
// 1.6% wide, up to 2^histTop microseconds (about 71 minutes). A latency is
// counted in whole microseconds rounded up, and a percentile read as its
// bucket's upper bound, so never below the true value. A latency past
// the top is counted apart, as overflow, and a percentile that falls
// among the overflow says so. The maintainer allowed this histogram, the
// load tool's own, as an exception for the test tool: neither the standard
// library nor an approved dependency offers one.
type histogram struct {
	counts   [histBuckets]uint64
	overflow uint64
	total    uint64
}

const (
	histShift   = 6
	histSub     = 1 << histShift
	histTop     = 32
	histBuckets = (histTop - histShift + 1) * histSub
)

// bucket is the bucket of v microseconds, and false past the top.
func bucket(v uint64) (int, bool) {
	if v < histSub {
		return int(v), true
	}
	exp := bits.Len64(v) - histShift - 1 // v's bits below its top histShift+1
	if exp >= histTop-histShift {
		return 0, false
	}
	return (exp+1)*histSub + int(v>>exp)&(histSub-1), true
}

// upper is the highest value, in microseconds, bucket i counts.
func upper(i int) uint64 {
	if i < histSub {
		return uint64(i)
	}
	exp := i/histSub - 1
	return (uint64(histSub+i%histSub)+1)<<exp - 1
}

// micros is d in whole microseconds, rounded up; a negative d is zero.
func micros(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64((d + time.Microsecond - 1) / time.Microsecond)
}

// add counts one latency.
func (h *histogram) add(d time.Duration) {
	h.total++
	if i, ok := bucket(micros(d)); ok {
		h.counts[i]++
	} else {
		h.overflow++
	}
}

// merge adds o's counts to h's.
func (h *histogram) merge(o *histogram) {
	for i, n := range &o.counts {
		h.counts[i] += n
	}
	h.overflow += o.overflow
	h.total += o.total
}

// A quantile is one percentile read from a histogram.
type quantile struct {
	value time.Duration
	// none is set where the histogram counted nothing; over where the
	// percentile falls among the overflow, value then being the top.
	none, over bool
}

// quantile is the nearest-rank quantile q of what h counted.
func (h *histogram) quantile(q float64) quantile {
	if h.total == 0 {
		return quantile{none: true}
	}
	rank := max(uint64(math.Ceil(q*float64(h.total))), 1)
	var seen uint64
	for i, n := range &h.counts {
		if seen += n; seen >= rank {
			return quantile{value: time.Duration(upper(i)) * time.Microsecond}
		}
	}
	return quantile{value: time.Duration(upper(histBuckets-1)) * time.Microsecond, over: true}
}
