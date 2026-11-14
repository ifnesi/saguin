package main

import (
	"fmt"
	"math/rand/v2"
	"strconv"

	"github.com/ifnesi/saguin/internal/config"
)

// maxPayload is the largest payload a publication carries: what fits in a
// PUBLISH within the broker's default Maximum Packet Size,
// broker.limits.max_packet_size 1MiB (config.md), which counts the whole
// packet. publishOverhead bounds the rest of it: the fixed header (at
// most 5 bytes), the topic (2 bytes of length and load/<g>/<p>, at most
// 11 for the recipe's sizes), the packet identifier (2) and the property
// length (1).
const (
	packetLimit     = 1 << 20
	publishOverhead = 32
	maxPayload      = packetLimit - publishOverhead
)

// A payloadDist is how big each publication's payload is: fixed bytes, or,
// where fixed is zero, drawn from mix.
type payloadDist struct{ fixed int }

// mix is -payload mix, the maintainer-agreed starting assumption for
// industrial and edge telemetry: each publication's size drawn by weight
// (out of 100) from a band, then uniformly within it, bounds included.
// 80% are small readings of 50 to 500 B, 15% batches of 1 to 16 KiB, 5%
// blobs of 64 KiB to 1 MiB, the top bounded by maxPayload so the
// publication fits the broker's Maximum Packet Size. Every size is above
// stampSize, so every payload carries its stamp whole.
var mix = []struct{ weight, lo, hi int }{
	{80, 50, 500},
	{15, 1 << 10, 16 << 10},
	{5, 64 << 10, maxPayload},
}

// parsePayload reads -payload: mix, or a size in bytes, KiB or MiB, from
// stampSize, the stamp every payload carries, to maxPayload.
func parsePayload(v string) (payloadDist, error) {
	if v == "mix" {
		return payloadDist{}, nil
	}
	n, err := config.ParseSize(v)
	if err != nil || n > maxPayload || n < stampSize {
		return payloadDist{}, fmt.Errorf("-payload %q: mix, or a size of %d to %d bytes (B, KiB or MiB)", v, stampSize, maxPayload)
	}
	return payloadDist{fixed: int(n)}, nil
}

// String names d in the results.
func (d payloadDist) String() string {
	if d.fixed == 0 {
		return "mix (80% 50-500 B, 15% 1-16 KiB, 5% 64 KiB-1 MiB)"
	}
	return strconv.Itoa(d.fixed) + " B"
}

// largest is the largest payload d draws.
func (d payloadDist) largest() int {
	if d.fixed > 0 {
		return d.fixed
	}
	return mix[len(mix)-1].hi
}

// source is publisher i's own source of sizes, from the recipe's seed: the
// same sequence of sizes on every run, whatever the other publishers do.
func (d payloadDist) source(i int) *rand.Rand {
	return rand.New(rand.NewPCG(seed, uint64(i)+1)) // stream 0 is the QoS draw's
}

// draw is the next publication's payload size, from src.
func (d payloadDist) draw(src *rand.Rand) int {
	if d.fixed > 0 {
		return d.fixed
	}
	w := src.IntN(100)
	b := mix[len(mix)-1]
	for _, band := range mix[:len(mix)-1] {
		if w < band.weight {
			b = band
			break
		}
		w -= band.weight
	}
	return b.lo + src.IntN(b.hi-b.lo+1)
}
