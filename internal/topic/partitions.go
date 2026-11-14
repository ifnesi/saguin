package topic

import (
	"strconv"
	"strings"
)

// Partitions is a topic partition selection, immutable after declaration.
// Repeated indices form a set; clients choose the set themselves.
type Partitions struct {
	Count   uint32
	Indices map[uint32]struct{}
}

// Add accepts exactly one topic_hash(count,index) declaration. Only ASCII
// spaces and tabs around the integers and comma are permitted.
func (p *Partitions) Add(declaration string) bool {
	body, ok := strings.CutPrefix(declaration, "topic_hash(")
	if !ok || !strings.HasSuffix(body, ")") {
		return false
	}
	count, index, ok := strings.Cut(body[:len(body)-1], ",")
	if !ok {
		return false
	}
	n, valid := partitionInteger(count)
	if !valid || n == 0 || (p.Count != 0 && p.Count != n) {
		return false
	}
	i, valid := partitionInteger(index)
	if !valid || i >= n {
		return false
	}
	if p.Indices == nil {
		p.Indices = make(map[uint32]struct{})
	}
	p.Count = n
	p.Indices[i] = struct{}{}
	return true
}

func partitionInteger(s string) (uint32, bool) {
	s = strings.Trim(s, " \t")
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(s, 10, 32)
	return uint32(n), err == nil
}

// Hash applies FNV-1a-64 and the design's fixed avalanche to UTF-8 bytes.
// Unsigned arithmetic supplies the specified reduction modulo 2^64.
func Hash(name []byte) uint64 {
	x := uint64(14695981039346656037)
	for _, b := range name {
		x = (x ^ uint64(b)) * 1099511628211
	}
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}
