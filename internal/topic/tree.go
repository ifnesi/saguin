package topic

import "strings"

// Tree is the MQTT filter trie shared by routing and compiled access grants.
// Zero is a missing value. Build it before publishing it to readers; thereafter
// Match is read-only. More exact filters win, as channel routing requires.
type Tree struct {
	written map[string]*Tree
	plus    *Tree
	end     int
	hash    int
}

// Add inserts a checked, expanded filter with a nonzero value.
func (n *Tree) Add(filter string, value int) {
	for level := range strings.SplitSeq(filter, "/") {
		switch level {
		case "#":
			n.hash = value
		case "+":
			if n.plus == nil {
				n.plus = new(Tree)
			}
			n = n.plus
		default:
			if n.written == nil {
				n.written = make(map[string]*Tree)
			}
			if n.written[level] == nil {
				n.written[level] = new(Tree)
			}
			n = n.written[level]
		}
	}
	if !strings.HasSuffix(filter, "#") {
		n.end = value
	}
}

// Match resolves a nonempty name, excluding root wildcards for $ names.
func (n *Tree) Match(name string) int {
	if n == nil {
		return 0
	}
	if strings.HasPrefix(name, "$") {
		level, rest, more := strings.Cut(name, "/")
		return n.written[level].resolve(rest, more)
	}
	return n.resolve(name, true)
}

func (n *Tree) resolve(name string, more bool) int {
	if n == nil {
		return 0
	}
	if !more {
		if n.end != 0 {
			return n.end
		}
		return n.hash
	}
	level, rest, more := strings.Cut(name, "/")
	if found := n.written[level].resolve(rest, more); found != 0 {
		return found
	}
	if found := n.plus.resolve(rest, more); found != 0 {
		return found
	}
	return n.hash
}
