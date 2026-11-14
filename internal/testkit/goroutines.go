package testkit

import (
	"runtime"
	"strings"
)

// Goid returns the calling goroutine's id, from the first line of its stack
// ("goroutine 7 [running]:").
func Goid() string {
	buf := make([]byte, 64)
	return strings.Fields(string(buf[:runtime.Stack(buf, false)]))[1]
}

// Goroutine is one live goroutine: its id, the function holding the go
// statement that started it, the id of the goroutine that ran that
// statement, and its whole stack.
type Goroutine struct {
	ID, Creator, Parent, Stack string
}

// Goroutines returns every live goroutine, by id. A goroutine's stack ends
// "created by <the function holding the go statement> in goroutine <its
// parent>"; the runtime's first goroutine has no such line.
func Goroutines() map[string]Goroutine {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	all := map[string]Goroutine{}
	for stack := range strings.SplitSeq(string(buf), "\n\n") {
		g := Goroutine{ID: strings.Fields(stack)[1], Stack: stack}
		for line := range strings.SplitSeq(stack, "\n") {
			if f := strings.Fields(line); len(f) >= 6 && f[0] == "created" && f[1] == "by" && f[3] == "in" {
				g.Creator, g.Parent = f[2], f[5]
			}
		}
		all[g.ID] = g
	}
	return all
}

// Descendants returns, by id, the live goroutines root admits, and those
// grow admits whose parent is already among them, again until none is
// added: a family of goroutines started from one another.
func Descendants(root, grow func(Goroutine) bool) map[string]Goroutine {
	return descendants(Goroutines(), root, grow)
}

// descendants is Descendants among all, the live goroutines by id.
func descendants(all map[string]Goroutine, root, grow func(Goroutine) bool) map[string]Goroutine {
	members := map[string]Goroutine{}
	for grew := true; grew; {
		grew = false
		for id, g := range all {
			if _, in := members[id]; in {
				continue
			}
			if _, parentIn := members[g.Parent]; root(g) || (parentIn && grow(g)) {
				members[id] = g
				grew = true
			}
		}
	}
	return members
}

// ServedConnection is the function net/http starts each connection's
// goroutine in, on the goroutine serving a listener: design.md, R1, "A
// WebSocket connection accepted by a listener has as its reader the
// goroutine net/http starts for it",
// as is an operations connection's HTTP request goroutine.
const ServedConnection = "net/http.(*Server).Serve"

// Started returns the live goroutines started from goroutine parent, directly
// or through each other, by functions whose names begin with prefix (a
// module's path: its code's goroutines), and those net/http starts for the
// connections they serve (ServedConnection), counted by the function that
// started each.
func Started(parent, prefix string) map[string]int {
	ours := func(g Goroutine) bool { return strings.HasPrefix(g.Creator, prefix) }
	return byCreator(Descendants(func(g Goroutine) bool { return ours(g) && g.Parent == parent }, startedGrow(prefix)))
}

// Running is Started, also counting the goroutines started by functions
// whose names begin with prefix from a goroutine that has since ended, and
// those started from them: a bridge's reader and writer outlive the dialer
// that starts them (design.md, R1: "dialer | 1 per bridge connection
// attempt | the attempt"; "Dials, starts"), so their starter is no longer
// among the live goroutines. A goroutine of the caller's code whose starter
// has ended is counted alike, so a test using Running leaves none.
func Running(parent, prefix string) map[string]int {
	all := Goroutines()
	root := func(g Goroutine) bool {
		if !strings.HasPrefix(g.Creator, prefix) {
			return false
		}
		_, starterLive := all[g.Parent]
		return g.Parent == parent || !starterLive
	}
	return byCreator(descendants(all, root, startedGrow(prefix)))
}

// startedGrow admits the goroutines Started and Running count from a
// member: those functions whose names begin with prefix start, and those
// net/http starts for the connections they serve (ServedConnection).
func startedGrow(prefix string) func(Goroutine) bool {
	return func(g Goroutine) bool { return strings.HasPrefix(g.Creator, prefix) || g.Creator == ServedConnection }
}

// byCreator counts members by the function that started each.
func byCreator(members map[string]Goroutine) map[string]int {
	counts := map[string]int{}
	for _, g := range members {
		counts[g.Creator]++
	}
	return counts
}
