// SPDX-License-Identifier: MIT
// SPDX-FileContributor: Italo Nesi

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/metrics"
	"strings"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/ifnesi/saguin/internal/sourcetree"
)

// **The heap floor is live, holds no pointers and is never touched**
// (heapFloor): live, or the collector does not count it and it moves
// nothing; free of pointers, or the collector scans it at every cycle; and
// untouched, or it costs its whole size in resident memory at rest.
func TestTheHeapFloorIsLivePointerFreeAndUntouched(t *testing.T) {
	require.Equal(t, heapFloorSize, len(heapFloor), "the floor is not its stated size")

	// Pointer-free: a byte slice's elements hold none, so the runtime puts
	// it in a span the collector does not scan.
	elem := reflect.TypeOf(heapFloor).Elem()
	require.Equal(t, reflect.Uint8, elem.Kind(), "the floor's elements are %s, which the collector may scan", elem)

	// Live: counted in what the collector found reachable.
	runtime.GC()
	ss := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(ss)
	live := ss[0].Value.Uint64()
	require.GreaterOrEqual(t, live, uint64(heapFloorSize), "the live heap (%d bytes) does not hold the floor", live)

	// Untouched: of the floor's pages, the kernel backs almost none.
	page := os.Getpagesize()
	start := uintptr(unsafe.Pointer(unsafe.SliceData(heapFloor)))
	first := (start + uintptr(page) - 1) &^ uintptr(page-1)
	n := (start + uintptr(len(heapFloor)) - first) / uintptr(page)
	vec := make([]byte, n)
	if _, _, errno := syscall.Syscall(syscall.SYS_MINCORE, first, n*uintptr(page), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		t.Fatalf("mincore: %v", errno)
	}
	resident := 0
	for _, v := range vec {
		resident += int(v & 1)
	}
	t.Logf("%d of the floor's %d pages resident; live heap %d bytes", resident, n, live)

	// Never a huge page: otherwise the kernel backs the floor's share of a
	// 2 MB region a neighbour touches, and residency stops meaning a write.
	// **Asked of /proc/self/smaps, which is Linux's**: the "nh" flag is what
	// withoutHugePages sets there, and transparent huge pages are a Linux
	// mechanism. Elsewhere there is no flag to read and the residency count
	// below is the whole check.
	if runtime.GOOS == "linux" {
		smaps, err := os.ReadFile("/proc/self/smaps")
		require.NoError(t, err)
		end := start + uintptr(len(heapFloor))
		covered, vmas, overlap := first, 0, false
		for _, line := range strings.Split(string(smaps), "\n") {
			var lo, hi uintptr
			if _, err := fmt.Sscanf(line, "%x-%x ", &lo, &hi); err == nil {
				vmas++
				overlap = lo < end && hi > first
				if overlap {
					require.LessOrEqual(t, lo, covered, "a gap in the floor's mappings at %#x", covered)
					covered = hi
				}
				continue
			}
			if overlap && strings.HasPrefix(line, "VmFlags:") {
				t.Logf("a mapping of the floor: %s", line)
				require.Contains(t, strings.Fields(line), "nh", "the floor's mapping may be backed by huge pages")
			}
		}
		require.Greater(t, vmas, 0, "no mapping parsed from /proc/self/smaps")
		require.GreaterOrEqual(t, covered, end, "the floor's mappings were not all examined")
	} else {
		t.Logf("the huge-page check is skipped on %s: /proc/self/smaps and its nh flag are Linux's", runtime.GOOS)
	}
	require.Less(t, resident, int(n)/10, "%d of the floor's %d pages are resident: something writes it", resident, n)
}

// **A floor that starts inside a huge page keeps none of it**
// (withoutHugePages). Reproduces what THP "always" did to the floor on
// GitHub's runner, whatever this host's mode: heap made before the floor
// touches the 2 MB region the floor starts in, the kernel backs that region
// with one huge page, and the floor's share of it is resident before the
// floor exists. MADV_COLLAPSE backs a region as the fault or khugepaged
// does. One allocation stands for the heap around the floor, so the floor
// sits where CI's did: 512 KB into a region a neighbour has touched.
func TestAFloorInsideAHugePageKeepsNoneOfIt(t *testing.T) {
	mode, err := os.ReadFile("/sys/kernel/mm/transparent_hugepage/enabled")
	if err != nil || strings.Contains(string(mode), "[never]") {
		t.Skipf("this host has no transparent huge pages (%q, %v)", mode, err)
	}
	const huge = 2 << 20
	heap := make([]byte, heapFloorSize+2*huge)
	base := uintptr(unsafe.Pointer(unsafe.SliceData(heap)))
	head := (base + huge - 1) &^ (huge - 1)
	at := int(head - base + huge/4)
	floor := heap[at : at+heapFloorSize]
	tail := (head + huge/4 + heapFloorSize) &^ (huge - 1)
	madvise := func(region uintptr, advice int) syscall.Errno {
		_, _, errno := syscall.Syscall(syscall.SYS_MADVISE, region, huge, uintptr(advice))
		return errno
	}
	const hugepage, collapse = 14, 25 // MADV_HUGEPAGE, MADV_COLLAPSE: Linux's

	// THP "always" lets the kernel back any region nothing has advised
	// against. Reused heap may lie under an earlier floor's advice, so the
	// region the floor starts in is made eligible, as it was on the runner.
	require.Zero(t, madvise(head, hugepage), "MADV_HUGEPAGE of the region the floor starts in")
	heap[at-1] = 1 // the neighbour before the floor
	require.Zero(t, madvise(head, collapse), "MADV_COLLAPSE of the region the floor starts in")
	share := floor[:huge-huge/4]
	require.Equal(t, len(share)/os.Getpagesize(), residentPages(t, share), "the huge page does not cover the floor's share of its first region, so the reproduction did not run")
	before := residentPages(t, floor)

	floor = withoutHugePages(floor)
	heap[at+heapFloorSize] = 1 // the neighbour after the floor, made later
	for _, region := range []uintptr{head, tail} {
		t.Logf("MADV_COLLAPSE %#x after the floor is made: %v", region, madvise(region, collapse))
	}
	after := residentPages(t, floor)
	t.Logf("of the floor's %d pages, %d resident before withoutHugePages, %d after it and khugepaged", len(floor)/os.Getpagesize(), before, after)
	require.Zero(t, after, "%d of the floor's pages stay resident: something left them backed", after)
	runtime.KeepAlive(heap)
}

// residentPages is how many of b's pages the kernel backs.
func residentPages(t *testing.T, b []byte) int {
	page := os.Getpagesize()
	start := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	require.Zero(t, start%uintptr(page), "b does not start on a page")
	vec := make([]byte, len(b)/page)
	if _, _, errno := syscall.Syscall(syscall.SYS_MINCORE, start, uintptr(len(vec)*page), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		t.Fatalf("mincore: %v", errno)
	}
	n := 0
	for _, v := range vec {
		n += int(v & 1)
	}
	return n
}

// **Nothing names the heap floor but its declaration**, so nothing reads or
// writes it: a write makes its pages resident, and a read of a page never
// written maps the kernel's zero page for nothing. Walks the syntax tree of
// every Go file in the repository, tests included, other than this one.
func TestNothingNamesTheHeapFloor(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	var scanned, declared int
	var named []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || filepath.Base(path) == "heapfloor_test.go" {
			return nil
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err, path)
		ast.Inspect(f, func(n ast.Node) bool {
			if vs, ok := n.(*ast.ValueSpec); ok {
				for _, name := range vs.Names {
					if name.Name == "heapFloor" {
						declared++
					}
				}
				for _, v := range vs.Values {
					ast.Inspect(v, func(n ast.Node) bool { return visitName(fset, root, path, n, &named) })
				}
				return false
			}
			return visitName(fset, root, path, n, &named)
		})
		return nil
	})
	require.NoError(t, err)
	t.Logf("%d Go files examined, heapFloor declared %d times", scanned, declared)
	require.Greater(t, scanned, 100, "the walk is not reaching the repository")
	require.Equal(t, 1, declared, "the walk did not find heapFloor's declaration, so it has stopped matching it")
	require.Empty(t, named, "heapFloor is named outside its declaration")
}

// visitName records an identifier heapFloor at n.
func visitName(fset *token.FileSet, root, path string, n ast.Node, named *[]string) bool {
	if id, ok := n.(*ast.Ident); ok && id.Name == "heapFloor" {
		rel, _ := filepath.Rel(root, path)
		*named = append(*named, rel+":"+strings.TrimPrefix(fset.Position(id.Pos()).String(), path+":"))
	}
	return true
}
