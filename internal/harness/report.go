package harness

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

// TestSupportDirs lists module-relative test-support directories, using "/"
// separators and ending in "/". The fuzz gap report and R1/R3 rule scans
// read this list.
var TestSupportDirs = []string{"internal/harness/", "internal/testkit/"}

// InTestSupport reports whether a module-relative, "/"-separated path is a
// listed test-support directory or lies under one. Adding "/" lets the
// directory itself match the list's trailing-slash convention.
func InTestSupport(path string) bool {
	return slices.ContainsFunc(TestSupportDirs, func(dir string) bool { return strings.HasPrefix(path+"/", dir) })
}

// Say writes one line of a runner's report. A report the terminal cannot
// take has nowhere else to go, so a failed write is dropped.
func Say(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}
