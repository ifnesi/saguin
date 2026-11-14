package harness

import "testing"

// TestInTestSupport: audit round 261015-03, observation 2, "the prefix rule
// over TestSupportDirs is written as a one-line slices.ContainsFunc twice"
// (the fuzz runner's gap report over a file path, the rules tests over a
// path with "/" added); InTestSupport holds it once, beside the list. A
// module-relative, "/"-separated path is test support when it is one of the
// harness's or testkit's directories, or lies under one. A directory that
// merely begins with the same letters is not: the trailing "/" each listed
// directory ends in is what keeps internal/harnessx out.
func TestInTestSupport(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		// Under each directory, at its top and deeper down.
		{"internal/harness/report.go", true},
		{"internal/harness/cmd/fuzz/main.go", true},
		{"internal/testkit/testkit.go", true},
		{"internal/testkit/sub/sub.go", true},
		// Each directory itself, named without its slash, as a package's
		// directory is.
		{"internal/harness", true},
		{"internal/testkit", true},
		// Siblings whose names begin with a listed directory's.
		{"internal/harnessx/x.go", false},
		{"internal/harnessx", false},
		{"internal/testkitx", false},
		{"internal/testkitx/x.go", false},
		// Broker code, the parent directory, and no path at all.
		{"internal/core/core.go", false},
		{"internal", false},
		{"", false},
	}
	for _, c := range cases {
		if got := InTestSupport(c.path); got != c.want {
			t.Errorf("InTestSupport(%q) = %v, want %v", c.path, got, c.want)
		}
	}
}
