package main

// A heading that appears twice in one document, across every document this
// repository ships.
//
// **This exists because it happened.** A large section was written into
// RFC 0003 with a shell heredoc that failed on a quoting error *after*
// partially executing, and the file ended up carrying two copies of a
// 163-line specification section. Nothing noticed: the document rendered,
// every existing check passed, and it surfaced only because an unrelated
// test counted four worked examples and found eight.
//
// **The class rather than the instance.** Nothing about that is specific to
// RFC 0003 or to that section - any document edited that way can take a
// duplicate, and a reader meeting the second copy has no way to know which
// one is current. So every document is swept, and the sweep counts what it
// examined: one that stopped finding headings would otherwise report
// success over work it did not do.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoDocumentRepeatsAHeading(t *testing.T) {
	var docs []string
	for _, pat := range []string{
		"../../README.md", "../../CONTRIBUTING.md", "../../SECURITY.md",
		"../../CHANGELOG.md", "../../docs/invariants.md", "../../docs/rfcs/*.md",
	} {
		found, err := filepath.Glob(pat)
		if err != nil {
			t.Fatalf("glob %s: %v", pat, err)
		}
		docs = append(docs, found...)
	}
	if len(docs) < 8 {
		t.Fatalf("found %d documents to sweep and this repository ships more: the "+
			"paths here have gone stale, so this check is reporting on work it did "+
			"not do", len(docs))
	}

	headings := 0
	for _, path := range docs {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		seen := map[string][]int{}
		fenced := false
		for n, line := range strings.Split(string(body), "\n") {
			// A `#` inside a fenced block is a shell comment or a topic
			// filter, not a heading. Counting those would make this fail on
			// every document that shows two `# comment` lines.
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				continue
			}
			if fenced || !strings.HasPrefix(line, "#") {
				continue
			}
			text := strings.TrimSpace(strings.TrimLeft(line, "#"))
			if text == "" {
				continue
			}
			headings++
			seen[line] = append(seen[line], n+1)
		}
		for heading, lines := range seen {
			if len(lines) > 1 {
				t.Errorf("%s carries %q %d times, at lines %v.\n"+
					"  A reader meeting the second copy cannot tell which is "+
					"current, and a section written twice is how a partially "+
					"applied edit survives review",
					filepath.Base(path), heading, len(lines), lines)
			}
		}
	}
	if headings < 100 {
		t.Fatalf("swept %d headings across %d documents, and these documents carry "+
			"far more: this check has stopped matching the shape of a heading",
			headings, len(docs))
	}
	t.Logf("%d headings across %d documents, none repeated within one", headings, len(docs))
}
