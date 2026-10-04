package main

// The saguin-ask skills and the shipped Q&A agent against the documents
// they answer from.
//
// **Both answer from saguin's documents, so both go stale when the
// documents move and nothing says so.** A month of document changes left
// two of the agent's questions expecting headings that had been respelled
// ("Sagüin's", "`synchronous=NORMAL`") - questions that failed whatever the
// agent answered - and the Claude skill teaching a QoS 2 switch that no
// longer exists. These tests hold what can be checked mechanically: every
// path the tools cite exists, every citation a question expects names a
// heading, file or invariant the corpus holds, and the two skills and the
// agent name the same documents. Each counts what it examined, so a parser
// that stopped finding anything fails rather than passing over nothing.

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const repoRoot = "../.."

var (
	askClaudeSkill = ".claude/skills/saguin-ask/SKILL.md"
	askCodexSkill  = ".agents/skills/saguin-ask/SKILL.md"
	askAgentDir    = "docs/saguin-agent"
)

// foldCitation is a citation as the agent's question runner compares it
// (docs/saguin-agent/app/questions.py, folded): case, backticks and the
// accents on Latin letters dropped, so a heading respelled "Sagüin" still
// matches a question written "saguin".
func foldCitation(s string) string {
	const from, to = "àáâãäåçèéêëìíîïñòóôõöùúûüýÿ", "aaaaaaceeeeiiiinooooouuuuyy"
	fromRunes, toRunes := []rune(from), []rune(to)
	fold := map[rune]rune{}
	for i, r := range fromRunes {
		fold[r] = toRunes[i]
	}
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if r == '`' {
			continue
		}
		if f, ok := fold[r]; ok {
			r = f
		}
		b.WriteRune(r)
	}
	return b.String()
}

// askCorpus is the agent's corpus list, read from the code that indexes it
// rather than restated here.
func askCorpus(t *testing.T) []string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot, askAgentDir, "app", "corpus.py"))
	if err != nil {
		t.Fatal(err)
	}
	_, list, ok := strings.Cut(string(body), "\nCORPUS = [")
	if ok {
		list, _, ok = strings.Cut(list, "\n]")
	}
	if !ok {
		t.Fatal("corpus.py has no `CORPUS = [ ... ]` list; this test reads the corpus from it")
	}
	var out []string
	for _, m := range regexp.MustCompile(`"([^"]+)"`).FindAllStringSubmatch(list, -1) {
		out = append(out, m[1])
	}
	return out
}

// resolves reports whether a path a tool cites is there, relative to one
// of bases: a file or directory, a glob that matches something, or a
// prefix one file begins with (`docs/rfcs/0001` in a table of them).
func resolves(path string, bases ...string) bool {
	for _, base := range bases {
		p := filepath.Join(repoRoot, base, strings.TrimSuffix(path, "/"))
		if strings.ContainsAny(path, "*?[") {
			if m, _ := filepath.Glob(p); len(m) > 0 {
				return true
			}
			continue
		}
		if _, err := os.Stat(p); err == nil {
			return true
		}
		if m, _ := filepath.Glob(p + "*"); len(m) > 0 {
			return true
		}
	}
	return false
}

var (
	inlineCode = regexp.MustCompile("`([^`\n]+)`")
	pathExt    = regexp.MustCompile(`\.(md|ya?ml|py|txt|go|json)$`)
)

// citedPaths is what in text reads as a path in the checkout: an inline
// code span, or a word of a fenced block, with a slash or a file's
// extension in it.
//
// **Left out by what they are**, not by who cites them: an absolute path
// names the reader's machine rather than the checkout (a scratch file, the
// gitignored `/bin/`); `bin/` is build output the reader makes; and a word
// carrying a topic wildcard, a placeholder, a URL, a variable or a flag is
// a topic, a template or a command, not a file.
func citedPaths(text string) []string {
	var words []string
	fenced := false
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			continue
		}
		if fenced {
			for _, w := range strings.Fields(line) {
				words = append(words, strings.Trim(w, `'"(),;`))
			}
			continue
		}
		for _, m := range inlineCode.FindAllStringSubmatch(line, -1) {
			words = append(words, m[1])
		}
	}
	var out []string
	for _, w := range words {
		switch {
		case w == "" || strings.ContainsAny(w, " \t$<>…:={}+#%|@\"'"):
		case strings.HasPrefix(w, "/") || strings.HasPrefix(w, "-"):
		case strings.HasPrefix(w, "./bin/") || strings.HasPrefix(w, "bin/"):
		case strings.Contains(w, "/") || pathExt.MatchString(w) || w == "LICENSE":
			out = append(out, w)
		}
	}
	return out
}

func TestEveryPathTheAskToolsCiteExistsBesideTheDocuments(t *testing.T) {
	sources := []struct {
		file  string
		bases []string
	}{
		// A skill's corpus lists the files inside examples/ by their own
		// names, so a bare name is looked for there as well.
		{askClaudeSkill, []string{"", "examples"}},
		{askCodexSkill, []string{"", "examples"}},
		{askAgentDir + "/README.md", []string{askAgentDir, ""}},
	}
	examined := 0
	var names []string
	for _, s := range sources {
		body, err := os.ReadFile(filepath.Join(repoRoot, s.file))
		if err != nil {
			t.Fatal(err)
		}
		paths := citedPaths(string(body))
		if len(paths) < 3 {
			t.Errorf("%s: found %d cited paths, and it cites more: the extraction here has gone "+
				"stale, so this check is reporting on work it did not do", s.file, len(paths))
		}
		for _, p := range paths {
			// **A slash does not make a path.** A word whose first segment is
			// no directory in the checkout, and that has no file's extension,
			// is a name of something elsewhere - the embedding model the
			// agent downloads - and is listed below rather than dropped.
			first, _, _ := strings.Cut(p, "/")
			if strings.Contains(p, "/") && !pathExt.MatchString(p) && !resolves(first+"/", s.bases...) {
				names = append(names, p)
				continue
			}
			examined++
			if !resolves(p, s.bases...) {
				t.Errorf("%s cites %q, which is not in the checkout", s.file, p)
			}
		}
	}
	for _, p := range askCorpus(t) {
		examined++
		if !resolves(p, "") {
			t.Errorf("the agent's corpus (corpus.py) lists %q, which matches nothing", p)
		}
	}
	t.Logf("%d cited paths examined across the two skills, the agent's README and its corpus; "+
		"not paths in the checkout: %v", examined, names)
}

func TestEveryCitationTheAskToolsMakeIsInTheDocuments(t *testing.T) {
	// The corpus's headings, files and numbered invariants, folded.
	var files []string
	for _, pat := range askCorpus(t) {
		m, _ := filepath.Glob(filepath.Join(repoRoot, pat))
		files = append(files, m...)
	}
	var headings []string
	invariants := map[string]bool{}
	numbered := regexp.MustCompile(`^\*\*(\d+)\.\s`)
	for _, f := range files {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(repoRoot, f)
		fenced := false
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fenced = !fenced
				continue
			}
			if !fenced && strings.HasSuffix(f, ".md") && strings.HasPrefix(line, "#") {
				headings = append(headings, foldCitation(strings.TrimLeft(line, "# ")))
			}
			if m := numbered.FindStringSubmatch(line); m != nil && rel == "docs/invariants.md" {
				invariants["invariant "+m[1]] = true
			}
		}
		headings = append(headings, foldCitation(rel))
	}
	if len(headings) < 200 || len(invariants) < 10 {
		t.Fatalf("read %d headings and %d invariants from %d corpus files: too few to be the "+
			"documents, so this check is reporting on work it did not do",
			len(headings), len(invariants), len(files))
	}

	body, err := os.ReadFile(filepath.Join(repoRoot, askAgentDir, "questions.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var questions []struct {
		Question string    `yaml:"question"`
		Expect   yaml.Node `yaml:"expect_citation"`
		Refusal  bool      `yaml:"expect_refusal"`
	}
	if err := yaml.Unmarshal(body, &questions); err != nil {
		t.Fatalf("questions.yaml: %v", err)
	}
	checked := 0
	for _, q := range questions {
		var wants []string
		switch q.Expect.Kind {
		case yaml.ScalarNode:
			wants = []string{q.Expect.Value}
		case yaml.SequenceNode:
			for _, n := range q.Expect.Content {
				wants = append(wants, n.Value)
			}
		}
		if len(wants) == 0 && !q.Refusal {
			t.Errorf("%q expects neither a citation nor a refusal, so nothing can judge it", q.Question)
		}
		for _, want := range wants {
			checked++
			// "queue › Restart" is a path of headings: every part must name
			// one, as the runner's substring match of the whole needs.
			for _, part := range strings.Split(want, " › ") {
				part = foldCitation(strings.TrimSpace(part))
				found := invariants[part]
				for _, h := range headings {
					if found {
						break
					}
					found = strings.Contains(h, part)
				}
				if !found {
					t.Errorf("%q expects %q, and no heading, file or invariant in the corpus "+
						"holds %q: the question fails whatever the agent answers",
						q.Question, want, part)
				}
			}
		}
	}
	if len(questions) < 10 || checked < 20 {
		t.Fatalf("read %d questions and %d expected citations: too few to be the question set",
			len(questions), checked)
	}
	t.Logf("%d expected citations of %d questions checked against %d headings and %d invariants",
		checked, len(questions), len(headings), len(invariants))
}

// askCorpusNamed is the documents a tool names, as the corpus's top-level
// entries: every RFC is `docs/rfcs` and every example `examples`, so a table
// listing the RFCs one by one and a glob listing them together agree. A
// word that is not a path in the checkout - a file inside examples/ named
// on its own - is a detail of an entry rather than an entry.
func askCorpusNamed(tokens []string) []string {
	set := map[string]bool{}
	for _, tok := range tokens {
		if !resolves(tok, "") {
			continue
		}
		switch tok = strings.TrimSuffix(tok, "/"); {
		case strings.HasPrefix(tok, "docs/rfcs"):
			tok = "docs/rfcs"
		case strings.HasPrefix(tok, "examples"):
			tok = "examples"
		}
		set[tok] = true
	}
	var out []string
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestBothAskSkillsAndTheAgentNameTheSameDocuments(t *testing.T) {
	section := func(file, from, to string) []string {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(repoRoot, file))
		if err != nil {
			t.Fatal(err)
		}
		_, rest, ok := strings.Cut(string(body), from)
		if ok {
			rest, _, ok = strings.Cut(rest, to)
		}
		if !ok {
			t.Fatalf("%s has no corpus section between %q and %q", file, from, to)
		}
		var tokens []string
		for _, m := range inlineCode.FindAllStringSubmatch(rest, -1) {
			tokens = append(tokens, m[1])
		}
		return tokens
	}
	claude := askCorpusNamed(section(askClaudeSkill, "## The corpus", "**Not the corpus.**"))
	codex := askCorpusNamed(section(askCodexSkill, "## Documentation corpus", "Do not treat"))
	agent := askCorpusNamed(askCorpus(t))
	if len(agent) < 6 {
		t.Fatalf("the agent's corpus reduced to %v: too few to be the documents", agent)
	}
	for name, got := range map[string][]string{askClaudeSkill: claude, askCodexSkill: codex} {
		if strings.Join(got, " ") != strings.Join(agent, " ") {
			t.Errorf("%s names %v as the documents it answers from, and the agent indexes %v",
				name, got, agent)
		}
	}
	t.Logf("each names %d documents: %v", len(agent), agent)
}
