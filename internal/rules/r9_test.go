package rules

import (
	"debug/buildinfo"
	"debug/dwarf"
	"debug/elf"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"
)

// maxBinaryBytes is R9's limit: CI fails on a binary of 20 MB or more. The
// design writes binary units as KiB, MiB and GiB, so MB is 1,000,000 bytes.
const maxBinaryBytes = 20_000_000

// dwarfLangGo is DW_LANG_Go. A compile unit in any other language is C code
// built with debug information.
const dwarfLangGo = 0x16

// The libraries the design names: SQLite's (Store: "SQLite through
// github.com/mattn/go-sqlite3, linked statically") and WebSocket's (R1:
// "The WebSocket library, github.com/gorilla/websocket").
const (
	sqliteModule    = "github.com/mattn/go-sqlite3"
	websocketModule = "github.com/gorilla/websocket"
)

// approved is R9's list of dependencies. Only the maintainer adds to it, in
// the item that first needs the library, and the test code is Claude's, so
// the coder cannot approve its own dependency.
var approved = map[string]bool{
	sqliteModule: true,
	// The WebSocket library, approved by the maintainer for item 0013d
	// (its comment of 261102 231543): github.com/gorilla/websocket v1.5.3,
	// BSD-2-Clause, with no dependencies of its own.
	websocketModule: true,
	// The configuration file's YAML reader, approved by the maintainer for
	// item 0004a (its comment of 261011 124002): the YAML organisation's
	// continuation of gopkg.in/yaml.v3, with no dependencies of its own.
	"go.yaml.in/yaml/v3": true,
	// The terminal library, approved by the maintainer for item 0018a (its
	// comment of 261028 083930): the Go team's golang.org/x/term, for
	// `saguin --passwd add` reading a password without echo;
	// golang.org/x/sys because x/term needs it.
	"golang.org/x/term": true,
	"golang.org/x/sys":  true,
	// The compression library, approved by the maintainer for item 0031a
	// (its comment of 261031 070347): github.com/klauspost/compress, its s2
	// package only, for the S2 block format design.md, Store, Compression
	// names; it has no dependencies of its own.
	"github.com/klauspost/compress": true,
}

// TestOnlyApprovedDependencies fails on any module in the build list that is
// not approved. The module itself is the one entry that is not a dependency.
func TestOnlyApprovedDependencies(t *testing.T) {
	root := moduleRoot(t)
	self := strings.TrimSpace(run(t, root, time.Minute, "go", "list", "-m"))
	list := run(t, root, time.Minute, "go", "list", "-m", "-f", "{{.Path}}", "all")
	for path := range strings.FieldsSeq(list) {
		if path != self && !approved[path] {
			t.Errorf("unapproved dependency %s (R9): the maintainer approves each one", path)
		}
	}
}

// TestShippedBinary builds the binary as the contract with the Makefile says
// (`make build` writes bin/saguin) and checks it against R9: under 20 MB,
// built with Go's default compiler and linker settings plus the flags a
// static glibc link needs, with Go's own DNS resolver, SQLite's C code
// without debug information, and nothing else stripped. The libraries the
// design names, go-sqlite3 and gorilla/websocket, must be linked in, or the
// size means nothing.
func TestShippedBinary(t *testing.T) {
	root := moduleRoot(t)
	run(t, root, 15*time.Minute, "make", "build")
	path := filepath.Join(root, "bin", "saguin")

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("make build left no binary: %v", err)
	}
	f, err := elf.Open(path)
	if err != nil {
		t.Fatalf("not an ELF binary: %v", err)
	}
	defer func() { _ = f.Close() }() // read only; a failed close loses nothing
	syms, symErr := f.Symbols()
	have := make(map[string]bool, len(syms))
	for _, s := range syms {
		have[s.Name] = true
	}
	// needSymbols stops a subtest that reads the symbol table when there is
	// none: a check that a symbol is absent would pass on such a binary.
	needSymbols := func(t *testing.T) {
		t.Helper()
		if symErr != nil || len(syms) == 0 {
			t.Fatalf("no symbol table to read: %v", symErr)
		}
	}

	t.Run("under 20 MB", func(t *testing.T) {
		if info.Size() >= maxBinaryBytes {
			t.Errorf("binary is %d bytes, R9 needs fewer than %d", info.Size(), maxBinaryBytes)
		}
	})

	t.Run("static", func(t *testing.T) {
		for _, p := range f.Progs {
			if p.Type == elf.PT_INTERP {
				t.Error("the binary names a dynamic loader (PT_INTERP): the link is not static")
			}
		}
		libs, err := f.ImportedLibraries()
		if err != nil {
			t.Fatalf("reading needed libraries: %v", err)
		}
		if len(libs) > 0 {
			t.Errorf("the binary needs shared libraries %v", libs)
		}
	})

	// glibc and musl each define a start-up function the other lacks. glibc's
	// static start-up sets up thread-local storage in __libc_setup_tls, which
	// a dynamic glibc binary leaves to the dynamic loader; musl's start-up is
	// __init_libc.
	t.Run("static glibc", func(t *testing.T) {
		needSymbols(t)
		if !have["__libc_setup_tls"] {
			t.Error("no glibc static start-up (__libc_setup_tls): the binary is not linked with static glibc")
		}
		if have["__init_libc"] {
			t.Error("musl start-up found (__init_libc): the binary is not linked with glibc")
		}
	})

	// Go's net package documents its cgo resolver as calling the C library's
	// getaddrinfo and getnameinfo, and as chosen over Go's own resolver by
	// what the machine says when the binary runs: GODEBUG, other environment
	// variables, /etc/resolv.conf and /etc/nsswitch.conf. A static binary
	// holds every C function it calls, so one that holds neither cannot look
	// a name up through the C library, whatever those say: Go's own resolver
	// is the only one it has. Go documents the netgo build tag as the way a
	// cgo build leaves the cgo resolver out; the test reads the binary, not
	// its flags.
	t.Run("Go's own DNS resolver", func(t *testing.T) {
		needSymbols(t)
		for _, name := range []string{"getaddrinfo", "getnameinfo"} {
			if have[name] {
				t.Errorf("the C library's resolver is linked in (%s): names are not resolved by Go's own resolver alone", name)
			}
		}
	})

	// A binary linked with -s or -w has no symbol table or no Go debug data.
	t.Run("nothing else stripped", func(t *testing.T) {
		if symErr != nil || len(syms) == 0 {
			t.Errorf("no symbol table (stripped with -s?): %v", symErr)
		}
		if f.Section(".debug_info") == nil && f.Section(".zdebug_info") == nil {
			t.Error("no debug information (stripped with -w?)")
		}
	})

	t.Run("links the libraries the design names", func(t *testing.T) {
		bi, err := buildinfo.ReadFile(path)
		if err != nil {
			t.Fatalf("reading build information: %v", err)
		}
		for _, module := range []string{sqliteModule, websocketModule} {
			if !slices.ContainsFunc(bi.Deps, func(dep *debug.Module) bool { return dep.Path == module }) {
				t.Errorf("%s is not linked into the binary", module)
			}
		}
	})

	// Go's own compile units are in Go. One in C, named for SQLite, is its
	// amalgamation built with -g.
	t.Run("SQLite's C code without debug information", func(t *testing.T) {
		d, err := f.DWARF()
		if err != nil {
			t.Fatalf("reading debug information: %v", err)
		}
		r := d.Reader()
		for {
			e, err := r.Next()
			if err != nil {
				t.Fatalf("reading debug information: %v", err)
			}
			if e == nil {
				return
			}
			if e.Tag != dwarf.TagCompileUnit {
				continue
			}
			name, _ := e.Val(dwarf.AttrName).(string)
			lang, _ := e.Val(dwarf.AttrLanguage).(int64)
			if lang != dwarfLangGo && strings.Contains(name, "sqlite3") {
				t.Errorf("SQLite's C code carries debug information (%s)", name)
			}
			r.SkipChildren()
		}
	})

	t.Run("runs", func(t *testing.T) {
		if out := run(t, root, 10*time.Second, path, "--version"); strings.TrimSpace(out) == "" {
			t.Error("--version printed nothing")
		}
	})
}
