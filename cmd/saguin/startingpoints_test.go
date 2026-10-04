package main

import (
	"path/filepath"
	"testing"

	"github.com/ifnesi/saguin/internal/config"
)

// The starting-point configurations in examples/starting-points are what an
// operator copies first. So every one of them is loaded as the broker loads
// a configuration - an unknown key is a typo and a refused value is a
// finding, config.Load says both - and a key the binary stops accepting
// fails here rather than in somebody's first start. The docker example's
// configuration is one too: it is what `docker compose up` mounts.
func TestTheStartingPointConfigurationsAreAcceptedByTheBinary(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(repoRoot, "examples", "starting-points", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 3 {
		t.Fatalf("%d starting-point files, want the three at least, so this check has stopped finding what it guards", len(files))
	}
	files = append(files, filepath.Join(repoRoot, "examples", "docker", "saguin.yaml"))
	for _, f := range files {
		if _, _, err := config.Load(f); err != nil {
			t.Errorf("%s is refused by the binary's loader:\n%v", f, err)
		}
	}
	t.Logf("%d starting-point configurations loaded", len(files))
}
