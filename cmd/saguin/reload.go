package main

import (
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/core"
)

// reloadStep runs on main, never on the core. The startup configuration fixes
// the file paths and listeners; only log admission and origins are read anew
// from the configuration file. Each result is handed to the core once.
func reloadStep(cfg *config.Config, path string, part int) core.Reload {
	switch part {
	case 0:
		snapshot, err := cfg.ReloadLogAndOrigins(path)
		return core.Reload{Part: "log_and_origins", Origins: snapshot, Err: err}
	case 1:
		snapshot, err := cfg.ReloadAccess()
		return core.Reload{Part: "access", Access: snapshot, Err: err}
	default:
		snapshot, err := cfg.ReloadTLS()
		return core.Reload{Part: "tls", TLS: snapshot, Err: err}
	}
}
