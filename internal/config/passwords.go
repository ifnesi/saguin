package config

import (
	"errors"
	"os"

	"github.com/ifnesi/saguin/internal/operations/routes"
	"github.com/ifnesi/saguin/internal/passwd"
)

// LoadPasswords reads every configured password file before startup
// writes any broker files. Listeners inheriting a file share one immutable
// copy. Parsing configuration alone does not read authentication files.
func (c *Config) LoadPasswords() error {
	m := &c.Broker.MQTT
	refs := listenerRefs(&m.Listen, "broker.mqtt", nil)
	files := make(map[string]*passwd.File, len(refs)+1)
	type passwordPath struct {
		name      string
		operators bool
	}
	paths := []passwordPath{{name: m.PasswordFile}}
	for _, ref := range refs {
		paths = append(paths, passwordPath{name: ref.listener.PasswordFile})
	}
	if o := c.Broker.Operations; o != nil {
		paths = append(paths, passwordPath{name: o.PasswordFile, operators: true})
		for _, ref := range listenerRefs(&o.Listen, "broker.operations", nil) {
			paths = append(paths, passwordPath{name: ref.listener.PasswordFile, operators: true})
		}
	}
	var findings []error
	for _, path := range paths {
		name := path.name
		if _, seen := files[name]; name == "" || seen {
			continue
		}
		var file *passwd.File
		data, err := os.ReadFile(name)
		if err == nil {
			if path.operators {
				file, err = passwd.ParseOperators(name, data, routes.Paths[:])
			} else {
				file, err = passwd.Parse(name, data)
			}
		}
		if err != nil {
			findings = append(findings, err)
		}
		files[name] = file
	}
	if err := errors.Join(findings...); err != nil {
		return err
	}
	c.Passwords = files
	return nil
}
