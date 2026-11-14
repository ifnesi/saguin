package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

// LoadBridgePasswords reads the peers' binary credentials before startup
// writes any broker files. Exactly one final line ending is omitted.
// These credentials are separate from the reloadable client password files.
func (c *Config) LoadBridgePasswords() error {
	var findings []error
	for i := range c.Bridges {
		b := &c.Bridges[i]
		if b.PasswordFile == "" {
			continue
		}
		password, err := os.ReadFile(b.PasswordFile)
		if err == nil {
			// A bare final CR is password data, not a line ending.
			if bytes.HasSuffix(password, []byte("\n")) {
				password = password[:len(password)-1]
				password = bytes.TrimSuffix(password, []byte("\r"))
			}
			if len(password) > maxMQTTTwoByteInteger {
				err = fmt.Errorf("password exceeds %d bytes", maxMQTTTwoByteInteger)
			}
		}
		if err != nil {
			findings = append(findings, fmt.Errorf("bridge %s: password file %s: %w", b.Name, b.PasswordFile, err))
		} else {
			b.Password = password
		}
	}
	return errors.Join(findings...)
}
