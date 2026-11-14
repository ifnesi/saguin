package passwd

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Load reads and parses one client password file.
func Load(name string) (*File, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return Parse(name, data)
}

// Set replaces a user's line in place or appends a new one. It does not
// write to disk; Save installs the finished file in one rename.
func (f *File) Set(user, password string) error {
	if password == "" {
		return errors.New("an empty password is refused")
	}
	if err := checkUser(user); err != nil {
		return err
	}
	salt := make([]byte, saltBytes)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("generate password salt: %w", err)
	}
	key, err := pbkdf2.Key(sha512.New, password, salt, defaultIterations, sha512.Size)
	if err != nil {
		return err
	}
	line := user + ":$7$" + strconv.Itoa(defaultIterations) + "$" + base64.StdEncoding.EncodeToString(salt) + "$" + base64.StdEncoding.EncodeToString(key)
	if e, exists := f.entries[user]; exists && e.scope != nil {
		line += ":" + strings.Join(e.scope, ",")
	}
	return f.replace(user, line)
}

// replace shares line splicing between password and scope changes.
func (f *File) replace(user, line string) error {
	text := f.text
	if e, exists := f.entries[user]; exists {
		if strings.HasSuffix(text[e.start:e.end], "\n") {
			line += "\n"
		}
		text = text[:e.start] + line + text[e.end:]
	} else {
		if text != "" && !strings.HasSuffix(text, "\n") {
			text += "\n"
		}
		text += line + "\n"
	}
	updated, err := parse(f.name, []byte(text), f.routes)
	if err != nil {
		return err
	}
	*f = *updated
	return nil
}

// Delete removes the complete line of a listed user and reports whether it
// existed. Every remaining source byte is preserved; Save writes the change.
func (f *File) Delete(user string) bool {
	e, exists := f.entries[user]
	if !exists {
		return false
	}
	updated, err := parse(f.name, []byte(f.text[:e.start]+f.text[e.end:]), f.routes)
	if err != nil {
		panic("passwd: deleting a line invalidated a parsed file: " + err.Error())
	}
	*f = *updated
	return true
}
