package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// pathRef names a path's supplied key and whether Sagüin writes it.
// Inherited files retain their original key, as listener settings do.
type pathRef struct {
	key, value string
	written    bool
}

// DatabaseFiles describes the database and every file belonging beside it.
// Data may be replaced or removed while Lock is held. The lock file itself
// is never removed.
type DatabaseFiles struct {
	Data     [4]string // database, WAL, shared memory and rollback journal
	Lock     string
	Snapshot string // replacement staged beside the disk database
}

// FilesForDatabase gives path validation and cleanup the same file set.
func FilesForDatabase(path string) DatabaseFiles {
	path = filepath.Clean(path)
	return DatabaseFiles{
		Data:     [4]string{path, path + "-wal", path + "-shm", path + "-journal"},
		Lock:     path + ".lock",
		Snapshot: path + ".new",
	}
}

// LockPath is the storage lock beside the configured disk database.
func (s *Storage) LockPath() string {
	return FilesForDatabase(s.FilePath).Lock
}

// MemoryPath names the working database in memory mode.
func (c *Config) MemoryPath() string {
	return filepath.Join(c.Broker.Storage.MemoryDir, c.Broker.ID+".db")
}

// ResolveStoragePaths fixes the bases of SQLite's file sets before startup
// compares or writes any of them. Missing leaves retain their name beneath
// their resolved parent; startup confirms the database after creating it.
func (c *Config) ResolveStoragePaths() error {
	s := &c.Broker.Storage
	for _, path := range []struct {
		key   string
		value *string
	}{
		{"broker.storage.file_path", &s.FilePath},
		{"broker.storage.memory_dir", &s.MemoryDir},
	} {
		if *path.value == "" {
			continue
		}
		resolved, err := absoluteFile(*path.value)
		if err != nil {
			return fmt.Errorf("%s (%s): %w", path.key, *path.value, err)
		}
		*path.value = resolved
	}
	return nil
}

// paths lists file uses in configuration order. Files only read may share a
// path; writable files and directories must be distinct from every use.
// Derived paths retain the key that supplies their base path.
func (c *Config) paths(mqtt, operations []listenerRef, seen map[string]bool) []pathRef {
	b := &c.Broker
	paths := []pathRef{
		{"broker.pid_file", b.PIDFile, true},
		{"broker.mqtt.password_file", b.MQTT.PasswordFile, false},
		{"broker.mqtt.acl_file", b.MQTT.ACLFile, false},
	}
	paths = appendListenerPaths(paths, mqtt, seen)
	if o := b.Operations; o != nil {
		paths = append(paths, pathRef{"broker.operations.password_file", o.PasswordFile, false})
		paths = appendListenerPaths(paths, operations, seen)
	}
	paths = appendDatabasePaths(paths, "broker.storage.file_path", b.Storage.FilePath)
	if b.Storage.Mode == Memory {
		paths = append(paths, pathRef{"broker.storage.file_path", FilesForDatabase(b.Storage.FilePath).Snapshot, true})
	}
	paths = append(paths, pathRef{"broker.storage.memory_dir", b.Storage.MemoryDir, true})
	if b.Storage.Mode == Memory {
		paths = appendDatabasePaths(paths, "broker.storage.memory_dir", c.MemoryPath())
	}
	for i := range c.Bridges {
		bridge := &c.Bridges[i]
		key := keyPath("bridges", bridge.Name)
		paths = append(paths, pathRef{key + ".password_file", bridge.PasswordFile, false})
		if t := bridge.TLS; t != nil {
			paths = append(paths,
				pathRef{key + ".tls.ca_file", t.CAFile, false},
				pathRef{key + ".tls.cert_file", t.CertFile, false},
				pathRef{key + ".tls.key_file", t.KeyFile, false},
			)
		}
	}
	return paths
}

// appendDatabasePaths applies the same collision rules to disk and RAM files.
func appendDatabasePaths(paths []pathRef, key, path string) []pathRef {
	files := FilesForDatabase(path)
	for _, path := range files.Data {
		paths = append(paths, pathRef{key, path, true})
	}
	return append(paths, pathRef{key, files.Lock, true})
}

func appendListenerPaths(paths []pathRef, refs []listenerRef, seen map[string]bool) []pathRef {
	for _, ref := range refs {
		l := ref.listener
		if l.Path != "" {
			key := ref.key + ".path"
			paths = append(paths, pathRef{key, l.Path, true}, pathRef{key, l.Path + ".lock", true})
		}
		paths = append(paths, pathRef{ref.source("password_file", seen), l.PasswordFile, false})
		if t := l.TLS; t != nil {
			paths = append(paths,
				pathRef{ref.key + ".tls.cert_file", t.CertFile, false},
				pathRef{ref.key + ".tls.key_file", t.KeyFile, false},
				pathRef{ref.key + ".tls.client_ca_file", t.ClientCAFile, false},
			)
		}
	}
	return paths
}

// validatePaths refuses the later use if either use writes the cleaned path,
// naming the first use. Lock files and SQLite sidecars participate in
// this same rule.
func validatePaths(paths []pathRef) error {
	uses := make(map[string]pathRef, len(paths))
	for _, ref := range paths {
		if ref.value == "" {
			continue
		}
		path := filepath.Clean(ref.value)
		if earlier, ok := uses[path]; ok {
			if mustDiffer(ref, earlier) {
				return refuse(ref.key, "must differ from "+earlier.key)
			}
			continue
		}
		uses[path] = ref
	}
	return nil
}

// compareFiles uses the same file-use list as spelling validation. Existing
// hard links compare by inode; symlinks and missing leaves compare by their
// resolved absolute names, including a relative Unix socket's working dir.
func compareFiles(paths []pathRef) error {
	type fileUse struct {
		ref  pathRef
		name string
		info os.FileInfo
	}
	uses := make([]fileUse, 0, len(paths))
	for _, ref := range paths {
		if ref.value == "" {
			continue
		}
		name, err := absoluteFile(ref.value)
		if err != nil {
			return fmt.Errorf("%s (%s): %w", ref.key, ref.value, err)
		}
		info, err := os.Stat(name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s (%s): %w", ref.key, ref.value, err)
		}
		for _, earlier := range uses {
			if !mustDiffer(ref, earlier.ref) {
				continue
			}
			if name == earlier.name || (info != nil && earlier.info != nil && os.SameFile(info, earlier.info)) {
				return refuse(ref.key, ref.value+" must differ from "+earlier.ref.key+" ("+earlier.ref.value+")")
			}
		}
		uses = append(uses, fileUse{ref: ref, name: name, info: info})
	}
	return nil
}

// mustDiffer also separates client and operator credentials, even though
// both files are read-only. Spelling and filesystem checks share this rule.
func mustDiffer(a, b pathRef) bool {
	if a.written || b.written {
		return true
	}
	if !strings.HasSuffix(a.key, ".password_file") || !strings.HasSuffix(b.key, ".password_file") {
		return false
	}
	return (strings.HasPrefix(a.key, "broker.mqtt.") && strings.HasPrefix(b.key, "broker.operations.")) ||
		(strings.HasPrefix(b.key, "broker.mqtt.") && strings.HasPrefix(a.key, "broker.operations."))
}

// absoluteFile shares absolute-name resolution between storage's base paths
// and the identity comparison of every configured file.
func absoluteFile(path string) (string, error) {
	name, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return resolveFile(name, 0)
}

// resolveFile follows links even when their target does not exist yet.
// Missing leaves keep their names after the nearest existing parent resolves.
func resolveFile(path string, links int) (string, error) {
	if links == 40 {
		return "", fmt.Errorf("too many symbolic links resolving %s", path)
	}
	info, err := os.Lstat(path)
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		return resolveFile(filepath.Clean(target), links+1)
	}
	if err == nil {
		return filepath.EvalSymlinks(path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent, err := resolveFile(filepath.Dir(path), links)
	return filepath.Join(parent, filepath.Base(path)), err
}
