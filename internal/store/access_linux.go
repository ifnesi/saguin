package store

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"slices"
	"strconv"
	"syscall"
)

const accessACL = "system.posix_acl_access"

// databaseAccess is the read grant on the disk file a snapshot will replace.
// Read it for every snapshot so an operator's changes take effect immediately.
type databaseAccess struct {
	mode  os.FileMode
	group int
	acl   []byte
}

func readDatabaseAccess(path string) (*databaseAccess, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a := &databaseAccess{mode: info.Mode(), group: int(info.Sys().(*syscall.Stat_t).Gid)}
	n, err := syscall.Getxattr(path, accessACL, nil)
	if errors.Is(err, syscall.ENODATA) || errors.Is(err, syscall.ENOTSUP) {
		return a, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ACL of %s: %w", path, err)
	}
	a.acl = make([]byte, n)
	n, err = syscall.Getxattr(path, accessACL, a.acl)
	if err != nil {
		return nil, fmt.Errorf("read ACL of %s: %w", path, err)
	}
	a.acl = a.acl[:n]
	return a, nil
}

// checkSnapshotGroup refuses a grant the broker cannot reproduce. Include
// the effective group, which need not appear in the supplementary group list.
func checkSnapshotGroup(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	group := int(info.Sys().(*syscall.Stat_t).Gid)
	groups, err := os.Getgroups()
	if err != nil {
		return err
	}
	if group == os.Getegid() || slices.Contains(groups, group) {
		return nil
	}
	broker, owningGroup := snapshotNames(group)
	return fmt.Errorf("file_path %s has group %q, which broker user %q does not belong to; add %q to group %q, or name that group in %s's ACL", path, owningGroup, broker, broker, owningGroup, path)
}

// snapshotNames supplies operator-facing names only; membership and ownership
// always use the process's IDs. The osusergo build tag keeps these lookups in
// Go. An unnamed user or group is still identified by its numeric ID.
func snapshotNames(group int) (userName, groupName string) {
	userName, groupName = strconv.Itoa(os.Getuid()), strconv.Itoa(group)
	if broker, err := user.Current(); err == nil && broker.Username != "" {
		userName = broker.Username
	}
	if owningGroup, err := user.LookupGroupId(groupName); err == nil && owningGroup.Name != "" {
		groupName = owningGroup.Name
	}
	return userName, groupName
}

// apply runs after copying: chown and writing can clear special mode bits.
// A group changed to one we cannot give is the one nonfatal preservation
// failure; mode and ACL must still survive that snapshot.
func (a *databaseAccess) apply(dst *os.File, path string) (warning, err error) {
	if a != nil {
		if err := dst.Chown(-1, a.group); errors.Is(err, syscall.EPERM) {
			broker, owningGroup := snapshotNames(a.group)
			warning = fmt.Errorf("snapshot of %s could not keep owning group %q for broker user %q: %w", path, owningGroup, broker, err)
			// A setgid directory can give the staged file a different group.
			// Install our own group explicitly, so the next snapshot observes
			// that group on file_path and needs no further warning.
			if err := dst.Chown(-1, os.Getegid()); err != nil {
				return warning, err
			}
		} else if err != nil {
			return nil, err
		}
		if err := dst.Chmod(a.mode); err != nil {
			return warning, err
		}
		if len(a.acl) != 0 {
			return warning, syscall.Setxattr(dst.Name(), accessACL, a.acl, 0)
		}
	}
	// A new file may inherit its directory's default ACL. Neither a private
	// RAM copy nor a snapshot of a file without an ACL should keep that grant.
	err = syscall.Removexattr(dst.Name(), accessACL)
	if errors.Is(err, syscall.ENODATA) || errors.Is(err, syscall.ENOTSUP) {
		err = nil
	}
	return warning, err
}
