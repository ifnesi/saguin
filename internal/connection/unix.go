package connection

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"syscall"

	"github.com/ifnesi/saguin/internal/store"
)

// UnixListener holds its socket's live exclusive lock until Close removes the
// socket. MQTT and operations listeners use the same binding lifecycle.
type UnixListener struct {
	*net.UnixListener
	lock *store.FileLock
}

// ListenUnix locks path, replaces only a leftover socket and applies mode
// before listening. The lock file stays in place, including after a crash.
func ListenUnix(path string, mode fs.FileMode) (_ *UnixListener, err error) {
	lock, err := store.LockFile(path + ".lock")
	if err != nil {
		return nil, err
	}
	bound := false
	defer func() {
		if err != nil {
			if bound {
				err = errors.Join(err, os.Remove(path))
			}
			err = errors.Join(err, lock.Release())
		}
	}()
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, err
	case info.Mode().Type() != fs.ModeSocket:
		return nil, fmt.Errorf("unix listener %s: existing file is not a socket", path)
	default:
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	}
	// net.ListenUnix binds and listens together. Use the socket syscalls to
	// set permissions between those steps, then hand the descriptor to net.
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, os.NewSyscallError("socket", err)
	}
	syscall.CloseOnExec(fd)
	file := os.NewFile(uintptr(fd), path)
	defer func() { _ = file.Close() }() // net.FileListener owns a duplicate
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		return nil, fmt.Errorf("bind Unix listener %s: %w", path, err)
	}
	bound = true
	if err := os.Chmod(path, mode); err != nil {
		return nil, err
	}
	if err := syscall.Listen(fd, syscall.SOMAXCONN); err != nil {
		return nil, fmt.Errorf("listen Unix listener %s: %w", path, err)
	}
	listener, err := net.FileListener(file)
	if err != nil {
		return nil, err
	}
	unix := listener.(*net.UnixListener)
	unix.SetUnlinkOnClose(true)
	return &UnixListener{UnixListener: unix, lock: lock}, nil
}

// Close removes the listening socket before releasing its lock.
func (l *UnixListener) Close() error {
	return errors.Join(l.UnixListener.Close(), l.lock.Release())
}
