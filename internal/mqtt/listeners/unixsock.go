// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: jason@zgwit.com

package listeners

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"log/slog"
)

// ListenUnix binds a Unix socket at path, holding path+".lock" for as long as
// the listener is open, and gives the socket mode (zero leaves it as bound).
//
// **The lock is how saguin tells a socket left behind from one still in use**,
// and it is the interlock PostgreSQL and MySQL put beside their sockets and
// tmux beside its own. A broker killed outright, crashed or cut off by a power
// failure leaves its socket file but not its lock, because the lock is an
// flock and the kernel drops it with the process. So:
//
//   - the lock is free: whatever socket is at the path was left behind, and it
//     is replaced, which is what lets an unattended broker restart by itself;
//   - the lock is held: another saguin is serving that path, and this one
//     refuses rather than deleting the socket from under it - which is what
//     used to happen, leaving the first broker running and unreachable;
//   - something at the path is not a socket: it is refused and left alone,
//     because a regular file or directory there is a mistake in the
//     configuration, not a leftover.
//
// Closing the listener removes its socket while the lock is still held, so it
// can only ever remove its own, and then releases the lock. The lock file
// itself is kept: removing it would let a starting broker lock a file that is
// then unlinked from under it. Abstract names, which begin with "@", have no
// file to leave behind or to lock.
func ListenUnix(path string, mode os.FileMode) (net.Listener, error) {
	if strings.HasPrefix(path, "@") {
		return net.Listen("unix", path)
	}

	lock, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening the lock beside the socket %s: %w", path, err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("the socket %s is in use: another saguin holds %s", path, path+".lock")
		}
		return nil, fmt.Errorf("locking %s: %w", path+".lock", err)
	}
	fail := func(err error) (net.Listener, error) {
		_ = lock.Close()
		return nil, err
	}

	switch fi, err := os.Lstat(path); {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return fail(fmt.Errorf("checking the socket path %s: %w", path, err))
	case fi.Mode()&os.ModeSocket == 0:
		return fail(fmt.Errorf("%s exists and is not a socket, so it is not a socket saguin left "+
			"behind and saguin will not remove it", path))
	default:
		if err := os.Remove(path); err != nil {
			return fail(fmt.Errorf("replacing the socket %s left behind: %w", path, err))
		}
	}

	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return fail(err)
	}
	// Removed by Close below, under the lock, and nowhere else.
	ln.SetUnlinkOnClose(false)
	if mode != 0 {
		if err := os.Chmod(path, mode); err != nil {
			_ = ln.Close()
			_ = os.Remove(path)
			return fail(fmt.Errorf("permissions on %s: %w", path, err))
		}
	}
	return &lockedUnix{UnixListener: ln, path: path, lock: lock}, nil
}

// lockedUnix is a Unix listener holding the lock beside its socket.
type lockedUnix struct {
	*net.UnixListener
	path string
	lock *os.File
	once sync.Once
	err  error
}

// Close stops listening, removes the socket while the lock is still held, and
// releases the lock. A second Close does nothing.
func (l *lockedUnix) Close() error {
	l.once.Do(func() {
		l.err = l.UnixListener.Close()
		_ = os.Remove(l.path)
		_ = l.lock.Close()
	})
	return l.err
}

// UnixSock is a listener for establishing client connections on basic UnixSock protocol.
type UnixSock struct {
	sync.RWMutex
	id      string       // the internal id of the listener.
	address string       // the network address to bind to.
	config  Config       // configuration values for the listener
	listen  net.Listener // a net.Listener which will listen for new clients.
	log     *slog.Logger // server logger
	end     uint32       // ensure the close methods are only called once.
	admit   Admission    // the server's connection count; set before Init
}

// NewUnixSock initializes and returns a new UnixSock listener, listening on an address.
func NewUnixSock(config Config) *UnixSock {
	return &UnixSock{
		id:      config.ID,
		address: config.Address,
		config:  config,
	}
}

// ID returns the id of the listener.
func (l *UnixSock) ID() string {
	return l.id
}

// Address returns the address of the listener.
func (l *UnixSock) Address() string {
	return l.address
}

// Protocol returns the address of the listener.
func (l *UnixSock) Protocol() string {
	return "unix"
}

// Init initializes the listener.
func (l *UnixSock) Init(log *slog.Logger) error {
	l.log = log

	var err error
	l.listen, err = ListenUnix(l.address, l.config.FileMode)
	if err != nil {
		return err
	}
	l.listen = RateLimit(l.listen, l.config.ConnectRate)
	if l.admit != nil {
		l.listen = Admit(l.listen, l.admit, l.id, true)
	}
	return nil
}

// SetAdmission is called by the server before Init: every socket is
// admitted as it is accepted (Admit).
func (l *UnixSock) SetAdmission(a Admission) { l.admit = a }

// Serve starts waiting for new UnixSock connections, and calls the establish
// connection callback for any received.
func (l *UnixSock) Serve(establish EstablishFn) {
	for {
		if atomic.LoadUint32(&l.end) == 1 {
			return
		}

		conn, err := l.listen.Accept()
		if err != nil {
			return
		}

		// **Accepted as the door shut: closed, not left.** Accept admitted
		// it, so it holds a max_connections slot that only its Close gives
		// back; left here, nothing would ever close it.
		if atomic.LoadUint32(&l.end) == 1 {
			_ = conn.Close()
			return
		}
		go func() {
			err = establish(l.id, conn)
			if err != nil {
				l.log.Warn("connection ended with an error", "error", err)
			}
		}()
	}
}

// Close closes the listener and any client connections.
func (l *UnixSock) Close(closeClients CloseFn) {
	l.Lock()
	defer l.Unlock()

	if atomic.CompareAndSwapUint32(&l.end, 0, 1) {
		closeClients(l.id)
	}

	if l.listen != nil {
		err := l.listen.Close()
		if err != nil {
			return
		}
	}
}
