// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package listeners

import (
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"

	"log/slog"
)

// TCP is a listener for establishing client connections on basic TCP protocol.
type TCP struct { // [MQTT-4.2.0-1]
	sync.RWMutex
	id      string       // the internal id of the listener
	address string       // the network address to bind to
	listen  net.Listener // a net.Listener which will listen for new clients
	config  Config       // configuration values for the listener
	log     *slog.Logger // server logger
	end     uint32       // ensure the close methods are only called once
	admit   Admission    // the server's connection count; set before Init
}

// SetAdmission is called by the server before Init: every socket is
// admitted as it is accepted (Admit), under its TLS.
func (l *TCP) SetAdmission(a Admission) { l.admit = a }

// NewTCP initializes and returns a new TCP listener, listening on an address.
func NewTCP(config Config) *TCP {
	return &TCP{
		id:      config.ID,
		address: config.Address,
		config:  config,
	}
}

// ID returns the id of the listener.
func (l *TCP) ID() string {
	return l.id
}

// Address returns the address of the listener.
func (l *TCP) Address() string {
	if l.listen != nil {
		return l.listen.Addr().String()
	}
	return l.address
}

// Protocol returns the address of the listener.
func (l *TCP) Protocol() string {
	return "tcp"
}

// Init initializes the listener.
func (l *TCP) Init(log *slog.Logger) error {
	l.log = log

	var err error
	l.listen, err = net.Listen("tcp", l.address)
	if err != nil {
		return err
	}
	l.listen = RateLimit(l.listen, l.config.ConnectRate)
	if l.admit != nil {
		l.listen = Admit(l.listen, l.admit, l.id, true)
	}
	if l.config.TLSConfig != nil {
		l.listen = tls.NewListener(l.listen, l.config.TLSConfig)
	}
	return nil
}

// Serve starts waiting for new TCP connections, and calls the establish
// connection callback for any received.
func (l *TCP) Serve(establish EstablishFn) {
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
func (l *TCP) Close(closeClients CloseFn) {
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
