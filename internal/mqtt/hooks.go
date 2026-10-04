// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co, thedevop, dgduncan

package mqtt

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

const (
	SetOptions byte = iota
	OnStarted
	OnConnectAuthenticate
	OnACLCheck
	OnConnect
	OnSessionEstablish
	OnSessionEstablished
	OnDisconnect
	OnPacketRead
	OnPacketEncode
	OnPacketSent
	OnSubscribe
	OnSubscribed
	OnSelectSubscribers
	OnUnsubscribe
	OnUnsubscribed
	OnPublish
	OnPublishDropped
	OnRetainMessage
	OnQosPublish
	OnQosComplete
	OnQosDropped
	OnWill
	OnClientExpired
	// OnConnectRefused is appended rather than grouped with the other
	// connection events on purpose: these are iota constants, and inserting
	// one anywhere but the end renumbers every value after it.
	OnConnectRefused
	// The DeliveryKeeper calls, which follow one delivery's exchange.
	OnDeliveryReleased
	OnDeliveryDone
	// OnSessionRegistered is appended for the reason OnConnectRefused is.
	OnSessionRegistered
	// And OnConnectionRefused.
	OnConnectionRefused
	// And OnSessionSuperseded.
	OnSessionSuperseded
	// And OnSocketRefused.
	OnSocketRefused
	// And OnDisconnecting.
	OnDisconnecting
	// And OnDeliveryUnsent.
	OnDeliveryUnsent
)

var (
	// ErrInvalidConfigType indicates a different Type of config value was expected to what was received.
	ErrInvalidConfigType = errors.New("invalid config type provided")
)

// Hook provides an interface of handlers for different events which occur
// during the lifecycle of the broker.
//
// **Some hooks are called holding the client id's session lock** (saguin): the
// lock a connection holds while it claims its id, and that LockSession takes.
// They are OnSessionEstablish, OnClientExpired, OnSessionRegistered,
// OnSubscribe, OnACLCheck for a SUBSCRIBE's filters, OnSubscribed,
// OnUnsubscribe, OnUnsubscribed and OnDisconnecting - and OnPacketEncode, OnPacketSent,
// OnQosDropped, OnDeliveryDone and OnPublishDropped for what the engine writes
// or drops while it holds it: a CONNACK, a takeover's DISCONNECT, a taken-over
// connection's in-flight table, a QoS 0 PUBLISH held behind a resend with no
// room to wait in (holdForResend).
//
// **Such a hook must never call anything that takes that same lock**:
// LockSession, or a hook method that takes it, such as saguin's OnDisconnect,
// OnWill or its Will timer. The lock is not reentrant, so the connection
// waits on itself for good - which a test's hook did, and a package run hung
// for forty-five minutes on it. saguin's own code is walked for this
// (TestNoSessionLockIsTakenWhileOneIsHeld), and so are the tests' hooks
// (TestNoTestHookTakesTheLockItsHookHolds).
type Hook interface {
	ID() string
	Provides(b byte) bool
	Init(config any) error
	Stop() error
	SetOpts(l *slog.Logger, o *HookOptions)

	OnStarted()
	OnConnectAuthenticate(cl *Client, pk packets.Packet) bool
	OnACLCheck(cl *Client, topic string, write bool) bool
	OnConnect(cl *Client, pk packets.Packet) error
	OnSessionEstablish(cl *Client, pk packets.Packet) error
	OnSessionEstablished(cl *Client, pk packets.Packet)
	OnDisconnect(cl *Client, err error, expire bool)
	OnPacketRead(cl *Client, pk packets.Packet) (packets.Packet, error) // triggers when a new packet is received by a client, but before packet validation
	OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet        // modify a packet before it is byte-encoded and written to the client
	OnPacketSent(cl *Client, pk packets.Packet, b []byte)               // triggers when packet bytes have been written to the client; b is reused once it returns
	OnSubscribe(cl *Client, pk packets.Packet) packets.Packet
	OnSubscribed(cl *Client, pk packets.Packet, reasonCodes []byte)
	OnSelectSubscribers(subs *Subscribers, pk packets.Packet) *Subscribers
	OnUnsubscribe(cl *Client, pk packets.Packet) packets.Packet
	OnUnsubscribed(cl *Client, pk packets.Packet)
	OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error)
	OnPublishDropped(cl *Client, pk packets.Packet)
	OnRetainMessage(cl *Client, pk packets.Packet, r int64)
	OnQosPublish(cl *Client, pk packets.Packet, sent int64, resends int)
	OnQosComplete(cl *Client, pk packets.Packet)
	OnQosDropped(cl *Client, pk packets.Packet)
	OnWill(cl *Client, will Will) (Will, error)
	OnClientExpired(cl *Client)
}

// HookOptions contains values which are inherited from the server on initialisation.
type HookOptions struct {
	Capabilities *Capabilities
}

// Hooks is a slice of Hook interfaces to be called in sequence.
type Hooks struct {
	Log        *slog.Logger   // a logger for the hook (from the server)
	internal   atomic.Value   // a slice of []Hook
	wg         sync.WaitGroup // a waitgroup for syncing hook shutdown
	qty        atomic.Int64   // the number of hooks in use
	sync.Mutex                // a mutex for locking when adding hooks
}

// Len returns the number of hooks added.
func (h *Hooks) Len() int64 {
	return h.qty.Load()
}

// Add adds and initializes a new hook.
func (h *Hooks) Add(hook Hook, config any) error {
	h.Lock()
	defer h.Unlock()

	err := hook.Init(config)
	if err != nil {
		return fmt.Errorf("failed initialising %s hook: %w", hook.ID(), err)
	}

	i, ok := h.internal.Load().([]Hook)
	if !ok {
		i = []Hook{}
	}

	i = append(i, hook)
	h.internal.Store(i)
	h.qty.Add(1)
	h.wg.Add(1)

	return nil
}

// GetAll returns a slice of all the hooks.
func (h *Hooks) GetAll() []Hook {
	i, ok := h.internal.Load().([]Hook)
	if !ok {
		return []Hook{}
	}

	return i
}

// Stop indicates all attached hooks to gracefully end.
func (h *Hooks) Stop() {
	go func() {
		for _, hook := range h.GetAll() {
			h.Log.Info("stopping hook", "hook", hook.ID())
			if err := hook.Stop(); err != nil {
				h.Log.Debug("problem stopping hook", "error", err, "hook", hook.ID())
			}

			h.wg.Done()
		}
	}()

	h.wg.Wait()
}

// OnStarted is called when the server has successfully started.
func (h *Hooks) OnStarted() {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnStarted) {
			hook.OnStarted()
		}
	}
}

// OnConnect is called when a new client connects, and may return a packets.Code as an error to halt the connection.
func (h *Hooks) OnConnect(cl *Client, pk packets.Packet) error {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnConnect) {
			err := hook.OnConnect(cl, pk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// OnSessionEstablish is called right after a new client connects and authenticates and right before
// the session is established and CONNACK is sent.
//
// **A hook may refuse the connection here, and this is the last moment one
// can** (saguin): the CONNACK's reason code is decided by what this returns,
// so a hook that has to keep something before the client is told it is
// connected - a Will, which MQTT makes session state - can answer the client
// with the reason it failed instead of accepting it and hanging up a moment
// later. The error is the code the CONNACK carries; the first hook to refuse
// decides it, and the rest are not asked.
func (h *Hooks) OnSessionEstablish(cl *Client, pk packets.Packet) error {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnSessionEstablish) {
			if err := hook.OnSessionEstablish(cl, pk); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnSessionEstablished is called when a new client establishes a session (after OnConnect).
func (h *Hooks) OnSessionEstablished(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnSessionEstablished) {
			hook.OnSessionEstablished(cl, pk)
		}
	}
}

// OnSessionRegistered is called once a connection is registered as its client
// id's, before the id's session lock is let go (saguin). Nothing else can
// claim the id until it returns, so a hook that must finish changing what the
// id holds before another connection can take it does that here - and
// publishes nothing, since a publish can wait on another id's session lock
// (Server.AfterSessionUnlock).
//
// Asserted rather than named on the Hook interface, as OnConnectRefused is: a
// hook that has nothing to finish under the lock says nothing.
func (h *Hooks) OnSessionRegistered(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if !hook.Provides(OnSessionRegistered) {
			continue
		}
		if r, ok := hook.(interface {
			OnSessionRegistered(cl *Client, pk packets.Packet)
		}); ok {
			r.OnSessionRegistered(cl, pk)
		}
	}
}

// OnSessionSuperseded is called when a connection is answered without
// claiming its client id, because a newer CONNECT for the id superseded it
// as it waited (saguin): it has been sent its CONNACK and the takeover's
// DISCONNECT, and no other session hook hears of it. successorClean is
// whether the CONNECT that would have taken the session from it asked for a
// clean start, which is what a taken-over connection's delayed Will turns
// on. It is called holding no lock, so a hook may publish from it.
//
// Asserted rather than named on the Hook interface, as OnConnectRefused is.
func (h *Hooks) OnSessionSuperseded(cl *Client, pk packets.Packet, successorClean bool) {
	for _, hook := range h.GetAll() {
		if !hook.Provides(OnSessionSuperseded) {
			continue
		}
		if r, ok := hook.(interface {
			OnSessionSuperseded(cl *Client, pk packets.Packet, successorClean bool)
		}); ok {
			r.OnSessionSuperseded(cl, pk, successorClean)
		}
	}
}

// OnConnectionRefused is called when the engine ends an established
// connection for a refusal of its own (saguin): a packet it refused while
// decoding it, one over the Maximum Packet Size, and a packet processPacket
// answered with a reason code. Once it returns, an MQTT 5 client is sent a
// DISCONNECT carrying the code, and the connection is closed.
//
// **Once for each connection a refusal ended, and only for one.** It is not
// called for a connection a hook ended first, which told itself, nor for a
// takeover, a shutdown, a keepalive or a write that failed: none of them is
// a refusal. It is called on the connection's read loop, never holding the
// id's session lock.
//
// Asserted rather than named on the Hook interface, as OnConnectRefused is.
func (h *Hooks) OnConnectionRefused(cl *Client, code packets.Code) {
	for _, hook := range h.GetAll() {
		if !hook.Provides(OnConnectionRefused) {
			continue
		}
		if r, ok := hook.(interface {
			OnConnectionRefused(cl *Client, code packets.Code)
		}); ok {
			r.OnConnectionRefused(cl, code)
		}
	}
}

// OnSocketRefused is called when a listener closes a socket at accept for a
// refusal, before there is a connection or a client: a ws socket that finds
// every max_connections slot taken (listeners.Admission). There is no client
// id and nothing was written to the socket.
//
// Asserted rather than named on the Hook interface, as OnConnectRefused is.
func (h *Hooks) OnSocketRefused(listener string, code packets.Code) {
	for _, hook := range h.GetAll() {
		if !hook.Provides(OnSocketRefused) {
			continue
		}
		if r, ok := hook.(interface {
			OnSocketRefused(listener string, code packets.Code)
		}); ok {
			r.OnSocketRefused(listener, code)
		}
	}
}

// OnConnectRefused is called when the server refuses a CONNECT before any
// other hook has run, so that an embedder can see which client it was.
//
// **Only the refusals no hook can otherwise observe**: the maximum-clients
// bound, a CONNECT refused while it was read - malformed, or over the
// Maximum Packet Size - and the checks in validateConnect - an unacceptable
// protocol version, a Will above the maximum QoS, a Will asking to be
// retained where retention is unavailable, and a zero-length client id on a
// session asked to persist. An authentication refusal is not among them: the
// hook that made that decision already knows.
//
// The client has been parsed by this point, so its identifier, username,
// listener and protocol version are all set - except for a CONNECT refused
// while it was read, whose packet carries only what was read before the
// refusal, and may carry nothing. Without this the only record
// of a refused connection is the returned code, which carries nothing about
// who was refused - and "which of my devices is being turned away" is the
// first question an operator asks.
func (h *Hooks) OnConnectRefused(cl *Client, pk packets.Packet, code packets.Code) {
	for _, hook := range h.GetAll() {
		if !hook.Provides(OnConnectRefused) {
			continue
		}

		// Asserted rather than named on the Hook interface, and the reason
		// has outlived the argument that produced it.
		//
		// Upstream, the point was that Hook carried 43 methods, every hook
		// in the wild embedded HookBase to get them, and an embedder who
		// implemented the interface directly would stop compiling the day
		// a method was added - which a minor release had no business
		// doing. That was a promise to somebody else's build. This engine
		// is internal to saguin, it carries 28 methods rather than 43, and
		// there are no embedders outside this repository to break.
		//
		// **It stays because the shape is still the right one**, not
		// because of the old reason: a hook that does not care about
		// refusals says nothing, and TestAHookWithoutHookBaseStillSatisfies
		// TheInterface holds the case the assertion exists for.
		if refuser, ok := hook.(interface {
			OnConnectRefused(cl *Client, pk packets.Packet, code packets.Code)
		}); ok {
			refuser.OnConnectRefused(cl, pk, code)
		}
	}
}

// Disconnector is a hook told of a client's DISCONNECT once the engine has
// accepted it, before the connection is closed (saguin): called holding the
// client id's session lock, from a connection that still owns the id, with
// the Session Expiry Interval the DISCONNECT carried already applied. What it
// stores is stored before the close that answers the DISCONNECT (invariant
// 18).
type Disconnector interface {
	OnDisconnecting(cl *Client, pk packets.Packet)
}

// OnDisconnecting tells every Disconnector a client's DISCONNECT was accepted.
func (h *Hooks) OnDisconnecting(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if d, ok := hook.(Disconnector); ok && hook.Provides(OnDisconnecting) {
			d.OnDisconnecting(cl, pk)
		}
	}
}

// DeliveryKeeper is a hook told of a delivery's exchange with the client: its
// PUBREC, and its leaving the in-flight table. Saguin's broadcast log drain is
// one, since the session's in-flight table there is what a restart re-sends.
// Each call names one in-flight entry of a delivery to the client, a PUBLISH
// at QoS 1 or 2 or the PUBREL that stands for one after its PUBREC.
type DeliveryKeeper interface {
	// OnDeliveryReleased says the client answered a QoS 2 delivery with a
	// successful PUBREC, so what it is owed is the PUBREL. An error is a
	// keeper that could not store that, and the PUBREL is not sent.
	OnDeliveryReleased(cl *Client, pk packets.Packet) error
	// OnDeliveryDone says a delivery has left the in-flight table other than
	// by its acknowledgement: refused by a PUBREC, expired, or cleared with its
	// connection's table. An acknowledged one is OnQosComplete's, and one given
	// up at the session's bound is nobody's.
	OnDeliveryDone(cl *Client, pk packets.Packet)
}

// OnDeliveryUnsent is called as a delivery leaves the in-flight table
// expired before it was ever sent - by the expiry sweep, or by the writer
// that claimed its first send (Client.retiredUnsent) - after OnDeliveryDone
// and before its identifier is released (saguin).
//
// **Its own call, because "dropped" says nothing of whether the client has
// it.** OnQosDropped is also a sent delivery's at its session's end, and
// OnDeliveryDone a cleared table's, so neither can tell a hook that holds
// work for the client that the client never saw it. Saguin gives a queue's
// job back for exactly that (invariant 7): told only of a drop, it kept the
// job held by a worker that never had it, waiting for a PUBACK that could
// not come.
//
// Asserted rather than named on the Hook interface, as OnSessionRegistered
// is.
func (h *Hooks) OnDeliveryUnsent(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if !hook.Provides(OnDeliveryUnsent) {
			continue
		}
		if u, ok := hook.(interface {
			OnDeliveryUnsent(cl *Client, pk packets.Packet)
		}); ok {
			u.OnDeliveryUnsent(cl, pk)
		}
	}
}

// OnDeliveryReleased tells every DeliveryKeeper a QoS 2 delivery was
// received, and answers the first error one returns.
func (h *Hooks) OnDeliveryReleased(cl *Client, pk packets.Packet) error {
	for _, hook := range h.GetAll() {
		if k, ok := hook.(DeliveryKeeper); ok && hook.Provides(OnDeliveryReleased) {
			if err := k.OnDeliveryReleased(cl, pk); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnDeliveryDone tells every DeliveryKeeper a delivery has left the in-flight
// table.
func (h *Hooks) OnDeliveryDone(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if k, ok := hook.(DeliveryKeeper); ok && hook.Provides(OnDeliveryDone) {
			k.OnDeliveryDone(cl, pk)
		}
	}
}

// OnDisconnect is called when a client is disconnected for any reason.
func (h *Hooks) OnDisconnect(cl *Client, err error, expire bool) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnDisconnect) {
			hook.OnDisconnect(cl, err, expire)
		}
	}
}

// OnPacketRead is called when a packet is received from a client.
func (h *Hooks) OnPacketRead(cl *Client, pk packets.Packet) (pkx packets.Packet, err error) {
	pkx = pk
	for _, hook := range h.GetAll() {
		if hook.Provides(OnPacketRead) {
			npk, err := hook.OnPacketRead(cl, pkx)
			if err != nil && errors.Is(err, packets.ErrRejectPacket) {
				h.Log.Debug("packet rejected", "hook", hook.ID(), "packet", pkx)
				return pk, err
			} else if err != nil {
				continue
			}

			pkx = npk
		}
	}

	return
}

// OnPacketEncode is called immediately before a packet is encoded to be sent to a client.
func (h *Hooks) OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnPacketEncode) {
			pk = hook.OnPacketEncode(cl, pk)
		}
	}

	return pk
}

// OnPacketSent is called when a packet has been sent to a client. It takes a bytes parameter
// containing the bytes sent.
func (h *Hooks) OnPacketSent(cl *Client, pk packets.Packet, b []byte) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnPacketSent) {
			hook.OnPacketSent(cl, pk, b)
		}
	}
}

// OnSubscribe is called when a client subscribes to one or more filters. This method
// differs from OnSubscribed in that it allows you to modify the subscription values
// before the packet is processed. The return values of the hook methods are passed-through
// in the order the hooks were attached.
func (h *Hooks) OnSubscribe(cl *Client, pk packets.Packet) packets.Packet {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnSubscribe) {
			pk = hook.OnSubscribe(cl, pk)
		}
	}
	return pk
}

// OnSubscribed is called when a client subscribes to one or more filters.
func (h *Hooks) OnSubscribed(cl *Client, pk packets.Packet, reasonCodes []byte) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnSubscribed) {
			hook.OnSubscribed(cl, pk, reasonCodes)
		}
	}
}

// OnSelectSubscribers is called when subscribers have been collected for a topic, but before
// shared subscription subscribers have been selected. This hook can be used to programmatically
// remove or add clients to a publish to subscribers process, or to select the subscriber for a shared
// group in a custom manner (such as based on client id, ip, etc).
func (h *Hooks) OnSelectSubscribers(subs *Subscribers, pk packets.Packet) *Subscribers {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnSelectSubscribers) {
			subs = hook.OnSelectSubscribers(subs, pk)
		}
	}
	return subs
}

// OnUnsubscribe is called when a client unsubscribes from one or more filters. This method
// differs from OnUnsubscribed in that it allows you to modify the unsubscription values
// before the packet is processed. The return values of the hook methods are passed-through
// in the order the hooks were attached.
func (h *Hooks) OnUnsubscribe(cl *Client, pk packets.Packet) packets.Packet {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnUnsubscribe) {
			pk = hook.OnUnsubscribe(cl, pk)
		}
	}
	return pk
}

// OnUnsubscribed is called when a client unsubscribes from one or more filters.
func (h *Hooks) OnUnsubscribed(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnUnsubscribed) {
			hook.OnUnsubscribed(cl, pk)
		}
	}
}

// OnPublish is called when a client publishes a message, and may modify the incoming
// packet before it is processed.
// The return values of the hook methods are passed-through in the order the hooks were attached.
func (h *Hooks) OnPublish(cl *Client, pk packets.Packet) (pkx packets.Packet, err error) {
	pkx = pk
	for _, hook := range h.GetAll() {
		if hook.Provides(OnPublish) {
			npk, err := hook.OnPublish(cl, pkx)
			if err != nil {
				if errors.Is(err, packets.ErrRejectPacket) {
					h.Log.Debug("publish packet rejected",
						"error", err,
						"hook", hook.ID(),
						"packet", pkx)
					return pk, err
				} else if errors.Is(err, packets.CodeSuccessIgnore) {
					return pk, err
				}
				// A hook answering with a reason code has made a decision,
				// not hit a fault: processPublish turns it into the PUBACK
				// the client receives. Reporting an ordinary refusal - a
				// quota exceeded, an invalid topic name - at error level
				// tells an operator the server failed when it worked, and
				// formats the whole packet, payload included, to say so.
				var code packets.Code
				if errors.As(err, &code) {
					h.Log.Debug("publish packet refused by hook",
						"code", code.Code,
						"reason", code.Reason,
						"hook", hook.ID(),
						"packet", pkx)
					return pk, err
				}
				h.Log.Error("publish packet error",
					"error", err,
					"hook", hook.ID(),
					"packet", pkx)
				return pk, err
			}
			pkx = npk
		}
	}

	return
}

// OnPublishDropped is called when a message to a client was dropped instead of delivered
// such as when a client is too slow to respond.
func (h *Hooks) OnPublishDropped(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnPublishDropped) {
			hook.OnPublishDropped(cl, pk)
		}
	}
}

// OnRetainMessage is called then a published message is retained.
func (h *Hooks) OnRetainMessage(cl *Client, pk packets.Packet, r int64) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnRetainMessage) {
			hook.OnRetainMessage(cl, pk, r)
		}
	}
}

// OnQosPublish is called when a publish packet with Qos >= 1 is issued to a subscriber.
// In other words, this method is called when a new inflight message is created or resent.
// It is typically used to store a new inflight message.
func (h *Hooks) OnQosPublish(cl *Client, pk packets.Packet, sent int64, resends int) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnQosPublish) {
			hook.OnQosPublish(cl, pk, sent, resends)
		}
	}
}

// OnQosComplete is called when the Qos flow for a message has been completed.
// In other words, when an inflight message is resolved.
// It is typically used to delete an inflight message from a store.
func (h *Hooks) OnQosComplete(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnQosComplete) {
			hook.OnQosComplete(cl, pk)
		}
	}
}

// OnQosDropped is called the Qos flow for a message expires. In other words, when
// an inflight message expires or is abandoned. It is typically used to delete an
// inflight message from a store.
func (h *Hooks) OnQosDropped(cl *Client, pk packets.Packet) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnQosDropped) {
			hook.OnQosDropped(cl, pk)
		}
	}
}

// OnWill is called when a client disconnects and publishes an LWT message, and may
// modify the LWT message before it is published. The return values of the hook methods are passed-through in the order
// the hooks were attached.
func (h *Hooks) OnWill(cl *Client, will Will) Will {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnWill) {
			mlwt, err := hook.OnWill(cl, will)
			if err != nil {
				// The Will itself is not logged: its topic and payload are
				// the client's, and unbounded. The client id says whose it
				// was, which is the question an operator has here.
				h.Log.Error("parse will error",
					"error", err,
					"hook", hook.ID(),
					"client", cl.ID)
				continue
			}
			will = mlwt
		}
	}

	return will
}

// OnClientExpired is called when a client session has expired and should be deleted.
func (h *Hooks) OnClientExpired(cl *Client) {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnClientExpired) {
			hook.OnClientExpired(cl)
		}
	}
}

// OnConnectAuthenticate is called when a user attempts to authenticate with the server.
// An implementation of this method MUST be used to allow or deny access to the
// server (see hooks/auth/allow_all or basic). It can be used in custom hooks to
// check connecting users against an existing user database.
func (h *Hooks) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnConnectAuthenticate) {
			if ok := hook.OnConnectAuthenticate(cl, pk); ok {
				return true
			}
		}
	}

	return false
}

// OnACLCheck is called when a user attempts to publish or subscribe to a topic filter.
// An implementation of this method MUST be used to allow or deny access to the
// (see hooks/auth/allow_all or basic). It can be used in custom hooks to
// check publishing and subscribing users against an existing permissions or roles database.
func (h *Hooks) OnACLCheck(cl *Client, topic string, write bool) bool {
	for _, hook := range h.GetAll() {
		if hook.Provides(OnACLCheck) {
			if ok := hook.OnACLCheck(cl, topic, write); ok {
				return true
			}
		}
	}

	return false
}

// HookBase provides a set of default methods for each hook. It should be embedded in
// all hooks.
type HookBase struct {
	Hook
	Log  *slog.Logger
	Opts *HookOptions
}

// ID returns the ID of the hook.
func (h *HookBase) ID() string {
	return "base"
}

// Init performs any pre-start initializations for the hook, such as connecting to databases
// or opening files.
func (h *HookBase) Init(config any) error {
	return nil
}

// SetOpts is called by the server to propagate internal values and generally should
// not be called manually.
func (h *HookBase) SetOpts(l *slog.Logger, opts *HookOptions) {
	h.Log = l
	h.Opts = opts
}

// Provides indicates which methods a hook provides. The default is none - this method
// should be overridden by the embedding hook.
func (h *HookBase) Provides(b byte) bool {
	return false
}

// Stop is called to gracefully shut down the hook.
func (h *HookBase) Stop() error {
	return nil
}

// OnStarted is called when the server starts.
func (h *HookBase) OnStarted() {}

// OnConnectAuthenticate is called when a user attempts to authenticate with the server.
func (h *HookBase) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool {
	return false
}

// OnACLCheck is called when a user attempts to subscribe or publish to a topic.
func (h *HookBase) OnACLCheck(cl *Client, topic string, write bool) bool {
	return false
}

// OnConnect is called when a new client connects.
func (h *HookBase) OnConnect(cl *Client, pk packets.Packet) error {
	return nil
}

// OnSessionEstablish is called right after a new client connects and authenticates and right before
// the session is established and CONNACK is sent.
func (h *HookBase) OnSessionEstablish(cl *Client, pk packets.Packet) error { return nil }

// OnSessionEstablished is called when a new client establishes a session (after OnConnect).
func (h *HookBase) OnSessionEstablished(cl *Client, pk packets.Packet) {}

// OnDisconnect is called when a client is disconnected for any reason.
func (h *HookBase) OnDisconnect(cl *Client, err error, expire bool) {}

// OnConnectRefused is a no-op, so every existing hook is unaffected.
func (h *HookBase) OnConnectRefused(cl *Client, pk packets.Packet, code packets.Code) {}

// OnPacketRead is called when a packet is received.
func (h *HookBase) OnPacketRead(cl *Client, pk packets.Packet) (packets.Packet, error) {
	return pk, nil
}

// OnPacketEncode is called before a packet is byte-encoded and written to the client.
func (h *HookBase) OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnPacketSent is called immediately after a packet is written to a client.
func (h *HookBase) OnPacketSent(cl *Client, pk packets.Packet, b []byte) {}

// OnSubscribe is called when a client subscribes to one or more filters.
func (h *HookBase) OnSubscribe(cl *Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnSubscribed is called when a client subscribes to one or more filters.
func (h *HookBase) OnSubscribed(cl *Client, pk packets.Packet, reasonCodes []byte) {}

// OnSelectSubscribers is called when selecting subscribers to receive a message.
func (h *HookBase) OnSelectSubscribers(subs *Subscribers, pk packets.Packet) *Subscribers {
	return subs
}

// OnUnsubscribe is called when a client unsubscribes from one or more filters.
func (h *HookBase) OnUnsubscribe(cl *Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnUnsubscribed is called when a client unsubscribes from one or more filters.
func (h *HookBase) OnUnsubscribed(cl *Client, pk packets.Packet) {}

// OnPublish is called when a client publishes a message.
func (h *HookBase) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	return pk, nil
}

// OnPublishDropped is called when a message to a client is dropped instead of being delivered.
func (h *HookBase) OnPublishDropped(cl *Client, pk packets.Packet) {}

// OnRetainMessage is called then a published message is retained.
func (h *HookBase) OnRetainMessage(cl *Client, pk packets.Packet, r int64) {}

// OnQosPublish is called when a publish packet with Qos > 1 is issued to a subscriber.
func (h *HookBase) OnQosPublish(cl *Client, pk packets.Packet, sent int64, resends int) {}

// OnQosComplete is called when the Qos flow for a message has been completed.
func (h *HookBase) OnQosComplete(cl *Client, pk packets.Packet) {}

// OnQosDropped is called the Qos flow for a message expires.
func (h *HookBase) OnQosDropped(cl *Client, pk packets.Packet) {}

// OnWill is called when a client disconnects and publishes an LWT message.
func (h *HookBase) OnWill(cl *Client, will Will) (Will, error) {
	return will, nil
}

// OnClientExpired is called when a client session has expired.
func (h *HookBase) OnClientExpired(cl *Client) {}
