// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package system

import "sync/atomic"

// Info contains atomic counters and values for various server statistics.
//
// mochi kept a wider set here to fill its $SYS topics, and refreshed several
// of them - uptime, the clock, heap and goroutine counts, the connected and
// disconnected client totals - only at the moment it published them. This
// engine does not publish $SYS, so those are gone rather than left to read
// as zero forever: a counter nothing writes is worse than a counter that is
// not there, because it answers.
//
// What remains is what the engine maintains at the site of the event, and
// it is read one field at a time by saguin's
// /metrics path - the only consumer this type has.
//
// **No json tags and no Clone.** Both served the HTTP stats listener the
// strip removed: it marshalled an Info to answer a request, and took a
// consistent snapshot to do it. Nothing marshals an engine type now, which
// was checked through every marshal site in saguin including the one that
// takes `any`, and Clone's only caller had become its own test.
// **The counters are atomic.Int64 rather than int64.** They are read and
// written with 64-bit atomics from several goroutines, and a raw int64 among
// them would be aligned only by where it happens to sit: on a 32-bit
// platform a 64-bit atomic on a 4-byte boundary panics, so the safety of
// every counter here would rest on nobody inserting a narrower field above
// them. The atomic types carry align64, which makes that unrepresentable
// rather than merely true today.
type Info struct {
	Version          string       // the current version of the server
	Started          int64        // the time the server started in unix seconds
	BytesReceived    atomic.Int64 // total number of bytes received since the broker started
	BytesSent        atomic.Int64 // total number of bytes sent since the broker started
	ClientsConnected atomic.Int64 // number of currently connected clients
	MessagesReceived atomic.Int64 // total number of publish messages received
	MessagesSent     atomic.Int64 // total number of publish messages sent
	InflightDropped  atomic.Int64 // the number of inflight messages which were dropped
	// PacketIDsExhausted is the part of InflightDropped refused for want of a
	// packet identifier, and SessionQueueDropped every delivery a session
	// gave up or was refused at its byte bound - the rest of InflightDropped
	// is the refused ones: different reasons a delivery never reached a
	// session, which an operator answers different ways.
	PacketIDsExhausted  atomic.Int64
	SessionQueueDropped atomic.Int64
	// DeliveriesTooLarge is every delivery discarded rather than sent because
	// it was larger than its client's Maximum Packet Size [MQTT-3.1.2-25].
	DeliveriesTooLarge atomic.Int64
	Subscriptions      atomic.Int64 // total number of subscriptions active on the broker
	// SubscribeRefused counts every filter a SUBACK refused, by the reason
	// code decided for it - before a 3.1.1 client's is written as 0x80, which
	// is that protocol's only failure and would say nothing about why.
	SubscribeRefused [256]atomic.Int64
}
