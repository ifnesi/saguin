package sqlite_test

// A whole broker on a sqlite provider, whose write queue is held and filled
// from inside (export_test.go), so that what a client's packet waits behind
// is decided rather than raced.

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"

	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/store/sqlite"
)

// departedSlow is how long each queued departed write takes to run: three
// full groups of them are about a second of the one writer, far longer than
// a packet's own writes take to join.
const departedSlow = 30 * time.Millisecond

// waitFor fails t unless cond holds within ten seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("waited ten seconds for %s", what)
		}
	}
}

// ranBefore reports whether client's kth write in ran (counting from 1) came
// before the nth write of a departed client's (prefix gone-, counting from
// 0).
func ranBefore(ran []string, client string, k, n int) bool {
	at, seen, gone := -1, 0, 0
	nth := len(ran)
	for i, c := range ran {
		if c == client {
			if seen++; seen == k {
				at = i
			}
		}
		if strings.HasPrefix(c, "gone-") {
			if gone == n {
				nth = i
			}
			gone++
		}
	}
	return at >= 0 && at < nth
}

// **A clean DISCONNECT's write, held under its id's session lock, is moved up
// by the next CONNECT for the id** (mqtt.Server.ClientWaiting, DB.Waiting).
// The DISCONNECT's Disconnected is a departed client's, queued behind three
// full groups of other departed clients' writes while the connection holds
// the id's session lock to write it. A CONNECT for the id waits on that lock;
// marked as it is read, before it takes the lock, it moves the write up and
// is answered, the write running before the second group - the first may be
// the turn the rest are owed after four answering groups (handOn) - rather
// than behind all three, as when marked only once it held the lock, which it
// could not take before they had run.
func TestAPreCloseDisconnectIsMovedUpByTheNextConnect(t *testing.T) {
	h := brokertest.StartDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	db := h.DB
	c := brokertest.Dial(t, h, "dev", true, false, 10, 3600, 0)
	ran := sqlite.RecordRuns(t, db)
	release := sqlite.HoldACommit(t, db)
	allRan := sqlite.QueueDeparted(t, db, 3*sqlite.SessionGroupMax, "gone-", departedSlow)
	if err := c.C.Disconnect(&paho.Disconnect{ReasonCode: 0}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the DISCONNECT's write to queue among the departed", func() bool {
		return sqlite.QueuedFor(db, "dev") == 1
	})

	type answer struct {
		ca  *paho.Connack
		err error
	}
	answered := make(chan answer, 1)
	go func() {
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			answered <- answer{err: err}
			return
		}
		cl := paho.NewClient(paho.ClientConfig{Conn: conn})
		expiry := uint32(3600)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		ca, err := cl.Connect(ctx, &paho.Connect{ClientID: "dev", KeepAlive: 30,
			Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry}})
		answered <- answer{ca, err}
		if err == nil {
			_ = cl.Disconnect(&paho.Disconnect{})
		}
		conn.Close()
	}()
	waitFor(t, "the CONNECT to move its id's queued write up while the writer is held", func() bool {
		return sqlite.Marked(db, "dev") > 0 && sqlite.QueuedFor(db, "dev") == 0 &&
			sqlite.QueuedAnswering(db, "dev") == 1
	})
	select {
	case a := <-answered:
		t.Fatalf("the CONNECT was answered while its predecessor's write was held: %+v %v", a.ca, a.err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	a := <-answered
	if a.err != nil || a.ca.ReasonCode != 0 {
		t.Fatalf("connect: %+v %v", a.ca, a.err)
	}
	allRan()
	if got := ran(); !ranBefore(got, "dev", 1, sqlite.SessionGroupMax) {
		t.Fatalf("the DISCONNECT's write ran after departed clients' writes queued before it: %v", got)
	}
}

// subscribedConsumer is a durable consumer of events, with backlog behind it
// so that the head is past its start.
func subscribedConsumer(t *testing.T, h *brokertest.Harness, subscribe bool) *brokertest.Client {
	t.Helper()
	pub := brokertest.Dial(t, h, "pub", true, false, 10, 0, 0)
	for i := range 3 {
		pub.Pub(t, "events/x", fmt.Sprint("old", i))
	}
	cons := brokertest.Dial(t, h, "cons", true, false, 10, 3600, 0)
	if subscribe {
		cons.Sub(t, "events/#", 1)
	}
	return cons
}

// **A SUBSCRIBE with Retain Handling 2 is served ahead of departed clients'
// writes**: its record's write and its position moved to the head, both
// written while the SUBSCRIBE is marked (mqtt.Server.ClientWaiting), are
// each queued among the answering writes, ahead of five full groups of
// departed clients' writes queued before them - so the position is stored
// before the last of them. Between the two the SUBSCRIBE prepares and reads
// the consumer's stored position on the write connection (Broker.cursorC),
// each waiting for the group under way, which no queue decides. Unmarked,
// the record was a departed client's write behind all five, and the
// position a connected session's.
func TestASubscribeAskingForNoReplayGoesAheadOfDepartedWrites(t *testing.T) {
	h := brokertest.StartDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	db := h.DB
	cons := subscribedConsumer(t, h, false)
	ran := sqlite.RecordRuns(t, db)
	release := sqlite.HoldACommit(t, db)
	allRan := sqlite.QueueDeparted(t, db, 5*sqlite.SessionGroupMax, "gone-", departedSlow)
	subacked := make(chan error, 1)
	go func() {
		sa, err := cons.C.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
			{Topic: "events/#", QoS: 1, RetainHandling: 2}}})
		if err == nil && (len(sa.Reasons) != 1 || sa.Reasons[0] != 1) {
			err = fmt.Errorf("SUBACK %v", sa.Reasons)
		}
		subacked <- err
	}()
	waitFor(t, "the SUBSCRIBE's record write to queue among the answering", func() bool {
		return sqlite.QueuedAnswering(db, "cons") == 1
	})
	release()
	if err := <-subacked; err != nil {
		t.Fatal(err)
	}
	// The position at the head, joining once the SUBACK is written, is
	// queued among the answering writes too, behind the group under way.
	waitFor(t, "the position at the head to queue among the answering", func() bool {
		return sqlite.QueuedAnswering(db, "cons") == 1
	})
	allRan()
	got := ran()
	if n := strings.Count(strings.Join(got, " "), "cons"); n != 2 {
		t.Fatalf("cons's record and position are 2 writes, and %d ran: %v", n, got)
	}
	if !ranBefore(got, "cons", 2, 4*sqlite.SessionGroupMax) {
		t.Fatalf("the SUBSCRIBE's position ran behind the departed clients' last group: %v", got)
	}
}

// **A seek is served ahead of departed clients' writes**: the position it
// stores, written while its PUBLISH is marked (mqtt.Server.ClientWaiting),
// runs before the second of three full groups of departed writes queued
// before it - the first may be the turn the rest are owed (handOn) - rather
// than as a connected session's write behind them all.
func TestASeekGoesAheadOfDepartedWrites(t *testing.T) {
	h := brokertest.StartDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	db := h.DB
	cons := subscribedConsumer(t, h, true)
	ran := sqlite.RecordRuns(t, db)
	release := sqlite.HoldACommit(t, db)
	allRan := sqlite.QueueDeparted(t, db, 3*sqlite.SessionGroupMax, "gone-", departedSlow)
	acked := make(chan error, 1)
	go func() {
		ack, err := cons.C.Publish(context.Background(), &paho.Publish{
			Topic: "$saguin/consumer/events/seek", QoS: 1, Payload: []byte("1")})
		if err == nil && ack.ReasonCode != 0 {
			err = fmt.Errorf("PUBACK %#x", ack.ReasonCode)
		}
		acked <- err
	}()
	waitFor(t, "the seek's position write to queue among the answering", func() bool {
		return sqlite.QueuedAnswering(db, "cons") == 1
	})
	release()
	if err := <-acked; err != nil {
		t.Fatal(err)
	}
	allRan()
	got := ran()
	if !slices.Contains(got, "cons") {
		t.Fatalf("no position of cons's was written: %v", got)
	}
	if !ranBefore(got, "cons", 1, sqlite.SessionGroupMax) {
		t.Fatalf("the seek's position ran behind departed clients' writes: %v", got)
	}
}

// **A PUBREL is served ahead of departed clients' writes**, whatever the
// release writes: a held deletion of a topic with no value is released by
// forgetting the hold, which is a departed client's write unless the PUBREL
// is marked (DB.unhold). The release runs from a hook as the packet is read
// (Broker.OnPacketRead), before the PUBCOMP, so the PUBREL has to be marked
// from the moment it is decoded: marked only once that hook had returned,
// the write queued behind three full groups of departed writes and the
// PUBCOMP waited on all of them.
func TestAReleasedDeletionGoesAheadOfDepartedWrites(t *testing.T) {
	h := brokertest.StartDurableSQLite(t, filepath.Join(t.TempDir(), "saguin.db"))
	db := h.DB
	w, _ := brokertest.QoS2Dial(t, h.Addr, "dev", true, 0)
	w.Publish(1, "state/absent", "", false)
	if rc := w.Pubrec(1); rc != 0 {
		t.Fatalf("PUBREC for a held deletion carried 0x%02X, want success", rc)
	}
	ran := sqlite.RecordRuns(t, db)
	release := sqlite.HoldACommit(t, db)
	allRan := sqlite.QueueDeparted(t, db, 3*sqlite.SessionGroupMax, "gone-", departedSlow)
	w.Pubrel(1)
	waitFor(t, "the release's write to queue", func() bool {
		return sqlite.QueuedAnswering(db, "dev")+sqlite.QueuedFor(db, "dev") == 1
	})
	if sqlite.QueuedAnswering(db, "dev") != 1 {
		t.Fatalf("the release's write queued among the departed (%d there), not among the answering",
			sqlite.QueuedFor(db, "dev"))
	}
	release()
	if rc := w.Pubcomp(1); rc != 0 {
		t.Fatalf("PUBCOMP carried 0x%02X, want success", rc)
	}
	allRan()
	got := ran()
	if n := strings.Count(strings.Join(got, " "), "dev"); n != 1 {
		t.Fatalf("the release is 1 write of dev's, and %d ran: %v", n, got)
	}
	if !ranBefore(got, "dev", 1, sqlite.SessionGroupMax) {
		t.Fatalf("the release's write ran behind departed clients' writes: %v", got)
	}
}
