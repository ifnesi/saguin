package broker

import (
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt"
)

// retryAway runs on the broker's loop once a second, so it must not wait for
// the session lock of a session a connection owns: that lock is held across
// a CONNECT's store wait, and the loop's queue offers, sweeps and position
// flush would stand behind it (invariant 17 still decides, under the lock,
// for an id that is not owned).
func TestRetryAwayDoesNotWaitOnAConnectedSessionsLock(t *testing.T) {
	b := metricsBroker(t)
	b.SetServer(mqtt.New(&mqtt.Options{}))
	d := &bdrain{b: b}
	const id = "connected"
	o := &owed{d: d, client: id, dirty: true, acked: nil}
	d.sessions.Store(id, o)
	b.mu.Lock()
	b.owner[id] = &mqtt.Client{ID: id}
	b.mu.Unlock()

	unlock := b.lockSession(id) // a CONNECT for the id waiting on the store
	returned := make(chan int, 1)
	go func() {
		refused, _ := d.retryAway()
		returned <- refused
	}()
	select {
	case refused := <-returned:
		if refused != 0 {
			t.Errorf("retryAway wrote %d of a connected session's acknowledgements", refused)
		}
	case <-time.After(2 * time.Second):
		unlock()
		<-returned
		t.Fatal("retryAway waited on the session lock of an id a connection owns")
	}
	unlock()
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.dirty {
		t.Error("retryAway took a connected session's cursor; its own drain writes it")
	}
}
