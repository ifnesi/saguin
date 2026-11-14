package conformance

import (
	"database/sql"
	"syscall"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
)

// design.md, Execution, Ready work 1: "Each change that allows work to
// continue schedules it immediately: applying a store answer, settling a
// delivery, returning capacity or ending a request"; 4: "no timer or
// periodic sweep stands in for scheduling". SUBSCRIBE, A shared group:
// "Already-started exchanges retain only the state required to finish MQTT."
//
// Hold the last PUBCOMP's write until a graceful stop has begun: Restart
// says "Once the stop begins, the core's timer starts no housekeeping" and
// "The core waits until every store request it sent, read or write, is
// answered." PINGRESP proves the PUBCOMP was taken; DISCONNECT 0x8B proves
// the stop began before the write can commit. Thus a retry sweep cannot
// hide missing ready work, however slowly the test runs. Exit is the
// completion condition; its watchdog is not a cleanup deadline.
func TestShareLastExchangeEndsGroupWithoutRetry(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		name := "live"
		if recovered {
			name = "recovered"
		}
		t.Run(name, func(t *testing.T) {
			b := support.Launch(t, &harness.Options{})
			filter := shareOf("g", "finish/#")
			connect := &harness.Connect{ClientID: "member", CleanStart: true, Props: support.Durable}
			member := support.Subscribed(t, b.Addr, harness.V5, connect, support.At(2, filter))
			pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "publisher", CleanStart: true})
			p := twice("finish/x", "last")
			published(t, pub, p)
			id := support.DeliveredAt(t, member, 2, p.Topic, p.Payload).ID
			released(t, member, id)
			unsuback(t, member, nil, []string{filter}, 0x00)
			if recovered {
				support.Away(t, member)
				member = resumed(t, b.Addr, harness.V5, connect)
				expectPubrel(t, member, id, 0x00, "resuming the last exchange after leaving its group")
			}
			support.Away(t, pub)

			db := support.OpenDB(t, b.DB)
			remaining := func() (n [4]int) {
				support.Query(t, db, `SELECT (SELECT count(*) FROM share_groups),
					(SELECT count(*) FROM share_cursors),
					(SELECT count(*) FROM share_returned),
					(SELECT count(*) FROM session_inflight WHERE share > 0)`,
					func(rows *sql.Rows) error { return rows.Scan(&n[0], &n[1], &n[2], &n[3]) })
				return n
			}
			if got := remaining(); got[0] != 1 || got[1] != 0 || got[3] != 1 {
				t.Fatalf("before PUBCOMP: groups, cursors, evidence, exchanges = %v; want one group and one exchange, with no cursor after UNSUBSCRIBE", got)
			}

			lock := holdWriteLock(t, b.DB)
			pubcomp(t, member, id)
			taken(t, member, "the last PUBCOMP, its write held")
			if err := b.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			shutDown(t, member)
			lock.release(t)
			if err := member.Close(); err != nil {
				t.Fatal(err)
			}
			if state := support.Exited(t, b); !state.Success() {
				t.Fatalf("the graceful stop ended %v; want exit status 0", state)
			}
			if got := remaining(); got != [4]int{} {
				t.Fatalf("after the last PUBCOMP committed with retry sweeps disabled: groups, cursors, evidence, exchanges = %v; want [0 0 0 0] (Ready work 1 and 4)", got)
			}
		})
	}
}
