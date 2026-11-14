package store

import (
	"context"
	"database/sql"
)

// finishRows completes a read only after its rows close, preserving an earlier
// iteration error. Callers retain cleanup for early scan or decode errors.
func finishRows(rows *sql.Rows) error {
	err := rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	return err
}

// runReader owns conn for life. Each reader reports at most one failure to
// the writer, so failures has room even while the writer is busy in SQLite.
func (s *Store) runReader(conn *Conn) {
	var failed error
	read := func(op Read) {
		err := failed
		if failed == nil {
			err = s.read(conn, op)
			if err != nil && !SnapshotFailed(err) {
				failed = err
				s.failures <- Answer{Op: op, Err: failed}
			}
		} else if Classify(op) != RecordRequest {
			s.answers <- Answer{Op: op, View: true, Err: failed}
		}
		s.answers <- Answer{Op: op, Err: err}
	}
	defer func() {
		s.readersDone <- conn.close()
	}()
	for {
		select {
		case <-s.stop:
			return
		case op := <-s.reads:
			read(op)
		}
	}
}

// read establishes a WAL view before confirming it, then keeps that view
// through the callback. Raw transaction statements leave database/sql free
// to close a broken connection even from inside ReadConn.
func (s *Store) read(conn *Conn, op Read) (err error) {
	if observation, ok := op.(ObservationRead); ok && observation.Abandoned() {
		s.answers <- Answer{Op: op, View: true}
		return nil
	}
	if Classify(op) == RecordRequest {
		return op.ReadConn(conn)
	}
	if _, err = conn.ExecContext(context.Background(), `BEGIN`); err != nil {
		s.answers <- Answer{Op: op, View: true, Err: err}
		return err
	}
	defer func() {
		if _, endErr := conn.ExecContext(context.Background(), `COMMIT`); endErr != nil {
			// A failed connection ends the store, even if copying also failed.
			err = endErr
		}
	}()
	// A deferred BEGIN alone reads nothing. Touch the always-present
	// $broadcast row so later commits cannot enter this read's view.
	var id int64
	err = conn.Prepared(readView).QueryRowContext(context.Background()).Scan(&id)
	if observation, ok := op.(interface{ measureView(*Conn) error }); err == nil && ok {
		// External observations, such as database and WAL file sizes, must
		// finish at this cut before confirmation releases later writes.
		err = observation.measureView(conn)
	}
	s.answers <- Answer{Op: op, View: true, Err: err}
	if err != nil {
		return err
	}
	if observation, ok := op.(ObservationRead); ok && !observation.AwaitCapture() {
		return nil
	}
	return op.ReadConn(conn)
}
