package support

import (
	"context"
	"database/sql"
)

// RowQuerier is a connection to a database, held (*sql.Conn) or pooled
// (*sql.DB).
type RowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
