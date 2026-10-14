package domain

import (
	"context"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// RollbackRecord is an alias for the upstream slippy.RollbackRecord: one gitops-rollback
// attempt for a slip (DEVOPS-320).
type RollbackRecord = slippy.RollbackRecord

// RollbackStatus is an alias for the upstream slippy.RollbackStatus.
type RollbackStatus = slippy.RollbackStatus

// RollbackStore records and lists a slip's rollback attempts. run() wires the Postgres slip
// store in directly, since *slippy.PostgresStore implements it, so no adapter sits between.
type RollbackStore interface {
	// RecordRollback writes one attempt and reports whether this call wrote it; false means a
	// record for the same workflow already existed. A missing slip is slippy.ErrSlipNotFound and
	// an unwritable record is slippy.ErrInvalidRollback.
	RecordRollback(ctx context.Context, rec RollbackRecord) (bool, error)

	// ListRollbacks returns the slip's attempts in recorded order; empty, not an error, when
	// there are none.
	ListRollbacks(ctx context.Context, correlationID string) ([]RollbackRecord, error)
}
