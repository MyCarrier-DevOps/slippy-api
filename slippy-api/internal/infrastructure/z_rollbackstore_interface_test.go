package infrastructure

import (
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// Compile-time check: run() passes *slippy.PostgresStore straight to the rollback routes as a
// domain.RollbackStore. If either side drifts, this line fails to compile.
var _ domain.RollbackStore = (*slippy.PostgresStore)(nil)
