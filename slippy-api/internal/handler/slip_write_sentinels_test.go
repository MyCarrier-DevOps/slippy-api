package handler

import (
	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// Compile-time anchor: ensure goLib sentinel name doesn't drift.
// If slippy.ErrTerminalAlreadyExists is renamed upstream, compile fails here
// BEFORE the mapWriteError 409 mapping silently regresses to 500.
var _ = slippy.ErrTerminalAlreadyExists
