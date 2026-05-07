package domain

import (
	"errors"
	"fmt"
)

// ErrStepAlreadyTerminal is returned by SlipWriter when an attempt is made to
// transition a step that is already in a terminal state to a different terminal
// state. Callers should treat this as a 409 Conflict.
//
// Wrap with context using StepAlreadyTerminalError to carry current/requested
// status detail.
var ErrStepAlreadyTerminal = errors.New("step already in terminal state")

// StepAlreadyTerminalError carries the detail for a terminal-overwrite attempt.
// It wraps ErrStepAlreadyTerminal so errors.Is checks still match.
type StepAlreadyTerminalError struct {
	StepName        string
	ComponentName   string
	CurrentStatus   StepStatus
	RequestedStatus StepStatus
}

func (e *StepAlreadyTerminalError) Error() string {
	target := e.StepName
	if e.ComponentName != "" {
		target = e.StepName + "/" + e.ComponentName
	}
	return fmt.Sprintf(
		"step %q is already in terminal state %q; refusing to overwrite with %q",
		target, e.CurrentStatus, e.RequestedStatus,
	)
}

func (e *StepAlreadyTerminalError) Unwrap() error { return ErrStepAlreadyTerminal }
