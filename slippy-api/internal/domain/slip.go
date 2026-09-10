package domain

import (
	"context"
	"errors"
	"fmt"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// ErrCreationInProgress indicates a concurrent creation of the same repo:sha
// slip is in progress and the dedup lock-miss path could not observe the slip
// before its poll deadline. Callers should treat this as a duplicate/conflict
// (HTTP 409), not a server fault (HTTP 500).
var ErrCreationInProgress = errors.New("slip creation in progress")

// ErrWriteContended is the storage-agnostic signal that a slip write lost the per-slip
// write-lock race and did not complete (the transaction rolled back with nothing applied).
// The infrastructure adapter translates the backend-specific error (Postgres SQLSTATE
// 55P03) into this sentinel so the transport layer maps it (→ HTTP 503, retryable) without
// importing driver types.
var ErrWriteContended = errors.New("slip write contended on the per-slip lock")

// ErrStatementTimeout is the storage-agnostic signal that a slip write exceeded the
// database's server-side statement timeout (Postgres SQLSTATE 57014). Translated by the
// adapter so the transport layer maps it (→ HTTP 504) without importing driver types.
var ErrStatementTimeout = errors.New("slip write exceeded the database statement timeout")

// Slip is an alias for the upstream slippy.Slip type.
// This keeps domain consumers decoupled from direct import of the library package,
// while avoiding unnecessary type duplication (DRY).
type Slip = slippy.Slip

// SlipWithCommit pairs a slip with the commit SHA that matched it.
type SlipWithCommit = slippy.SlipWithCommit

// Write-related type aliases — decouple domain consumers from the library package.
type (
	StepStatus          = slippy.StepStatus
	StateHistoryEntry   = slippy.StateHistoryEntry
	AncestryEntry       = slippy.AncestryEntry
	PushOptions         = slippy.PushOptions
	CreateSlipResult    = slippy.CreateSlipResult
	ComponentDefinition = slippy.ComponentDefinition
)

// SlipReader defines the read-only interface for querying routing slips.
// This is the contract that handlers depend on — implementations include the
// ClickHouse store adapter and the caching decorator.
type SlipReader interface {
	// Load retrieves a slip by its correlation ID
	Load(ctx context.Context, correlationID string) (*Slip, error)

	// LoadByCommit retrieves a slip by repository and commit SHA
	LoadByCommit(ctx context.Context, repository, commitSHA string) (*Slip, error)

	// LoadByCommitExact returns the LIVE slip for the EXACT commitSHA, bypassing
	// ancestry resolution. Returns slippy.ErrSlipNotFound when no live slip exists for
	// this exact commit. Use for in-flight dedup-loser polling. For ancestry or
	// image-tag historical lookup, use LoadByCommit or FindByCommits.
	LoadByCommitExact(ctx context.Context, repository, commitSHA string) (*Slip, error)

	// FindByCommits finds the first matching slip for an ordered list of commits.
	// Returns the slip and the matched commit SHA.
	FindByCommits(ctx context.Context, repository string, commits []string) (*Slip, string, error)

	// FindAllByCommits finds all slips matching any commit in the ordered list.
	FindAllByCommits(ctx context.Context, repository string, commits []string) (FindAllResult, error)
}

// FindAllResult is the outcome of a FindAllByCommits lookup.
//
// Truncated reports one specific shortfall: the ancestry fallback hit its resolution cap
// before covering every requested commit, so Slips may omit slips the caller asked about. It
// is a field rather than an error because the partial answer is still worth returning — for
// the single-lineage caller the dropped tail is entirely redundant, since resolution walks
// backwards from each ref — but an operation that promises to find *all* matches must not
// return a short answer that reads as a complete one.
//
// What Truncated does NOT cover, and what `false` therefore does NOT guarantee: the direct
// store lookup matches commits by exact SHA and FindAllByCommits short-circuits the moment it
// returns any row, skipping ancestry for the rest of the batch. So a mixed-lineage batch —
// commit A with a direct slip, commit B reachable only through ancestry — returns just A's
// slip with Truncated=false. `false` means "the ancestry cap did not bite", not "every
// requested commit was exhaustively resolved". Callers must not read it as the latter: a
// consumer treating a missing commit as "no slip yet" could create a second slip for one that
// already has an ancestry-reachable one, the phantom-slip condition the dedup lock exists to
// prevent, by a path the lock cannot observe.
//
// Making `false` a true completeness guarantee would mean resolving ancestry for the commits
// the direct lookup did not match, rather than short-circuiting on the first hit — a
// behavioural change with fan-out implications, tracked as a follow-up.
type FindAllResult struct {
	Slips     []SlipWithCommit
	Truncated bool
}

// TruncatedSearchError qualifies a not-found produced without examining every commit the
// caller supplied.
//
// FindByCommits shares FindAllByCommits' 256-resolution cap, but has no result struct to
// carry a flag — it returns (slip, matchedCommit, error). A bare ErrSlipNotFound there
// renders as a plain 404, which reads as "no slip exists for any of these commits" when the
// truth is "none of the first N; the rest were never looked at". The distinction matters
// beyond tidiness: a consumer treating 404 as "no slip yet" may create a second slip for a
// commit that already has one — the phantom-slip condition the Redis dedup lock exists to
// prevent, reached by a path the lock cannot see, because the lock keys on repo:sha for the
// creating request and cannot know a lookup was silently shortened.
//
// It unwraps to slippy.ErrSlipNotFound, so every existing errors.Is check and the 404
// mapping keep working unchanged; only callers that ask for the detail see it.
type TruncatedSearchError struct {
	Resolved  int // commits actually examined
	Requested int // commits the caller supplied
}

func (e *TruncatedSearchError) Error() string {
	return fmt.Sprintf("no slip found in the first %d of %d commits", e.Resolved, e.Requested)
}

func (e *TruncatedSearchError) Unwrap() error { return slippy.ErrSlipNotFound }

// Invalidator is a post-write hook that removes cached entries for a slip.
// Implementations must treat failures as non-fatal and log rather than propagate.
type Invalidator interface {
	InvalidateByCorrelationID(ctx context.Context, correlationID string)
}

// SlipWriter defines the write interface for mutating routing slips.
// Methods map to business-level operations used by pushhookparser (slip creation)
// and Slippy CI CLI (pre-job/post-job step lifecycle).
type SlipWriter interface {
	// CreateSlipForPush creates a new routing slip for a git push event,
	// including ancestry resolution and ancestor abandonment/promotion.
	CreateSlipForPush(ctx context.Context, opts PushOptions) (*CreateSlipResult, error)

	// StartStep marks a pipeline step as running.
	StartStep(ctx context.Context, correlationID, stepName, componentName string) error

	// CompleteStep marks a pipeline step as completed.
	CompleteStep(ctx context.Context, correlationID, stepName, componentName string) error

	// FailStep marks a pipeline step as failed with a reason.
	FailStep(ctx context.Context, correlationID, stepName, componentName, reason string) error

	// SkipStep marks a pipeline step as skipped with an optional reason.
	SkipStep(ctx context.Context, correlationID, stepName, componentName, reason string) error

	// SetComponentImageTag records the built container image tag for a component.
	SetComponentImageTag(ctx context.Context, correlationID, componentName, imageTag string) error

	// PromoteSlip marks a slip as promoted to another branch via a PR merge.
	PromoteSlip(ctx context.Context, correlationID, promotedTo string) error

	// AbandonSlip marks a slip as abandoned, superseded by a newer push.
	AbandonSlip(ctx context.Context, correlationID, supersededBy string) error

	// ClaimSlip records that an adopter now has work in flight against an
	// existing slip: it appends an adoption marker to the slip's state history
	// and sets the slip's status to in_progress.
	//
	// Any caller that adopts a correlation ID it did not create must claim it
	// BEFORE dispatching work. An ended slip (failed, completed, abandoned,
	// promoted, compensated) stays repave-eligible, so a same-commit push in the
	// window between adoption and the adopter's first step write deletes the row
	// out from under the in-flight work and every later write 404s. Claiming
	// closes that window: in_progress is not repaveable, so the push dedups onto
	// the adopter's slip instead (DEVOPS-285).
	//
	// A nil result means the claim is committed. An error means the claim is NOT
	// CONFIRMED — it does NOT mean the slip is unclaimed: each of the two writes runs
	// on a cancellation-detached context (instrumentedWrite/writeContext), so a caller
	// that times out can see an error against a slip already set in_progress. A caller
	// that gets an error must still dispatch nothing; the recovery is to claim again.
	//
	// A repeat claim on an already-in_progress slip is therefore deliberately a NO-OP,
	// never a 409: re-claiming is the recovery, and a deterministic rejection would
	// burn a consumer's whole retry budget and DLQ the message. Do not add an
	// already-claimed rejection. The no-op de-duplicates SEQUENTIAL retries only — the
	// Load holds no lock across the writes — so this grants no exclusivity, and this
	// interface deliberately does not promise any.
	//
	// If an adopter cannot dispatch after a committed claim, the slip joins the accepted
	// zombie class documented at PostgresStore.LoadByCommit ("no timeout or escape
	// hatch ... operator-recoverable"); it is NOT self-healing for a same-commit push.
	// Do NOT "release" it with AbandonSlip: abandoned loses the `failed`
	// empty-run-guard carve-out (see emptyRunGuardApplies) and suppresses the next
	// push's unit tests. Escalate to an operator instead.
	//
	// Claiming also moves a `failed` slip out of consumer-side stranded-slip protection
	// that keys on `failed` (pushhookparser's AbandonStrandedSlip carve-out), so an
	// adopter is exposed to a concurrent force-push or branch delete for the life of
	// its run. Adopters' cleanup paths must learn to recognise a claimed slip.
	//
	// claimedBy names the adopter (it becomes the history entry's actor); reason
	// is optional free text describing the scope of the adopted work.
	ClaimSlip(ctx context.Context, correlationID, claimedBy, reason string) error
}
