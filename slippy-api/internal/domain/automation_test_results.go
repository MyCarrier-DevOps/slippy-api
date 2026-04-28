package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// AutomationTestRunResult is one row from autotest_results.RunResults — a single
// test-suite run for a given (Environment, Stack, Stage, Attempt) tuple.
type AutomationTestRunResult struct {
	Outcome           string    `json:"outcome"`
	Passed            uint32    `json:"passed"`
	Failed            uint32    `json:"failed"`
	StartTime         time.Time `json:"start_time"`
	FinishTime        time.Time `json:"finish_time"`
	ReleaseID         string    `json:"release_id"`
	Attempt           uint32    `json:"attempt"`
	Stage             string    `json:"stage"`
	EnvironmentName   string    `json:"environment_name"`
	StackName         string    `json:"stack_name"`
	ErrorMessage      string    `json:"error_message,omitempty"`
	BranchName        string    `json:"branch_name,omitempty"`
	AttemptID         string    `json:"attempt_id,omitempty"`
	TestRunID         *string   `json:"test_run_id,omitempty"`
	CorrelationID     *string   `json:"correlation_id,omitempty"`
	JobNumber         string    `json:"job_number,omitempty"`
	BatchID           *string   `json:"batch_id,omitempty"`
	TotalTestJobCount uint32    `json:"total_test_job_count"`
}

// AutomationTestResultsQuery defines parameters for querying RunResults rows
// by correlation ID with optional environment/stack/stage/attempt filters.
// When Attempt is 0, results are collapsed to the latest attempt per
// (EnvironmentName, StackName, Stage) tuple.
type AutomationTestResultsQuery struct {
	CorrelationID uuid.UUID
	Environment   string
	Stack         string
	Stage         string
	Attempt       uint32 // 0 == fetch latest
}

// AutomationTestResultsByReleaseQuery defines parameters for querying
// RunResults rows whose ReleaseId contains the given substring (ILIKE %x%).
// When Attempt is 0, results are collapsed to the latest attempt per
// (ReleaseId, EnvironmentName, StackName, Stage) tuple so each matched
// release retains its own latest attempt.
type AutomationTestResultsByReleaseQuery struct {
	ReleaseIDSubstring string
	Environment        string
	Stack              string
	Stage              string
	Attempt            uint32 // 0 == fetch latest
}

// AutomationTestResultsResult contains query results.
type AutomationTestResultsResult struct {
	Runs  []AutomationTestRunResult `json:"runs"`
	Count int                       `json:"count"`
}

// AutomationTestResultsReader queries the autotest_results.RunResults table.
type AutomationTestResultsReader interface {
	QueryAutomationTestResults(
		ctx context.Context,
		q *AutomationTestResultsQuery,
	) (*AutomationTestResultsResult, error)

	QueryAutomationTestResultsByRelease(
		ctx context.Context,
		q *AutomationTestResultsByReleaseQuery,
	) (*AutomationTestResultsResult, error)
}
