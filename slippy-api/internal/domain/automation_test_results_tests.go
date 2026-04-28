package domain

import (
	"context"
	"errors"
	"time"
)

// ErrInvalidTestsCursor indicates the cursor parameter could not be parsed.
var ErrInvalidTestsCursor = errors.New("invalid tests cursor")

// AutomationTestResult is one row from autotest_results.TestResults — the
// outcome of a single test scenario within a test-suite run.
type AutomationTestResult struct {
	Feature                 string    `json:"feature"`
	TestName                string    `json:"test_name"`
	ResultMessage           string    `json:"result_message,omitempty"`
	ResultStatus            string    `json:"result_status"`
	Duration                float64   `json:"duration"`
	Description             string    `json:"description,omitempty"`
	ScenarioInfoTitle       string    `json:"scenario_info_title,omitempty"`
	ScenarioInfoDescription string    `json:"scenario_info_description,omitempty"`
	ScenarioInfoTags        []string  `json:"scenario_info_tags,omitempty"`
	ScenarioExecutionStatus string    `json:"scenario_execution_status,omitempty"`
	StackTrace              string    `json:"stack_trace,omitempty"`
	ReleaseID               string    `json:"release_id"`
	StackName               string    `json:"stack_name"`
	Stage                   string    `json:"stage"`
	EnvironmentName         string    `json:"environment_name"`
	Attempt                 uint8     `json:"attempt"`
	StartTime               time.Time `json:"start_time"`
	BranchName              string    `json:"branch_name,omitempty"`
	TestID                  string    `json:"test_id"`
}

// ResolvedRunKey is the 5-tuple that joins TestResults rows back to their
// parent RunResults row (TestResults has no CorrelationId).
type ResolvedRunKey struct {
	ReleaseID       string
	Attempt         uint8
	Stage           string
	EnvironmentName string
	StackName       string
}

// AutomationTestsQuery defines parameters for fetching individual test rows
// from autotest_results.TestResults. Runs is the set of resolved RunResults
// rows (typically obtained by calling AutomationTestResultsReader first);
// MinStart and MaxFinish bound the StartTime predicate that lets ClickHouse
// prune partitions. An empty Status disables the result-status filter.
type AutomationTestsQuery struct {
	Runs      []ResolvedRunKey
	MinStart  time.Time
	MaxFinish time.Time
	Status    string
	Limit     int
	Cursor    string // "RFC3339Nano|UUID" from a previous page
}

// AutomationTestsResult contains query results with pagination metadata.
type AutomationTestsResult struct {
	Tests      []AutomationTestResult `json:"tests"`
	NextCursor string                 `json:"next_cursor,omitempty"`
	Count      int                    `json:"count"`
}

// AutomationTestsReader queries individual test results from the
// autotest_results.TestResults table.
type AutomationTestsReader interface {
	QueryTests(ctx context.Context, q *AutomationTestsQuery) (*AutomationTestsResult, error)
}
