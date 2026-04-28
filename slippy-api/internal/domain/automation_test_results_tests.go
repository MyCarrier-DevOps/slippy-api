package domain

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrInvalidTestsCursor indicates the cursor parameter could not be parsed.
var ErrInvalidTestsCursor = errors.New("invalid tests cursor")

// ErrTestNotFound indicates that no row exists in TestResultsCor for the
// given (TestId, scope) pair.
var ErrTestNotFound = errors.New("test not found")

// AutomationTestResult is one row from autotest_results.TestResultsCor — the
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
	CorrelationID           *string   `json:"correlation_id,omitempty"`
}

// AutomationTestsByCorrelationQuery defines parameters for fetching tests
// whose CorrelationId equals the given UUID, with optional filters and
// cursor pagination.
type AutomationTestsByCorrelationQuery struct {
	CorrelationID uuid.UUID
	Environment   string
	Stack         string
	Stage         string
	Attempt       uint8  // 0 = no attempt filter
	Status        string // empty = no status filter
	Limit         int
	Cursor        string // "RFC3339Nano|UUID" from a previous page
}

// AutomationTestsResult contains query results with pagination metadata.
type AutomationTestsResult struct {
	Tests      []AutomationTestResult `json:"tests"`
	NextCursor string                 `json:"next_cursor,omitempty"`
	Count      int                    `json:"count"`
}

// LoadTestByCorrelationQuery defines parameters for fetching a single
// TestResultsCor row by its TestId, scoped to a CorrelationId so a TestId
// from an unrelated slip can't be returned.
type LoadTestByCorrelationQuery struct {
	CorrelationID uuid.UUID
	TestID        uuid.UUID
}

// AutomationTestsReader queries individual test rows from
// autotest_results.TestResultsCor.
type AutomationTestsReader interface {
	QueryTestsByCorrelation(
		ctx context.Context,
		q *AutomationTestsByCorrelationQuery,
	) (*AutomationTestsResult, error)

	LoadTestByCorrelation(
		ctx context.Context,
		q *LoadTestByCorrelationQuery,
	) (*AutomationTestResult, error)
}
