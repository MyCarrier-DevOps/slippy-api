package domain

import (
	"context"
	"errors"
	"time"
)

// ErrInvalidCursor indicates the cursor parameter could not be parsed.
var ErrInvalidCursor = errors.New("invalid cursor")

// SortOrder represents the direction of log timestamp sorting.
type SortOrder string

const (
	SortDesc SortOrder = "desc"
	SortAsc  SortOrder = "asc"
)

// CIJobLog represents a single CI job log entry from observability.ciJob.
type CIJobLog struct {
	Timestamp       time.Time `json:"timestamp"`
	Level           string    `json:"level"`
	Service         string    `json:"service"`
	Component       string    `json:"component"`
	Cluster         string    `json:"cluster"`
	Cloud           string    `json:"cloud"`
	Environment     string    `json:"environment"`
	Namespace       string    `json:"namespace"`
	Message         string    `json:"message"`
	CIJobInstance   string    `json:"ci_job_instance"`
	CIJobType       string    `json:"ci_job_type"`
	BuildRepository string    `json:"build_repository"`
	BuildImage      string    `json:"build_image"`
	BuildBranch     string    `json:"build_branch"`
}

// CIJobLogQuery defines parameters for querying CI job logs.
type CIJobLogQuery struct {
	CorrelationID string
	Limit         int
	Cursor        string // RFC3339Nano timestamp from previous page
	Sort          SortOrder

	// Column filters (exact match, applied when non-empty)
	Level           string
	Service         string
	Component       string
	Cluster         string
	Cloud           string
	Environment     string
	Namespace       string
	Message         string
	CIJobInstance   string
	CIJobType       string
	BuildRepository string
	BuildImage      string
	BuildBranch     string
}

// CIJobLogResult contains query results with pagination metadata.
type CIJobLogResult struct {
	Logs       []CIJobLog `json:"logs"`
	NextCursor string     `json:"next_cursor,omitempty"`
	Count      int        `json:"count"`
}

// CIJobLogReader queries CI job logs from the observability store.
type CIJobLogReader interface {
	QueryLogs(ctx context.Context, query *CIJobLogQuery) (*CIJobLogResult, error)
}
