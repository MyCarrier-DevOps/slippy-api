package infrastructure

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
	"github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse/clickhousetest"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- Mock SlipReader for BuildInfoReader tests ---

type buildInfoMockReader struct {
	loadFn func(ctx context.Context, id string) (*domain.Slip, error)
}

func (m *buildInfoMockReader) Load(ctx context.Context, id string) (*domain.Slip, error) {
	return m.loadFn(ctx, id)
}

func (m *buildInfoMockReader) LoadByCommit(_ context.Context, _, _ string) (*domain.Slip, error) {
	return nil, errors.New("not implemented")
}

func (m *buildInfoMockReader) FindByCommits(_ context.Context, _ string, _ []string) (*domain.Slip, string, error) {
	return nil, "", errors.New("not implemented")
}

func (m *buildInfoMockReader) FindAllByCommits(_ context.Context, _ string, _ []string) ([]domain.SlipWithCommit, error) {
	return nil, errors.New("not implemented")
}

// --- Helper to build a test slip ---

func testSlip(correlationID, repository, branch, commitSHA string, createdAt time.Time) *domain.Slip {
	return &domain.Slip{
		CorrelationID: correlationID,
		Repository:    repository,
		Branch:        branch,
		CommitSHA:     commitSHA,
		CreatedAt:     createdAt,
	}
}

// --- computeSlipTag tests ---

func TestComputeSlipTag(t *testing.T) {
	tests := []struct {
		name     string
		slip     *domain.Slip
		expected string
	}{
		{
			name:     "nil slip returns empty",
			slip:     nil,
			expected: "",
		},
		{
			name: "empty commit SHA returns empty",
			slip: &domain.Slip{
				CommitSHA: "",
				CreatedAt: time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC),
			},
			expected: "",
		},
		{
			name: "normal case produces YY.WW.SHA7 format",
			slip: &domain.Slip{
				CommitSHA: "aef1234abcdef",
				CreatedAt: time.Date(2026, 2, 25, 12, 0, 0, 0, time.UTC), // 2026, week 9
			},
			expected: "26.09.aef1234",
		},
		{
			name: "short commit SHA (< 7 chars) used as-is",
			slip: &domain.Slip{
				CommitSHA: "abc",
				CreatedAt: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC), // 2026, week 2
			},
			expected: "26.02.abc",
		},
		{
			name: "exactly 7 char SHA",
			slip: &domain.Slip{
				CommitSHA: "1234567",
				CreatedAt: time.Date(2025, 12, 29, 0, 0, 0, 0, time.UTC), // ISO week 1 of 2026
			},
			expected: "26.01.1234567",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeSlipTag(tt.slip)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// --- parseOwnerRepo tests ---

func TestParseOwnerRepo(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantOrg   string
		wantRepo  string
		wantError bool
	}{
		{
			name:     "valid owner/repo",
			input:    "MyCarrier-Engineering/MC.Example",
			wantOrg:  "MyCarrier-Engineering",
			wantRepo: "MC.Example",
		},
		{
			name:     "valid DevOps owner/repo",
			input:    "MyCarrier-DevOps/AlertManagement",
			wantOrg:  "MyCarrier-DevOps",
			wantRepo: "AlertManagement",
		},
		{
			name:      "no slash",
			input:     "just-a-repo",
			wantError: true,
		},
		{
			name:      "empty owner",
			input:     "/repo",
			wantError: true,
		},
		{
			name:      "empty repo",
			input:     "owner/",
			wantError: true,
		},
		{
			name:      "empty string",
			input:     "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			org, repo, err := parseOwnerRepo(tt.input)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantOrg, org)
				assert.Equal(t, tt.wantRepo, repo)
			}
		})
	}
}

// --- BuildInfoReader.ResolveImageTags tests ---

func TestResolveImageTags_BuildScopeAll(t *testing.T) {
	// Slip: created 2026-02-25 (week 9), commit aef1234...
	slip := testSlip("corr-001", "MyCarrier-Engineering/MC.Example", "main", "aef1234abcdef",
		time.Date(2026, 2, 25, 12, 0, 0, 0, time.UTC))

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			assert.Equal(t, "corr-001", id)
			return slip, nil
		},
	}

	callCount := 0
	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			callCount++
			switch {
			case strings.Contains(query, "ci.repoproperties"):
				// Return build_scope = "all"
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"all"}},
				}, nil
			case strings.Contains(query, "ci.buildinfo"):
				// Return two components
				return &clickhousetest.MockRows{
					NextData: []bool{true, true},
					ScanData: [][]any{
						{"api", "26.09.aef1234"},
						{"worker", "26.09.aef1234"},
					},
				}, nil
			default:
				t.Fatalf("unexpected query: %s", query)
				return nil, nil
			}
		},
	}

	bir := NewBuildInfoReader(session, reader)
	result, err := bir.ResolveImageTags(context.Background(), "corr-001")
	require.NoError(t, err)

	assert.Equal(t, domain.BuildScopeAll, result.BuildScope)
	assert.Equal(t, "26.09.aef1234", result.SlipTag)
	assert.Equal(t, 2, len(result.Tags))
	// In build_scope=all, all components get the slip tag.
	assert.Equal(t, "26.09.aef1234", result.Tags["api"])
	assert.Equal(t, "26.09.aef1234", result.Tags["worker"])
	assert.Equal(t, 2, callCount, "expected exactly 2 ClickHouse queries (repoproperties + buildinfo)")
}

func TestResolveImageTags_BuildScopeModified(t *testing.T) {
	slip := testSlip("corr-002", "MyCarrier-DevOps/AlertManagement", "feature1", "b5678efabc",
		time.Date(2026, 2, 25, 12, 0, 0, 0, time.UTC))

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			switch {
			case strings.Contains(query, "ci.repoproperties"):
				// Verify parameterized query: repoName, org
				assert.Equal(t, "AlertManagement", args[0])
				assert.Equal(t, "MyCarrier-DevOps", args[1])
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"modified"}},
				}, nil
			case strings.Contains(query, "ci.buildinfo"):
				assert.Equal(t, "corr-002", args[0])
				// Different tags per component (modified scenario)
				return &clickhousetest.MockRows{
					NextData: []bool{true, true},
					ScanData: [][]any{
						{"my_component", "26.09.aef1234"},
						{"my_other_component", "26.03.a4241ce"},
					},
				}, nil
			default:
				t.Fatalf("unexpected query: %s", query)
				return nil, nil
			}
		},
	}

	bir := NewBuildInfoReader(session, reader)
	result, err := bir.ResolveImageTags(context.Background(), "corr-002")
	require.NoError(t, err)

	assert.Equal(t, domain.BuildScopeModified, result.BuildScope)
	assert.Equal(t, "26.09.b5678ef", result.SlipTag)
	assert.Equal(t, 2, len(result.Tags))
	// In build_scope=modified, each component carries its actual tag.
	assert.Equal(t, "26.09.aef1234", result.Tags["my_component"])
	assert.Equal(t, "26.03.a4241ce", result.Tags["my_other_component"])
}

func TestResolveImageTags_SlipNotFound(t *testing.T) {
	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return nil, errors.New("slip not found")
		},
	}

	session := &clickhousetest.MockSession{}
	bir := NewBuildInfoReader(session, reader)

	result, err := bir.ResolveImageTags(context.Background(), "no-such-id")
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "slip not found")
}

func TestResolveImageTags_InvalidRepository(t *testing.T) {
	slip := testSlip("corr-bad", "invalid-repo-no-slash", "main", "abc1234",
		time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC))

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	session := &clickhousetest.MockSession{}
	bir := NewBuildInfoReader(session, reader)

	result, err := bir.ResolveImageTags(context.Background(), "corr-bad")
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid repository format")
}

func TestResolveImageTags_RepoPropertiesQueryFails_DefaultsToAll(t *testing.T) {
	slip := testSlip("corr-003", "MyCarrier-DevOps/SomeRepo", "main", "def4567abc",
		time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC))

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			switch {
			case strings.Contains(query, "ci.repoproperties"):
				return nil, errors.New("connection timeout")
			case strings.Contains(query, "ci.buildinfo"):
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"api", "26.09.def4567"}},
				}, nil
			default:
				return nil, errors.New("unexpected query")
			}
		},
	}

	bir := NewBuildInfoReader(session, reader)
	result, err := bir.ResolveImageTags(context.Background(), "corr-003")
	require.NoError(t, err)

	// Should default to "all" and use slip tag for all components.
	assert.Equal(t, domain.BuildScopeAll, result.BuildScope)
	assert.Equal(t, "26.09.def4567", result.SlipTag)
	assert.Equal(t, "26.09.def4567", result.Tags["api"])
}

func TestResolveImageTags_NoBuildInfoRows(t *testing.T) {
	slip := testSlip("corr-004", "MyCarrier-Engineering/MC.Empty", "main", "aaa1111bbb",
		time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC))

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			switch {
			case strings.Contains(query, "ci.repoproperties"):
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"all"}},
				}, nil
			case strings.Contains(query, "ci.buildinfo"):
				// No rows
				return &clickhousetest.MockRows{
					NextData: []bool{},
				}, nil
			default:
				return nil, errors.New("unexpected query")
			}
		},
	}

	bir := NewBuildInfoReader(session, reader)
	result, err := bir.ResolveImageTags(context.Background(), "corr-004")
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "no ci.buildinfo rows found")
}

func TestResolveImageTags_BuildInfoQueryError(t *testing.T) {
	slip := testSlip("corr-005", "MyCarrier-Engineering/MC.Fail", "main", "bbb2222ccc",
		time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC))

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			switch {
			case strings.Contains(query, "ci.repoproperties"):
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"all"}},
				}, nil
			case strings.Contains(query, "ci.buildinfo"):
				return nil, errors.New("clickhouse connection lost")
			default:
				return nil, errors.New("unexpected query")
			}
		},
	}

	bir := NewBuildInfoReader(session, reader)
	result, err := bir.ResolveImageTags(context.Background(), "corr-005")
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to query ci.buildinfo")
}

func TestResolveImageTags_UnknownBuildScope_DefaultsToAll(t *testing.T) {
	slip := testSlip("corr-006", "MyCarrier-Engineering/MC.Weird", "main", "ccc3333ddd",
		time.Date(2026, 1, 19, 0, 0, 0, 0, time.UTC)) // week 4

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			switch {
			case strings.Contains(query, "ci.repoproperties"):
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"unknown_value"}},
				}, nil
			case strings.Contains(query, "ci.buildinfo"):
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"svc", "26.04.ccc3333"}},
				}, nil
			default:
				return nil, errors.New("unexpected query")
			}
		},
	}

	bir := NewBuildInfoReader(session, reader)
	result, err := bir.ResolveImageTags(context.Background(), "corr-006")
	require.NoError(t, err)

	// Unknown build_scope defaults to "all".
	assert.Equal(t, domain.BuildScopeAll, result.BuildScope)
	assert.Equal(t, "26.04.ccc3333", result.SlipTag)
	assert.Equal(t, "26.04.ccc3333", result.Tags["svc"])
}

func TestResolveImageTags_NoRepoPropertiesRow_DefaultsToAll(t *testing.T) {
	slip := testSlip("corr-007", "MyCarrier-Engineering/MC.New", "main", "ddd4444eee",
		time.Date(2026, 2, 25, 0, 0, 0, 0, time.UTC))

	reader := &buildInfoMockReader{
		loadFn: func(_ context.Context, _ string) (*domain.Slip, error) {
			return slip, nil
		},
	}

	session := &clickhousetest.MockSession{
		QueryWithArgsFunc: func(_ context.Context, query string, args ...any) (ch.Rows, error) {
			switch {
			case strings.Contains(query, "ci.repoproperties"):
				// No rows — new repo, not yet in repoproperties
				return &clickhousetest.MockRows{
					NextData: []bool{},
				}, nil
			case strings.Contains(query, "ci.buildinfo"):
				return &clickhousetest.MockRows{
					NextData: []bool{true},
					ScanData: [][]any{{"api", "26.09.ddd4444"}},
				}, nil
			default:
				return nil, errors.New("unexpected query")
			}
		},
	}

	bir := NewBuildInfoReader(session, reader)
	result, err := bir.ResolveImageTags(context.Background(), "corr-007")
	require.NoError(t, err)

	assert.Equal(t, domain.BuildScopeAll, result.BuildScope)
	assert.Equal(t, "26.09.ddd4444", result.SlipTag)
	assert.Equal(t, "26.09.ddd4444", result.Tags["api"])
}
