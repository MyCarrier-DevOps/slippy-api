package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
	"github.com/MyCarrier-DevOps/slippy-api/internal/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// --- Handler Tracing Tests ---

func TestGetSlip_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	expected := &domain.Slip{CorrelationID: "abc-123"}
	reader := &mockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return expected, nil
		},
	}

	h := NewSlipHandler(reader)
	out, err := h.getSlip(context.Background(), &GetSlipInput{CorrelationID: "abc-123"})
	require.NoError(t, err)
	assert.Equal(t, expected, out.Body)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "handler.getSlip", span.Name())
	assert.Equal(t, codes.Ok, span.Status().Code)
	assertHandlerAttr(t, span.Attributes(), "slip.correlation_id", "abc-123")
}

func TestGetSlip_Error_RecordsSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	reader := &mockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	h := NewSlipHandler(reader)
	_, err := h.getSlip(context.Background(), &GetSlipInput{CorrelationID: "missing"})
	require.Error(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	require.NotEmpty(t, spans[0].Events(), "error should be recorded as event")
}

func TestGetSlipByCommit_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	reader := &mockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return &domain.Slip{CorrelationID: "def-456"}, nil
		},
	}

	h := NewSlipHandler(reader)
	out, err := h.getSlipByCommit(context.Background(), &GetSlipByCommitInput{
		Owner: "org", Repo: "repo", CommitSHA: "sha123",
	})
	require.NoError(t, err)
	assert.Equal(t, "def-456", out.Body.CorrelationID)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "handler.getSlipByCommit", span.Name())
	assertHandlerAttr(t, span.Attributes(), "slip.repository", "org/repo")
	assertHandlerAttr(t, span.Attributes(), "slip.commit_sha", "sha123")
}

func TestFindByCommits_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	reader := &mockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return &domain.Slip{CorrelationID: "ghi-789"}, "c1", nil
		},
	}

	h := NewSlipHandler(reader)
	input := &FindByCommitsInput{}
	input.Body.Repository = "org/repo"
	input.Body.Commits = []string{"c1", "c2"}
	out, err := h.findByCommits(context.Background(), input)
	require.NoError(t, err)
	assert.Equal(t, "c1", out.Body.MatchedCommit)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "handler.findByCommits", span.Name())
	assertHandlerAttr(t, span.Attributes(), "slip.matched_commit", "c1")
	assertHandlerIntAttr(t, span.Attributes(), "slip.commits_count", 2)
}

func TestFindAllByCommits_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	reader := &mockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return []domain.SlipWithCommit{
				{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
				{Slip: &domain.Slip{CorrelationID: "b"}, MatchedCommit: "c2"},
			}, nil
		},
	}

	h := NewSlipHandler(reader)
	input := &FindByCommitsInput{}
	input.Body.Repository = "org/repo"
	input.Body.Commits = []string{"c1", "c2"}
	out, err := h.findAllByCommits(context.Background(), input)
	require.NoError(t, err)
	assert.Len(t, out.Body, 2)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "handler.findAllByCommits", span.Name())
	assertHandlerIntAttr(t, span.Attributes(), "slip.results_count", 2)
}

// --- Assertion helpers ---

func assertHandlerAttr(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.Equal(t, want, a.Value.AsString(), "attribute %q", key)
			return
		}
	}
	t.Errorf("attribute %q not found in span", key)
}

func assertHandlerIntAttr(t *testing.T, attrs []attribute.KeyValue, key string, want int) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.Equal(t, int64(want), a.Value.AsInt64(), "attribute %q", key)
			return
		}
	}
	t.Errorf("attribute %q not found in span", key)
}
