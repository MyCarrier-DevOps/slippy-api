package infrastructure

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
	"go.opentelemetry.io/otel/trace"
)

// --- Store Adapter Tracing Tests ---

func TestSlipStoreAdapter_Load_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	expected := &domain.Slip{CorrelationID: "abc-123"}
	store := &mockSlipStore{
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			return expected, nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	_, err := adapter.Load(context.Background(), "abc-123")
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1, "expected exactly one span")

	span := spans[0]
	assert.Equal(t, "clickhouse.Load", span.Name())
	assert.Equal(t, trace.SpanKindClient, span.SpanKind())
	assertAttr(t, span.Attributes(), "db.system", "clickhouse")
	assertAttr(t, span.Attributes(), "db.operation", "Load")
	assertAttr(t, span.Attributes(), "slip.correlation_id", "abc-123")
}

func TestSlipStoreAdapter_Load_ErrorRecordsSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	store := &mockSlipStore{
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	adapter := NewSlipStoreAdapter(store)
	_, err := adapter.Load(context.Background(), "missing")
	require.Error(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "clickhouse.Load", span.Name())
	// Not-found is a client error — span status should be Unset, not Error.
	assert.Equal(t, codes.Unset, span.Status().Code)
	// The error should be recorded as an event.
	require.NotEmpty(t, span.Events())
}

func TestSlipStoreAdapter_LoadByCommit_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	store := &mockSlipStore{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*slippy.Slip, error) {
			return &domain.Slip{CorrelationID: "def-456"}, nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	_, err := adapter.LoadByCommit(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "clickhouse.LoadByCommit", span.Name())
	assert.Equal(t, trace.SpanKindClient, span.SpanKind())
	assertAttr(t, span.Attributes(), "slip.repository", "org/repo")
	assertAttr(t, span.Attributes(), "slip.commit_sha", "sha123")
}

func TestSlipStoreAdapter_FindByCommits_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	store := &mockSlipStore{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*slippy.Slip, string, error) {
			return &domain.Slip{CorrelationID: "ghi-789"}, "c1", nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	_, matched, err := adapter.FindByCommits(context.Background(), "org/repo", []string{"c1", "c2"})
	require.NoError(t, err)
	assert.Equal(t, "c1", matched)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "clickhouse.FindByCommits", span.Name())
	assertAttr(t, span.Attributes(), "slip.repository", "org/repo")
	assertIntAttr(t, span.Attributes(), "slip.commits_count", 2)
	assertAttr(t, span.Attributes(), "slip.matched_commit", "c1")
}

func TestSlipStoreAdapter_FindAllByCommits_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	store := &mockSlipStore{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]slippy.SlipWithCommit, error) {
			return []slippy.SlipWithCommit{
				{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
			}, nil
		},
	}

	adapter := NewSlipStoreAdapter(store)
	results, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	require.NoError(t, err)
	assert.Len(t, results, 1)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "clickhouse.FindAllByCommits", span.Name())
	assertIntAttr(t, span.Attributes(), "slip.results_count", 1)
}

func TestSlipStoreAdapter_FindAllByCommits_ServerError_SetsErrorStatus(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	store := &mockSlipStore{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]slippy.SlipWithCommit, error) {
			return nil, assert.AnError
		},
	}

	adapter := NewSlipStoreAdapter(store)
	_, err := adapter.FindAllByCommits(context.Background(), "org/repo", []string{"c1"})
	require.Error(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	// Non-client errors should set the span status to Error.
	assert.Equal(t, codes.Error, span.Status().Code)
	require.NotEmpty(t, span.Events())
}

// --- Cache Layer Tracing Tests ---

func TestCachedSlipReader_Load_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	mock := &mockSlipReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return &domain.Slip{CorrelationID: "abc-123"}, nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 0)
	_, err := cached.Load(context.Background(), "abc-123")
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "cache.Load", span.Name())
	assertAttr(t, span.Attributes(), "cache.system", "dragonfly")
	assertAttr(t, span.Attributes(), "cache.result", "passthrough")
}

func TestCachedSlipReader_LoadByCommit_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	mock := &mockSlipReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return &domain.Slip{CorrelationID: "def-456"}, nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 0)
	_, err := cached.LoadByCommit(context.Background(), "org/repo", "sha123")
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "cache.LoadByCommit", spans[0].Name())
}

func TestCachedSlipReader_FindByCommits_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	mock := &mockSlipReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return &domain.Slip{CorrelationID: "ghi-789"}, "c1", nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 0)
	_, _, err := cached.FindByCommits(context.Background(), "org/repo", []string{"c1"})
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "cache.FindByCommits", spans[0].Name())
	assertIntAttr(t, spans[0].Attributes(), "slip.commits_count", 1)
}

func TestCachedSlipReader_FindAllByCommits_CreatesSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	mock := &mockSlipReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return []domain.SlipWithCommit{}, nil
		},
	}

	cached := NewCachedSlipReader(mock, nil, 0)
	_, err := cached.FindAllByCommits(context.Background(), "org/repo", []string{"c1", "c2"})
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "cache.FindAllByCommits", spans[0].Name())
	assertIntAttr(t, spans[0].Attributes(), "slip.commits_count", 2)
}

func TestCachedSlipReader_Load_ErrorRecordsSpan(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	mock := &mockSlipReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return nil, assert.AnError
		},
	}

	cached := NewCachedSlipReader(mock, nil, 0)
	_, err := cached.Load(context.Background(), "abc-123")
	require.Error(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status().Code)
	require.NotEmpty(t, spans[0].Events())
}

// --- Span child-parent waterfall test ---

func TestSpan_Waterfall_CacheCallsStore(t *testing.T) {
	recorder, cleanup := telemetry.SetupTestTracing()
	defer cleanup()

	// Build a store adapter that returns a slip.
	store := &mockSlipStore{
		loadFn: func(_ context.Context, id string) (*slippy.Slip, error) {
			return &domain.Slip{CorrelationID: "abc-123"}, nil
		},
	}
	adapter := NewSlipStoreAdapter(store)

	// Wrap with cache layer.
	cached := NewCachedSlipReader(adapter, nil, 0)
	_, err := cached.Load(context.Background(), "abc-123")
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 2, "expected a cache span and a store span")

	// The store span should be a child of the cache span.
	// The first span to end is the inner (store) span.
	storeSpan := spans[0]
	cacheSpan := spans[1]
	assert.Equal(t, "clickhouse.Load", storeSpan.Name())
	assert.Equal(t, "cache.Load", cacheSpan.Name())

	// Verify parent-child relationship — the store span's parent should be the cache span.
	assert.Equal(t, cacheSpan.SpanContext().SpanID(), storeSpan.Parent().SpanID(),
		"store span should be a child of the cache span")
}

// --- Assertion helpers ---

// assertAttr asserts that the given attribute key has the expected string value
// in the span's attribute set.
func assertAttr(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.Equal(t, want, a.Value.AsString(), "attribute %q", key)
			return
		}
	}
	t.Errorf("attribute %q not found in span", key)
}

// assertIntAttr asserts that the given attribute key has the expected int value.
func assertIntAttr(t *testing.T, attrs []attribute.KeyValue, key string, want int) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.Equal(t, int64(want), a.Value.AsInt64(), "attribute %q", key)
			return
		}
	}
	t.Errorf("attribute %q not found in span", key)
}
