package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
)

// --- Mock SlipReader ---

type mockReader struct {
	loadFn              func(ctx context.Context, id string) (*domain.Slip, error)
	loadByCommitFn      func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	loadByCommitExactFn func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	findByCommitsFn     func(ctx context.Context, repo string, commits []string) (*domain.Slip, string, error)
	findAllByCommitsFn  func(ctx context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error)
}

func (m *mockReader) Load(ctx context.Context, id string) (*domain.Slip, error) {
	return m.loadFn(ctx, id)
}
func (m *mockReader) LoadByCommit(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitFn(ctx, repo, sha)
}
func (m *mockReader) LoadByCommitExact(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitExactFn(ctx, repo, sha)
}
func (m *mockReader) FindByCommits(ctx context.Context, repo string, commits []string) (*domain.Slip, string, error) {
	return m.findByCommitsFn(ctx, repo, commits)
}

func (m *mockReader) FindAllByCommits(
	ctx context.Context,
	repo string,
	commits []string,
) ([]domain.SlipWithCommit, error) {
	return m.findAllByCommitsFn(ctx, repo, commits)
}

// setupTestAPI creates a huma API with slip routes for testing (no auth middleware).
func setupTestAPI(reader domain.SlipReader) http.Handler {
	mux := http.NewServeMux()
	config := huma.DefaultConfig("Test Slippy API", "0.0.1")
	api := humago.New(mux, config)

	h := NewSlipHandler(reader)
	RegisterRoutes(api, h)

	return mux
}

func TestGetSlip_Success(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "abc-123", Repository: "org/repo", Branch: "main"}
	mock := &mockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			assert.Equal(t, "abc-123", id)
			return expected, nil
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/abc-123", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "abc-123", body.CorrelationID)
	assert.Equal(t, "org/repo", body.Repository)
}

func TestGetSlip_NotFound(t *testing.T) {
	mock := &mockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/not-found-id", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetSlipByCommit_Success(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "def-456", Repository: "org/repo", CommitSHA: "sha123"}
	mock := &mockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, "sha123", sha)
			return expected, nil
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/org/repo/sha123", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var body domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, "def-456", body.CorrelationID)
}

func TestFindByCommits_Success(t *testing.T) {
	expected := &domain.Slip{CorrelationID: "ghi-789"}
	mock := &mockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, []string{"c1", "c2"}, commits)
			return expected, "c1", nil
		},
	}

	handler := setupTestAPI(mock)
	body := `{"repository":"org/repo","commits":["c1","c2"]}`
	req := httptest.NewRequest(http.MethodPost, "/slips/find-by-commits", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Slip          domain.Slip `json:"slip"`
		MatchedCommit string      `json:"matched_commit"`
	}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "ghi-789", resp.Slip.CorrelationID)
	assert.Equal(t, "c1", resp.MatchedCommit)
}

func TestFindByCommits_NotFound(t *testing.T) {
	mock := &mockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return nil, "", slippy.ErrSlipNotFound
		},
	}

	handler := setupTestAPI(mock)
	body := `{"repository":"org/repo","commits":["c1"]}`
	req := httptest.NewRequest(http.MethodPost, "/slips/find-by-commits", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestFindAllByCommits_Success(t *testing.T) {
	results := []domain.SlipWithCommit{
		{Slip: &domain.Slip{CorrelationID: "a"}, MatchedCommit: "c1"},
		{Slip: &domain.Slip{CorrelationID: "b"}, MatchedCommit: "c2"},
	}
	mock := &mockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return results, nil
		},
	}

	handler := setupTestAPI(mock)
	body := `{"repository":"org/repo","commits":["c1","c2"]}`
	req := httptest.NewRequest(http.MethodPost, "/slips/find-all-by-commits", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var items []FindAllByCommitsItem
	require.NoError(t, json.NewDecoder(w.Body).Decode(&items))
	assert.Len(t, items, 2)
	assert.Equal(t, "a", items[0].Slip.CorrelationID)
	assert.Equal(t, "c1", items[0].MatchedCommit)
}

func TestMapError_SlipNotFound(t *testing.T) {
	err := mapError(slippy.ErrSlipNotFound)
	var he huma.StatusError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusNotFound, he.GetStatus())
}

func TestMapError_InvalidCorrelationID(t *testing.T) {
	err := mapError(slippy.ErrInvalidCorrelationID)
	var he huma.StatusError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusBadRequest, he.GetStatus())
}

func TestMapError_GenericError(t *testing.T) {
	err := mapError(assert.AnError)
	var he huma.StatusError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusInternalServerError, he.GetStatus())
}

func TestMapError_InvalidRepository(t *testing.T) {
	err := mapError(slippy.ErrInvalidRepository)
	var he huma.StatusError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusBadRequest, he.GetStatus())
}

func TestMapError_ContextCanceled(t *testing.T) {
	err := mapError(context.Canceled)
	var he huma.StatusError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusGatewayTimeout, he.GetStatus())
}

func TestMapError_DeadlineExceeded(t *testing.T) {
	err := mapError(context.DeadlineExceeded)
	var he huma.StatusError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusGatewayTimeout, he.GetStatus())
}

func TestGetSlip_InternalError(t *testing.T) {
	mock := &mockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return nil, errors.New("clickhouse timeout")
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/abc-123", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetSlip_InvalidCorrelationID(t *testing.T) {
	mock := &mockReader{
		loadFn: func(_ context.Context, id string) (*domain.Slip, error) {
			return nil, slippy.ErrInvalidCorrelationID
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/bad-id!", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetSlipByCommit_NotFound(t *testing.T) {
	mock := &mockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/org/repo/deadbeef", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestGetSlipByCommit_InvalidRepository(t *testing.T) {
	mock := &mockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, slippy.ErrInvalidRepository
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/bad/repo/sha", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGetSlipByCommit_InternalError(t *testing.T) {
	mock := &mockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			return nil, errors.New("network error")
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/org/repo/sha", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFindByCommits_InternalError(t *testing.T) {
	mock := &mockReader{
		findByCommitsFn: func(_ context.Context, repo string, commits []string) (*domain.Slip, string, error) {
			return nil, "", errors.New("db timeout")
		},
	}

	handler := setupTestAPI(mock)
	body := `{"repository":"org/repo","commits":["c1"]}`
	req := httptest.NewRequest(http.MethodPost, "/slips/find-by-commits", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFindAllByCommits_NotFound(t *testing.T) {
	mock := &mockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return nil, slippy.ErrSlipNotFound
		},
	}

	handler := setupTestAPI(mock)
	body := `{"repository":"org/repo","commits":["c1"]}`
	req := httptest.NewRequest(http.MethodPost, "/slips/find-all-by-commits", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestFindAllByCommits_InternalError(t *testing.T) {
	mock := &mockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return nil, errors.New("connection lost")
		},
	}

	handler := setupTestAPI(mock)
	body := `{"repository":"org/repo","commits":["c1"]}`
	req := httptest.NewRequest(http.MethodPost, "/slips/find-all-by-commits", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFindAllByCommits_EmptyResult(t *testing.T) {
	mock := &mockReader{
		findAllByCommitsFn: func(_ context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error) {
			return []domain.SlipWithCommit{}, nil
		},
	}

	handler := setupTestAPI(mock)
	body := `{"repository":"org/repo","commits":["c1"]}`
	req := httptest.NewRequest(http.MethodPost, "/slips/find-all-by-commits", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var items []FindAllByCommitsItem
	require.NoError(t, json.NewDecoder(w.Body).Decode(&items))
	assert.Empty(t, items)
}

func TestNewSlipHandler(t *testing.T) {
	mock := &mockReader{}
	h := NewSlipHandler(mock)
	assert.NotNil(t, h)
}

// TestGetSlipByCommit_RoutesToAncestryResolvingLoad guards R3 — the
// ancestry-preservation invariant for NON-full-SHA refs. The public
// GET /slips/by-commit/{owner}/{repo}/{ref} endpoint MUST route to the
// ancestry-resolving LoadByCommit on the reader. For a short SHA / branch ref
// (like "feedfacecafe", 12 hex chars — not a full 40-hex commit SHA) the
// adapter's exact-first optimization does NOT apply, so the resolver path is
// still the only path that can satisfy the request. Image-tag historical
// lookups and PR/CI "find slip for this commit" workflows on branch refs depend
// on the ancestry walk.
//
// NOTE: As of the exact-first fix in SlipResolverAdapter.LoadByCommit, a FULL
// 40-hex SHA ref MAY be served by a direct exact-SHA read before ancestry (see
// TestGetSlipByCommit_FullSHA_AllowsExactFirst below and the adapter-level
// regression tests in internal/infrastructure/ancestry_test.go). The handler
// still calls the same reader.LoadByCommit method; the exact-vs-ancestry choice
// is an internal adapter concern. This test therefore pins the invariant at the
// handler boundary (handler -> LoadByCommit, never the bare LoadByCommitExact
// method) rather than dictating the adapter's internal resolution path.
func TestGetSlipByCommit_RoutesToAncestryResolvingLoad(t *testing.T) {
	const sentinelCorrelationID = "ancestry-resolved-slip"
	loadByCommitCalls := 0
	loadByCommitExactCalls := 0
	mock := &mockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			loadByCommitCalls++
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, "feedfacecafe", sha)
			return &domain.Slip{
				CorrelationID: sentinelCorrelationID,
				Repository:    repo,
				CommitSHA:     sha,
			}, nil
		},
		loadByCommitExactFn: func(_ context.Context, _, _ string) (*domain.Slip, error) {
			loadByCommitExactCalls++
			return nil, errors.New("handler MUST NOT call the bare LoadByCommitExact method — guards R3; exact-first is an internal LoadByCommit concern")
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/org/repo/feedfacecafe", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, loadByCommitCalls,
		"public GET /slips/by-commit handler MUST call LoadByCommit (ancestry-resolving) exactly once")
	assert.Equal(t, 0, loadByCommitExactCalls,
		"public GET /slips/by-commit handler MUST route through reader.LoadByCommit, never the bare LoadByCommitExact method — exact-first SHA handling lives inside the adapter's LoadByCommit")

	var body domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, sentinelCorrelationID, body.CorrelationID,
		"response must come from the ancestry-resolved load for a non-full-SHA ref")
}

// TestGetSlipByCommit_FullSHA_AllowsExactFirst documents the complementary half
// of the invariant: for a FULL 40-hex SHA the public endpoint still calls
// reader.LoadByCommit (not the bare LoadByCommitExact method) and resolves
// correctly. The exact-first behavior that prevents the 404 flap is exercised at
// the adapter level; here we simply confirm the handler contract is unchanged
// for full-SHA refs and that the public endpoint resolves the slip.
func TestGetSlipByCommit_FullSHA_AllowsExactFirst(t *testing.T) {
	const sentinelCorrelationID = "exact-resolved-slip"
	const fullSHA = "f615c4c0000000000000000000000000deadbeef"
	loadByCommitCalls := 0
	loadByCommitExactCalls := 0
	mock := &mockReader{
		loadByCommitFn: func(_ context.Context, repo, sha string) (*domain.Slip, error) {
			loadByCommitCalls++
			assert.Equal(t, "org/repo", repo)
			assert.Equal(t, fullSHA, sha)
			return &domain.Slip{
				CorrelationID: sentinelCorrelationID,
				Repository:    repo,
				CommitSHA:     sha,
			}, nil
		},
		loadByCommitExactFn: func(_ context.Context, _, _ string) (*domain.Slip, error) {
			loadByCommitExactCalls++
			return nil, errors.New("handler MUST route through reader.LoadByCommit, not the bare LoadByCommitExact method")
		},
	}

	handler := setupTestAPI(mock)
	req := httptest.NewRequest(http.MethodGet, "/slips/by-commit/org/repo/"+fullSHA, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, loadByCommitCalls,
		"public GET /slips/by-commit handler MUST call reader.LoadByCommit exactly once, even for full-SHA refs")
	assert.Equal(t, 0, loadByCommitExactCalls,
		"handler must not call the bare LoadByCommitExact method; exact-first SHA handling is internal to the adapter's LoadByCommit")

	var body domain.Slip
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, sentinelCorrelationID, body.CorrelationID)
}
