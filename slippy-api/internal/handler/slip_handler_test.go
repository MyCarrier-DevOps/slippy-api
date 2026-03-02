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
	loadFn             func(ctx context.Context, id string) (*domain.Slip, error)
	loadByCommitFn     func(ctx context.Context, repo, sha string) (*domain.Slip, error)
	findByCommitsFn    func(ctx context.Context, repo string, commits []string) (*domain.Slip, string, error)
	findAllByCommitsFn func(ctx context.Context, repo string, commits []string) ([]domain.SlipWithCommit, error)
}

func (m *mockReader) Load(ctx context.Context, id string) (*domain.Slip, error) {
	return m.loadFn(ctx, id)
}
func (m *mockReader) LoadByCommit(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return m.loadByCommitFn(ctx, repo, sha)
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
