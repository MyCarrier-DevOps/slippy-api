package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"

	"github.com/MyCarrier-DevOps/slippy-api/internal/domain"
	"github.com/MyCarrier-DevOps/slippy-api/internal/handler"
	"github.com/MyCarrier-DevOps/slippy-api/internal/infrastructure"
	"github.com/MyCarrier-DevOps/slippy-api/internal/middleware"
)

// dedupPipelineConfigJSON is a minimal pipeline config with the first step
// (push_parsed) so CreateSlipForPush initializes a slip correctly.
const dedupPipelineConfigJSON = `{
	"name": "dedup-e2e",
	"steps": [
		{"name": "push_parsed"},
		{"name": "builds_completed", "aggregates": "build"}
	]
}`

// asyncInsertSlipStore is a concurrency-safe in-memory slippy.SlipStore that
// SIMULATES the production ClickHouse async-insert visibility lag that triggers
// the phantom-slip race: a row written by Create is not visible to LoadByCommit
// until insertLag has elapsed. This is exactly the window the dedup lock must
// cover. The store itself performs NO deduplication — every Create is recorded —
// so the test can count how many distinct slips were actually materialized.
type asyncInsertSlipStore struct {
	mu        sync.Mutex
	byCorr    map[string]*slippy.Slip
	byCommit  []*slippy.Slip // append-only; visibility gated by visibleAt
	visibleAt map[string]time.Time
	insertLag time.Duration
	creates   int // total Create calls that succeeded (== materialized slip count)

	// stepEvents tracks the latest pipeline-level (componentName == "") step
	// status per correlation_id for the I5 R1 (ADO #82468) reproducer. Indexed
	// by correlationID → stepName → latest status with its visibleAt timestamp
	// so the read path respects the same async-insert visibility window as the
	// routing_slips path. Default zero-value: no events recorded.
	stepEvents map[string]map[string]stepEventEntry
}

// stepEventEntry is the per-(correlation_id, step) event-log row used by
// asyncInsertSlipStore. The status is durable as of writtenAt + insertLag,
// matching the async-insert visibility model already applied to routing_slips.
type stepEventEntry struct {
	status    slippy.StepStatus
	visibleAt time.Time
}

func newAsyncInsertSlipStore(lag time.Duration) *asyncInsertSlipStore {
	return &asyncInsertSlipStore{
		byCorr:     make(map[string]*slippy.Slip),
		visibleAt:  make(map[string]time.Time),
		insertLag:  lag,
		stepEvents: make(map[string]map[string]stepEventEntry),
	}
}

// recordStepEvent registers a pipeline-level step status transition into the
// in-memory event log. componentName is non-empty for aggregate steps and is
// intentionally NOT recorded here — the I5 R1 guard only consults pipeline-level
// events. Visibility is gated by insertLag to mirror the routing_slips path.
//
// Callers must already hold s.mu.
func (s *asyncInsertSlipStore) recordStepEvent(correlationID, stepName, componentName string, status slippy.StepStatus) {
	if componentName != "" {
		return
	}
	if s.stepEvents[correlationID] == nil {
		s.stepEvents[correlationID] = make(map[string]stepEventEntry)
	}
	s.stepEvents[correlationID][stepName] = stepEventEntry{
		status:    status,
		visibleAt: time.Now().Add(s.insertLag),
	}
}

func commitKey(repo, sha string) string { return repo + "\x00" + sha }

func (s *asyncInsertSlipStore) Create(_ context.Context, slip *slippy.Slip) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates++
	cp := *slip
	s.byCorr[slip.CorrelationID] = &cp
	s.byCommit = append(s.byCommit, &cp)
	// The new row only becomes visible to LoadByCommit after the async-insert lag.
	s.visibleAt[commitKey(slip.Repository, slip.CommitSHA)] = time.Now().Add(s.insertLag)
	return nil
}

func (s *asyncInsertSlipStore) Load(_ context.Context, correlationID string) (*slippy.Slip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slip, ok := s.byCorr[correlationID]
	if !ok {
		return nil, slippy.ErrSlipNotFound
	}
	cp := *slip
	return &cp, nil
}

func (s *asyncInsertSlipStore) LoadByCommit(_ context.Context, repo, sha string) (*slippy.Slip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	visAt, ok := s.visibleAt[commitKey(repo, sha)]
	if !ok || time.Now().Before(visAt) {
		// Not yet inserted, or still inside the async-insert visibility lag.
		return nil, slippy.ErrSlipNotFound
	}
	for _, slip := range s.byCommit {
		if slip.Repository == repo && slip.CommitSHA == sha {
			cp := *slip
			return &cp, nil
		}
	}
	return nil, slippy.ErrSlipNotFound
}

// LoadLiveByCommit mirrors LoadByCommit but excludes terminal-status slips, matching
// the production goLibMyCarrier semantic. The visibility-lag behavior is preserved
// so the dedup-loser poll exercises the same async-insert window.
func (s *asyncInsertSlipStore) LoadLiveByCommit(_ context.Context, repo, sha string) (*slippy.Slip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	visAt, ok := s.visibleAt[commitKey(repo, sha)]
	if !ok || time.Now().Before(visAt) {
		return nil, slippy.ErrSlipNotFound
	}
	for _, slip := range s.byCommit {
		if slip.Repository == repo && slip.CommitSHA == sha && !slip.Status.IsTerminal() {
			cp := *slip
			return &cp, nil
		}
	}
	return nil, slippy.ErrSlipNotFound
}

func (s *asyncInsertSlipStore) createCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.creates
}

// --- Remaining SlipStore methods: minimal no-ops sufficient for create path ---

func (s *asyncInsertSlipStore) FindByCommits(
	_ context.Context, _ string, _ []string,
) (*slippy.Slip, string, error) {
	return nil, "", slippy.ErrSlipNotFound
}

func (s *asyncInsertSlipStore) FindAllByCommits(
	_ context.Context, _ string, _ []string,
) ([]slippy.SlipWithCommit, error) {
	return nil, nil
}

func (s *asyncInsertSlipStore) Update(_ context.Context, slip *slippy.Slip, _ ...slippy.StepStatusOverride) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *slip
	s.byCorr[slip.CorrelationID] = &cp
	return nil
}

func (s *asyncInsertSlipStore) UpdateStep(_ context.Context, id, step, comp string, status slippy.StepStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordStepEvent(id, step, comp, status)
	return nil
}

func (s *asyncInsertSlipStore) UpdateStepWithHistory(
	_ context.Context, id, step, comp string, status slippy.StepStatus, _ slippy.StateHistoryEntry,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordStepEvent(id, step, comp, status)
	return nil
}

// LatestStepStatusFromEvents implements the R1 (ADO #82468) event-log lookup
// against the in-memory step event store. Visibility is gated by insertLag so
// the dedup/cross-step tests can exercise the same async-insert window the
// routing_slips path already simulates. Returns ("", false, nil) when no event
// has been recorded OR when the event is still inside its visibility lag.
func (s *asyncInsertSlipStore) LatestStepStatusFromEvents(
	_ context.Context, correlationID, step string,
) (slippy.StepStatus, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stepMap, ok := s.stepEvents[correlationID]
	if !ok {
		return "", false, nil
	}
	entry, ok := stepMap[step]
	if !ok {
		return "", false, nil
	}
	if time.Now().Before(entry.visibleAt) {
		// inside the async-insert visibility window — treat as "not yet visible"
		return "", false, nil
	}
	return entry.status, true, nil
}

func (s *asyncInsertSlipStore) UpdateComponentStatus(_ context.Context, _, _, _ string, _ slippy.StepStatus) error {
	return nil
}

func (s *asyncInsertSlipStore) UpdateSlipStatus(_ context.Context, _ string, _ slippy.SlipStatus) error {
	return nil
}

func (s *asyncInsertSlipStore) AppendHistory(_ context.Context, _ string, _ slippy.StateHistoryEntry) error {
	return nil
}

func (s *asyncInsertSlipStore) SetComponentImageTag(_ context.Context, _, _, _, _ string) error {
	return nil
}

func (s *asyncInsertSlipStore) InsertAncestryLink(_ context.Context, _ *slippy.Slip, _ slippy.AncestryEntry) error {
	return nil
}

func (s *asyncInsertSlipStore) ResolveAncestry(
	_ context.Context, _, _, _ string, _ int,
) ([]slippy.AncestryEntry, error) {
	return nil, nil
}

func (s *asyncInsertSlipStore) Close() error                 { return nil }
func (s *asyncInsertSlipStore) Ping(_ context.Context) error { return nil }

// dedupGitHubAPI is a no-op GitHubAPI (no ancestry resolution in the e2e).
type dedupGitHubAPI struct{}

func (dedupGitHubAPI) GetCommitAncestry(_ context.Context, _, _, _ string, _ int) ([]string, error) {
	return nil, nil
}
func (dedupGitHubAPI) GetPRHeadCommit(_ context.Context, _, _ string, _ int) (string, error) {
	return "", nil
}
func (dedupGitHubAPI) ClearCache() {}

// storeReaderAdapter adapts the in-memory SlipStore to domain.SlipReader so the
// lock-miss poll path can observe committed (visible) rows.
type storeReaderAdapter struct{ store *asyncInsertSlipStore }

func (r storeReaderAdapter) Load(ctx context.Context, correlationID string) (*domain.Slip, error) {
	return r.store.Load(ctx, correlationID)
}
func (r storeReaderAdapter) LoadByCommit(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return r.store.LoadByCommit(ctx, repo, sha)
}
func (r storeReaderAdapter) LoadByCommitExact(ctx context.Context, repo, sha string) (*domain.Slip, error) {
	return r.store.LoadLiveByCommit(ctx, repo, sha)
}
func (r storeReaderAdapter) FindByCommits(
	ctx context.Context, repo string, commits []string,
) (*domain.Slip, string, error) {
	return r.store.FindByCommits(ctx, repo, commits)
}
func (r storeReaderAdapter) FindAllByCommits(
	ctx context.Context, repo string, commits []string,
) ([]domain.SlipWithCommit, error) {
	return r.store.FindAllByCommits(ctx, repo, commits)
}

// buildDedupWriteServer wires the real write stack: SlipWriteHandler →
// SlipWriterAdapter (with a real RedisLocker over miniredis) → slippy.Client over
// the async-insert in-memory store.
func buildDedupWriteServer(
	t *testing.T,
	apiKey string,
	store *asyncInsertSlipStore,
	rdb redis.Cmdable,
) http.Handler {
	t.Helper()

	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(dedupPipelineConfigJSON))
	require.NoError(t, err)

	client := slippy.NewClientWithDependencies(store, dedupGitHubAPI{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})

	locker := infrastructure.NewRedisLocker(rdb)
	reader := storeReaderAdapter{store: store}
	// dedup E2E exercises the repo:sha lock path; the I5 per-corrID lock is OFF
	// (production default) so failing to inject it does not affect this scenario.
	writer := infrastructure.NewSlipWriterAdapter(client, locker, reader, false)

	mux := http.NewServeMux()
	apiConfig := huma.DefaultConfig("Slippy API Dedup E2E", "0.0.1")
	apiConfig.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"writeApiKey": {Type: "http", Scheme: "bearer"},
	}
	api := humago.New(mux, apiConfig)
	api.UseMiddleware(middleware.NewAPIKeyAuth(apiKey, apiKey))

	v1Only := huma.NewGroup(api, "/v1")
	wh := handler.NewSlipWriteHandler(writer, nil)
	handler.RegisterWriteRoutes(v1Only, wh)

	return mux
}

// buildNoLockWriteServer wires the same write stack as buildDedupWriteServer but
// with a NIL locker — dedup disabled. It is the negative control proving the lock
// is what prevents the duplicate: without it, two concurrent identical-(repo,sha)
// POSTs both pass the lib's LoadByCommit (which is empty inside the async-insert
// visibility lag) and each calls Create, materializing TWO slips.
func buildNoLockWriteServer(
	t *testing.T,
	apiKey string,
	store *asyncInsertSlipStore,
) http.Handler {
	t.Helper()

	pipelineCfg, err := slippy.ParsePipelineConfig([]byte(dedupPipelineConfigJSON))
	require.NoError(t, err)

	client := slippy.NewClientWithDependencies(store, dedupGitHubAPI{}, slippy.Config{
		AncestryDepth:  5,
		PipelineConfig: pipelineCfg,
	})

	// Nil locker → dedup disabled (original lock-free behavior).
	reader := storeReaderAdapter{store: store}
	writer := infrastructure.NewSlipWriterAdapter(client, nil, reader, false)

	mux := http.NewServeMux()
	apiConfig := huma.DefaultConfig("Slippy API No-Lock E2E", "0.0.1")
	apiConfig.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"writeApiKey": {Type: "http", Scheme: "bearer"},
	}
	api := humago.New(mux, apiConfig)
	api.UseMiddleware(middleware.NewAPIKeyAuth(apiKey, apiKey))

	v1Only := huma.NewGroup(api, "/v1")
	wh := handler.NewSlipWriteHandler(writer, nil)
	handler.RegisterWriteRoutes(v1Only, wh)

	return mux
}

// postCreateSlip issues POST /v1/slips and returns the HTTP status + decoded body.
func postCreateSlip(
	t *testing.T,
	srv http.Handler,
	authHeader, correlationID, repo, branch, sha string,
) (int, handler.CreateSlipOutput) {
	t.Helper()
	body := `{` +
		`"correlation_id":"` + correlationID + `",` +
		`"repository":"` + repo + `",` +
		`"branch":"` + branch + `",` +
		`"commit_sha":"` + sha + `"` +
		`}`
	req := httptest.NewRequest(http.MethodPost, "/v1/slips", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var out handler.CreateSlipOutput
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &out.Body)
	}
	return w.Code, out
}

// TestE2E_DedupLock_ConcurrentDuplicatePost is the core regression: two concurrent
// POST /v1/slips with identical (repo, sha) but DISTINCT correlation IDs (mirrors
// duplicate GitHub push webhooks) must materialize EXACTLY ONE slip, and both
// successful responses must carry the SAME correlationID.
func TestE2E_DedupLock_ConcurrentDuplicatePost(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Ping(ctx).Err())

	// 200ms async-insert lag: long enough that without the lock both callers would
	// see "no slip" and both Create.
	store := newAsyncInsertSlipStore(200 * time.Millisecond)
	apiKey := "dedup-e2e-secret"
	srv := buildDedupWriteServer(t, apiKey, store, rdb)
	authHeader := "Bearer " + apiKey

	const repo, branch, sha = "Org/My-Service", "main", "abc123def456"

	type result struct {
		code int
		out  handler.CreateSlipOutput
	}
	results := make([]result, 2)
	corrIDs := []string{
		"aaaaaaaa-1111-2222-3333-444444444444",
		"bbbbbbbb-1111-2222-3333-444444444444",
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // release both goroutines simultaneously
			code, out := postCreateSlip(t, srv, authHeader, corrIDs[idx], repo, branch, sha)
			results[idx] = result{code: code, out: out}
		}(i)
	}
	close(start)
	wg.Wait()

	// Both requests must succeed (one creates, one is suppressed and returns the
	// in-flight slip once it becomes visible).
	for i, r := range results {
		require.Equalf(t, http.StatusCreated, r.code, "request %d failed (corr=%s)", i, corrIDs[i])
		require.NotNilf(t, r.out.Body.Slip, "request %d returned nil slip", i)
	}

	// EXACTLY ONE slip materialized in the store.
	assert.Equal(t, 1, store.createCount(), "expected exactly one slip Create across two duplicate requests")

	// Both responses carry the SAME correlationID (the winner's).
	assert.Equal(t,
		results[0].out.Body.Slip.CorrelationID,
		results[1].out.Body.Slip.CorrelationID,
		"both duplicate responses must return the same slip correlationID",
	)
}

// TestE2E_NoLock_ConcurrentDuplicatePost_CreatesTwo is the NEGATIVE CONTROL for
// TestE2E_DedupLock_ConcurrentDuplicatePost: it runs the identical two concurrent
// duplicate POSTs through the SAME async-insert harness but with the dedup lock
// DISABLED (nil locker). With no lock, both callers see "no slip" inside the
// async-insert visibility lag and both Create — materializing TWO slips. This
// proves the lock (not some other code path) is what suppresses the duplicate.
func TestE2E_NoLock_ConcurrentDuplicatePost_CreatesTwo(t *testing.T) {
	// 200ms async-insert lag: long enough that both concurrent callers race inside
	// the visibility window and both pass the lib's LoadByCommit pre-check.
	store := newAsyncInsertSlipStore(200 * time.Millisecond)
	apiKey := "nolock-e2e-secret"
	srv := buildNoLockWriteServer(t, apiKey, store)
	authHeader := "Bearer " + apiKey

	const repo, branch, sha = "Org/My-Service", "main", "abc123def456"

	type result struct {
		code int
		out  handler.CreateSlipOutput
	}
	results := make([]result, 2)
	corrIDs := []string{
		"cccccccc-1111-2222-3333-444444444444",
		"dddddddd-1111-2222-3333-444444444444",
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // release both goroutines simultaneously
			code, out := postCreateSlip(t, srv, authHeader, corrIDs[idx], repo, branch, sha)
			results[idx] = result{code: code, out: out}
		}(i)
	}
	close(start)
	wg.Wait()

	// Both requests succeed (no lock to suppress either one).
	for i, r := range results {
		require.Equalf(t, http.StatusCreated, r.code, "request %d failed (corr=%s)", i, corrIDs[i])
		require.NotNilf(t, r.out.Body.Slip, "request %d returned nil slip", i)
	}

	// NEGATIVE CONTROL: without the lock, BOTH requests materialized a slip.
	assert.Equal(t, 2, store.createCount(),
		"without the dedup lock, two concurrent duplicate requests must create TWO slips (negative control)")
}

// TestE2E_DedupLock_SequentialDuplicate_Idempotent verifies that once the first
// slip's dedup lock TTL has expired (the slip is durably visible), a later push
// for the SAME (repo, sha) is handled idempotently by the lib's retry path rather
// than being rejected — i.e. the lock does not permanently block the key.
func TestE2E_DedupLock_SequentialDuplicate_Idempotent(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Ping(ctx).Err())

	// No async lag here: the first slip is immediately visible.
	store := newAsyncInsertSlipStore(0)
	apiKey := "dedup-e2e-secret"
	srv := buildDedupWriteServer(t, apiKey, store, rdb)
	authHeader := "Bearer " + apiKey

	const repo, branch, sha = "org/repo", "main", "seq-sha-001"

	code1, out1 := postCreateSlip(t, srv, authHeader, "eeeeeeee-1111-2222-3333-444444444444", repo, branch, sha)
	require.Equal(t, http.StatusCreated, code1)
	require.NotNil(t, out1.Body.Slip)

	// Expire the dedup lock so the second request can re-acquire and the lib's
	// retry path (existing non-terminal slip) returns the same slip idempotently.
	mr.FastForward(infrastructure.DefaultLockTTL + time.Second)

	code2, out2 := postCreateSlip(t, srv, authHeader, "ffffffff-1111-2222-3333-444444444444", repo, branch, sha)
	require.Equal(t, http.StatusCreated, code2)
	require.NotNil(t, out2.Body.Slip)

	// Still exactly one materialized slip — the second request hit the lib retry
	// path (existing non-terminal slip) and did not create a second row.
	assert.Equal(t, 1, store.createCount(), "duplicate after TTL must not create a second slip")
	assert.Equal(t, out1.Body.Slip.CorrelationID, out2.Body.Slip.CorrelationID,
		"sequential duplicate must return the original slip")
}
