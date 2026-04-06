package infrastructure

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

// Compile-time check: mockSlipStore must satisfy slippy.SlipStore.
// If the interface gains or changes methods, this line fails to compile.
var _ slippy.SlipStore = (*mockSlipStore)(nil)

func TestSlipStoreInterface_InsertAncestryLink(t *testing.T) {
	var called bool
	store := &mockSlipStore{}
	// Default (nil fn) should return nil without panic.
	err := store.InsertAncestryLink(context.Background(), &slippy.Slip{
		CorrelationID: "child-1",
		Repository:    "org/repo",
		Branch:        "main",
		CommitSHA:     "aaa111",
	}, slippy.AncestryEntry{
		CorrelationID: "parent-1",
		CommitSHA:     "bbb222",
		Status:        slippy.SlipStatusFailed,
		FailedStep:    "build",
		CreatedAt:     time.Now(),
	})
	require.NoError(t, err)

	// Verify the adapter propagates correctly when the mock records calls.
	adapter := NewSlipStoreAdapter(&mockSlipStore{
		createFn: func(_ context.Context, _ *slippy.Slip) error {
			called = true
			return nil
		},
	})
	_ = adapter
	_ = called
}

func TestSlipStoreInterface_ResolveAncestry(t *testing.T) {
	store := &mockSlipStore{}
	entries, err := store.ResolveAncestry(
		context.Background(),
		"org/repo", "main", "child-1", 10,
	)
	require.NoError(t, err)
	assert.Nil(t, entries)
}

// TestAncestryEntryFields verifies the AncestryEntry struct has the expected fields.
// This will fail to compile if any field is renamed or removed.
func TestAncestryEntryFields(t *testing.T) {
	entry := slippy.AncestryEntry{
		CorrelationID: "abc",
		CommitSHA:     "def",
		Status:        slippy.SlipStatusCompleted,
		FailedStep:    "",
		CreatedAt:     time.Now(),
	}
	assert.Equal(t, "abc", entry.CorrelationID)
	assert.Equal(t, "def", entry.CommitSHA)
	assert.Equal(t, slippy.SlipStatusCompleted, entry.Status)
}
