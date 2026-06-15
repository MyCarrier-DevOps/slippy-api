package infrastructure

import (
	"context"
	"fmt"

	ch "github.com/MyCarrier-DevOps/goLibMyCarrier/clickhouse"
)

// expectedAsyncInsertSettings enumerates the ClickHouse session settings whose
// values must equal "1" for the I5 (ADO #82468) race fix to hold. With
// async_insert=1 the server batches client INSERTs in memory; the matching
// wait_for_async_insert=1 setting makes those INSERTs synchronous from the
// client's perspective — i.e. the call does not return until the data is
// durable and visible to subsequent SELECTs on the connection pool.
//
// The slippy-api I5 fix relies on this: after a synchronous event-log INSERT,
// SlipStore.LatestStepStatusFromEvents must observe the just-written row to
// enforce the terminal-wins guard in overlayPipelineStep.
var expectedAsyncInsertSettings = []string{
	"async_insert",
	"wait_for_async_insert",
}

// AssertAsyncInsertEnabled fails fast at startup if the connected ClickHouse
// user profile does not have async_insert=1 AND wait_for_async_insert=1. These
// settings are load-bearing for the I5 race fix (ADO #82468): without
// wait_for_async_insert=1, the event-log row written by appendHistoryWithOverrides
// is not guaranteed visible to the subsequent SELECT inside
// LatestStepStatusFromEvents, which would silently re-introduce the
// 436cc68c-style stale-overlay regression.
//
// Returns an error suitable for fatal logging from main.go. The error message
// includes the actual observed values to make on-call triage one-step.
//
// Defensive behaviour: if a setting is not present in system.settings AT ALL,
// the assertion treats that as a hard failure rather than silently passing —
// missing setting rows on supported CH versions (≥ 24.x) only ever happen if
// the user has been pointed at a non-CH backend or a stripped image.
func AssertAsyncInsertEnabled(ctx context.Context, session ch.ClickhouseSessionInterface) error {
	if session == nil {
		return fmt.Errorf("AssertAsyncInsertEnabled: nil session")
	}
	// Bind one placeholder per required setting; trust the driver to escape.
	args := make([]interface{}, 0, len(expectedAsyncInsertSettings))
	placeholders := ""
	for i, name := range expectedAsyncInsertSettings {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		args = append(args, name)
	}
	q := fmt.Sprintf("SELECT name, value FROM system.settings WHERE name IN (%s)", placeholders)
	rows, err := session.QueryWithArgs(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("AssertAsyncInsertEnabled: query system.settings: %w", err)
	}

	observed := make(map[string]string, len(expectedAsyncInsertSettings))
	var scanErr error
	for rows.Next() {
		var name, value string
		if scanErr = rows.Scan(&name, &value); scanErr != nil {
			scanErr = fmt.Errorf("AssertAsyncInsertEnabled: scan system.settings row: %w", scanErr)
			break
		}
		observed[name] = value
	}
	if closeErr := rows.Close(); scanErr == nil && closeErr != nil {
		return fmt.Errorf("AssertAsyncInsertEnabled: close rows: %w", closeErr)
	}
	if scanErr != nil {
		return scanErr
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("AssertAsyncInsertEnabled: iterate system.settings rows: %w", rowsErr)
	}

	for _, name := range expectedAsyncInsertSettings {
		got, present := observed[name]
		if !present {
			return fmt.Errorf(
				"AssertAsyncInsertEnabled: required setting %q not present in system.settings — I5 race fix prerequisite missing",
				name,
			)
		}
		if got != "1" {
			return fmt.Errorf(
				"AssertAsyncInsertEnabled: setting %q=%q (expected %q) — I5 race fix prerequisite missing (ADO #82468)",
				name, got, "1",
			)
		}
	}
	return nil
}
