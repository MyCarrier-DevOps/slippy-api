package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clearEnv unsets all config-related environment variables to ensure test isolation.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SLIPPY_API_KEY", "SLIPPY_WRITE_API_KEY", "PORT",
		"DRAGONFLY_HOST", "DRAGONFLY_PORT", "DRAGONFLY_PASSWORD",
		"CACHE_TTL",
		"SLIPPY_GITHUB_APP_ID", "SLIPPY_GITHUB_APP_PRIVATE_KEY",
		"SLIPPY_GITHUB_ENTERPRISE_URL", "SLIPPY_ANCESTRY_DEPTH",
		"SLIPPY_SKIP_MIGRATIONS",
		"K8S_NAMESPACE",
		"SLIPPY_WATCHDOG_MODE", "SLIPPY_STEP_RUNNING_MAX_DURATION",
		"SLIPPY_WATCHDOG_SWEEP_INTERVAL", "SLIPPY_WATCHDOG_BATCH_LIMIT",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

func TestLoad_MissingAPIKey(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "key")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_API_KEY is required")
}

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "test-key-123")
	t.Setenv("SLIPPY_WRITE_API_KEY", "test-write-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "test-pem")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "test-key-123", cfg.APIKey)
	assert.Equal(t, "test-write-key", cfg.WriteAPIKey)
	assert.Equal(t, 8080, cfg.Port)
	assert.Equal(t, "", cfg.DragonflyHost)
	assert.Equal(t, 6379, cfg.DragonflyPort)
	assert.Equal(t, "", cfg.DragonflyPassword)
	assert.Equal(t, 10*time.Minute, cfg.CacheTTL)
	assert.Equal(t, int64(99), cfg.GitHubAppID)
	assert.Equal(t, "test-pem", cfg.GitHubPrivateKey)
	assert.Equal(t, "", cfg.GitHubEnterpriseURL)
	assert.Equal(t, 25, cfg.AncestryDepth)
	assert.Equal(t, "ci", cfg.SlipDatabase)
	assert.False(t, cfg.SkipMigrations)

	// Watchdog defaults: off, 2h, 5m, 100.
	assert.Equal(t, WatchdogModeOff, cfg.WatchdogMode)
	assert.Equal(t, 2*time.Hour, cfg.StepRunningMaxDuration)
	assert.Equal(t, 5*time.Minute, cfg.WatchdogSweepInterval)
	assert.Equal(t, 100, cfg.WatchdogBatchLimit)
	assert.False(t, cfg.WatchdogEnabled())
	assert.False(t, cfg.WatchdogEnforces())
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
}

func TestLoad_WatchdogValues(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_WATCHDOG_MODE", "enforce")
	t.Setenv("SLIPPY_STEP_RUNNING_MAX_DURATION", "90m")
	t.Setenv("SLIPPY_WATCHDOG_SWEEP_INTERVAL", "30s")
	t.Setenv("SLIPPY_WATCHDOG_BATCH_LIMIT", "25")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, WatchdogModeEnforce, cfg.WatchdogMode)
	assert.Equal(t, 90*time.Minute, cfg.StepRunningMaxDuration)
	assert.Equal(t, 30*time.Second, cfg.WatchdogSweepInterval)
	assert.Equal(t, 25, cfg.WatchdogBatchLimit)
	assert.True(t, cfg.WatchdogEnabled())
	assert.True(t, cfg.WatchdogEnforces())
}

func TestLoad_WatchdogMode_Alert(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_WATCHDOG_MODE", "alert")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, WatchdogModeAlert, cfg.WatchdogMode)
	assert.True(t, cfg.WatchdogEnabled())
	assert.False(t, cfg.WatchdogEnforces(), "alert mode must not enforce")
}

func TestLoad_WatchdogMode_Invalid(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_WATCHDOG_MODE", "panic")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_WATCHDOG_MODE must be one of off, alert, enforce")
}

func TestLoad_WatchdogDuration_Invalid(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_STEP_RUNNING_MAX_DURATION", "two-hours")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_STEP_RUNNING_MAX_DURATION must be a valid duration")
}

func TestLoad_WatchdogDuration_NonPositive(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_STEP_RUNNING_MAX_DURATION", "0s")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_STEP_RUNNING_MAX_DURATION must be greater than 0")
}

func TestLoad_WatchdogSweepInterval_Invalid(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_WATCHDOG_SWEEP_INTERVAL", "soon")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_WATCHDOG_SWEEP_INTERVAL must be a valid duration")
}

func TestLoad_WatchdogBatchLimit_Invalid(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_WATCHDOG_BATCH_LIMIT", "lots")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_WATCHDOG_BATCH_LIMIT must be a valid integer")
}

func TestLoad_WatchdogBatchLimit_TooSmall(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	t.Setenv("SLIPPY_WATCHDOG_BATCH_LIMIT", "0")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_WATCHDOG_BATCH_LIMIT must be at least 1")
}

func TestLoad_WatchdogBatchLimit_TooLarge(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	// MaxInt32 + 1: would wrap when cast to uint32 at the ClickHouse query sink.
	t.Setenv("SLIPPY_WATCHDOG_BATCH_LIMIT", "2147483648")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_WATCHDOG_BATCH_LIMIT must not exceed 2147483647")
}

func TestLoad_WatchdogBatchLimit_MaxInt32Accepted(t *testing.T) {
	clearEnv(t)
	setRequiredEnv(t)
	// Boundary: MaxInt32 is still within uint32 range and must be accepted.
	t.Setenv("SLIPPY_WATCHDOG_BATCH_LIMIT", "2147483647")

	cfg, err := Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, 2147483647, cfg.WatchdogBatchLimit)
}

func TestLoad_SlipDatabase_DerivedFromNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		wantDB    string
	}{
		{"slippy-api-test", "ci_test"},
		{"slippy-api-dev", "ci_test"},
		{"dev", "ci_test"},
		{"feature-abc", "ci_test"},
		{"slippy-api-prod", "ci"},
		{"slippy-api", "ci"},
		{"", "ci"},
	}
	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", "key")
			t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			if tt.namespace != "" {
				t.Setenv("K8S_NAMESPACE", tt.namespace)
			}

			cfg, err := Load()
			require.NoError(t, err)
			assert.Equal(t, tt.wantDB, cfg.SlipDatabase)
		})
	}
}

func TestLoad_AllValues(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "my-secret")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-secret")
	t.Setenv("PORT", "9090")
	t.Setenv("DRAGONFLY_HOST", "dragonfly.local")
	t.Setenv("DRAGONFLY_PORT", "6380")
	t.Setenv("DRAGONFLY_PASSWORD", "dragon-pass")
	t.Setenv("CACHE_TTL", "5m")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "my-secret", cfg.APIKey)
	assert.Equal(t, 9090, cfg.Port)
	assert.Equal(t, "dragonfly.local", cfg.DragonflyHost)
	assert.Equal(t, 6380, cfg.DragonflyPort)
	assert.Equal(t, "dragon-pass", cfg.DragonflyPassword)
	assert.Equal(t, 5*time.Minute, cfg.CacheTTL)
}

func TestLoad_InvalidPort(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("PORT", "not-a-number")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "PORT must be a valid integer")
}

func TestLoad_InvalidDragonflyPort(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("DRAGONFLY_PORT", "bad")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "DRAGONFLY_PORT must be a valid integer")
}

func TestLoad_InvalidCacheTTL(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("CACHE_TTL", "not-a-duration")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "CACHE_TTL must be a valid duration")
}

func TestCacheEnabled(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		expected bool
	}{
		{"enabled when host set", "dragonfly.local", true},
		{"disabled when host empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{DragonflyHost: tt.host}
			assert.Equal(t, tt.expected, cfg.CacheEnabled())
		})
	}
}

func TestLoad_MissingGitHubAppID(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_GITHUB_APP_ID is required")
}

func TestLoad_MissingGitHubPrivateKey(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_GITHUB_APP_PRIVATE_KEY is required")
}

func TestLoad_GitHubConfig(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "12345")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "test-key-pem")
	t.Setenv("SLIPPY_GITHUB_ENTERPRISE_URL", "https://github.example.com")
	t.Setenv("SLIPPY_ANCESTRY_DEPTH", "50")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, int64(12345), cfg.GitHubAppID)
	assert.Equal(t, "test-key-pem", cfg.GitHubPrivateKey)
	assert.Equal(t, "https://github.example.com", cfg.GitHubEnterpriseURL)
	assert.Equal(t, 50, cfg.AncestryDepth)
}

func TestLoad_InvalidGitHubAppID(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "not-a-number")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_GITHUB_APP_ID must be a valid integer")
}

func TestLoad_InvalidAncestryDepth(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_ANCESTRY_DEPTH", "abc")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_ANCESTRY_DEPTH must be a valid integer")
}

func TestLoad_AncestryDepthTooSmall(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_ANCESTRY_DEPTH", "0")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_ANCESTRY_DEPTH must be at least 1")
}

func TestLoad_WriteAPIKey(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "read-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-key-abc")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "write-key-abc", cfg.WriteAPIKey)
}

func TestLoad_MissingWriteAPIKey(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "read-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_WRITE_API_KEY is required")
}

func TestLoad_SkipMigrations(t *testing.T) {
	tests := []struct {
		name     string
		envVal   string
		expected bool
	}{
		{"default (absent)", "", false},
		{"explicit true", "true", true},
		{"explicit TRUE", "TRUE", true},
		{"explicit 1", "1", true},
		{"explicit false", "false", false},
		{"explicit FALSE", "FALSE", false},
		{"explicit 0", "0", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", "key")
			t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			if tt.envVal != "" {
				t.Setenv("SLIPPY_SKIP_MIGRATIONS", tt.envVal)
			}

			cfg, err := Load()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, cfg.SkipMigrations)
		})
	}
}

func TestLoad_SkipMigrations_Invalid(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_SKIP_MIGRATIONS", "yes")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_SKIP_MIGRATIONS must be a valid boolean")
}
