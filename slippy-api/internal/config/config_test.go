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
		"SLIPPY_API_KEY", "PORT",
		"DRAGONFLY_HOST", "DRAGONFLY_PORT", "DRAGONFLY_PASSWORD",
		"CACHE_TTL",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

func TestLoad_MissingAPIKey(t *testing.T) {
	clearEnv(t)

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_API_KEY is required")
}

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "test-key-123")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "test-key-123", cfg.APIKey)
	assert.Equal(t, 8080, cfg.Port)
	assert.Equal(t, "", cfg.DragonflyHost)
	assert.Equal(t, 6379, cfg.DragonflyPort)
	assert.Equal(t, "", cfg.DragonflyPassword)
	assert.Equal(t, 10*time.Minute, cfg.CacheTTL)
}

func TestLoad_AllValues(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "my-secret")
	t.Setenv("PORT", "9090")
	t.Setenv("DRAGONFLY_HOST", "dragonfly.local")
	t.Setenv("DRAGONFLY_PORT", "6380")
	t.Setenv("DRAGONFLY_PASSWORD", "dragon-pass")
	t.Setenv("CACHE_TTL", "5m")

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
	t.Setenv("PORT", "not-a-number")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "PORT must be a valid integer")
}

func TestLoad_InvalidDragonflyPort(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
	t.Setenv("DRAGONFLY_PORT", "bad")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "DRAGONFLY_PORT must be a valid integer")
}

func TestLoad_InvalidCacheTTL(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "key")
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
