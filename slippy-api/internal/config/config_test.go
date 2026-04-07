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
		"SLIPPY_GITHUB_APP_ID", "SLIPPY_GITHUB_APP_PRIVATE_KEY",
		"SLIPPY_GITHUB_ENTERPRISE_URL", "SLIPPY_ANCESTRY_DEPTH",
		"K8S_NAMESPACE",
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
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "test-pem")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, "test-key-123", cfg.APIKey)
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
