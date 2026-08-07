package config

import (
	"os"
	"strings"
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

// TestTiersCollapsed pins the detection of a config that nullifies the read/write
// split. Every tiering mechanism in the middleware evaluates correctly when the two
// keys are equal and still produces the same outcome for both, so nothing downstream
// can notice; this predicate is the only place the condition is visible.
func TestTiersCollapsed(t *testing.T) {
	tests := []struct {
		name     string
		read     string
		write    string
		expected bool
	}{
		{"distinct keys", "read-key", "write-key", false},
		{"identical keys", "same-key", "same-key", true},
		{"differ by one byte", "same-keY", "same-key", false},
		{"differing lengths", "same-key", "same-key-longer", false},
		// The middleware trims the presented bearer token, so keys that differ only by
		// surrounding whitespace are the SAME credential at the comparison that decides
		// the tier. TiersCollapsed must agree with that view, not with a raw byte compare.
		{"differ only by trailing newline", "same-key\n", "same-key", true},
		{"differ only by leading space", " same-key", "same-key", true},
		{"differ only by surrounding whitespace", "\tsame-key ", "same-key", true},
		{"read key unset", "", "write-key", false},
		{"write key unset", "read-key", "", false},
		{"both unset", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{APIKey: tt.read, WriteAPIKey: tt.write}
			assert.Equal(t, tt.expected, cfg.TiersCollapsed())
		})
	}
}

// TestLoad_IdenticalKeysRefused pins the refusal. Identical keys make the read/write
// tier split inert, and the condition is invisible from the request path — the tiering
// evaluates correctly and simply returns the same answer for both. A pod that will not
// start is recoverable; a silently collapsed authorization boundary is not.
func TestLoad_IdenticalKeysRefused(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "same-key-for-both")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_WRITE_API_KEY", "same-key-for-both")

	cfg, err := Load()

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "SLIPPY_API_KEY")
	assert.Contains(t, err.Error(), "SLIPPY_WRITE_API_KEY")
	assert.Contains(t, err.Error(), "distinct")
}

// TestLoad_DistinctKeysAccepted is the control: the refusal must key on equality, not
// on both variables merely being set.
func TestLoad_DistinctKeysAccepted(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", "read-key")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_WRITE_API_KEY", "write-key")

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, "read-key", cfg.APIKey)
	assert.Equal(t, "write-key", cfg.WriteAPIKey)
}

// TestLoad_WhitespaceKeyRefused pins the refusal of a padded key.
//
// extractBearerToken trims the presented token, so a key carrying surrounding whitespace
// is not the value the middleware ends up comparing. Two consequences, both bad: a read
// key that is the write key plus a trailing newline passes the distinctness check and
// still authenticates at the write tier; and a write key carrying whitespace can never
// authenticate at all, because the trimmed token never equals the untrimmed key.
// Kubernetes Secrets created from files routinely carry a trailing newline.
func TestLoad_WhitespaceKeyRefused(t *testing.T) {
	tests := []struct {
		name  string
		read  string
		write string
		field string
	}{
		{"read key trailing newline", "read-key\n", "write-key", "SLIPPY_API_KEY"},
		{"write key trailing newline", "read-key", "write-key\n", "SLIPPY_WRITE_API_KEY"},
		{"read key leading space", " read-key", "write-key", "SLIPPY_API_KEY"},
		{
			"read key is write key plus newline",
			"write-key\n", "write-key", "SLIPPY_API_KEY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", tt.read)
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			t.Setenv("SLIPPY_WRITE_API_KEY", tt.write)

			cfg, err := Load()

			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Contains(t, err.Error(), tt.field)
			assert.Contains(t, err.Error(), "whitespace")
			// The message must never echo EITHER key's material. Asserting only on the read
			// key would leave the higher-privilege credential unchecked.
			assert.NotContains(t, err.Error(), strings.TrimSpace(tt.read))
			assert.NotContains(t, err.Error(), strings.TrimSpace(tt.write))
		})
	}
}
