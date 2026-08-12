package config

import (
	"os"
	"strconv"
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
		"SLIPPY_RATE_LIMIT_ENABLED", "SLIPPY_XFF_DEPTH",
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
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "test-pem")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, testReadKey, cfg.APIKey)
	assert.Equal(t, testWriteKey, cfg.WriteAPIKey)
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
			t.Setenv("SLIPPY_API_KEY", testReadKey)
			t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
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
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
	t.Setenv("PORT", "9090")
	t.Setenv("DRAGONFLY_HOST", "dragonfly.local")
	t.Setenv("DRAGONFLY_PORT", "6380")
	t.Setenv("DRAGONFLY_PASSWORD", "dragon-pass")
	t.Setenv("CACHE_TTL", "5m")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, testReadKey, cfg.APIKey)
	assert.Equal(t, 9090, cfg.Port)
	assert.Equal(t, "dragonfly.local", cfg.DragonflyHost)
	assert.Equal(t, 6380, cfg.DragonflyPort)
	assert.Equal(t, "dragon-pass", cfg.DragonflyPassword)
	assert.Equal(t, 5*time.Minute, cfg.CacheTTL)
}

func TestLoad_InvalidPort(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("PORT", "not-a-number")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "PORT must be a valid integer")
}

func TestLoad_InvalidDragonflyPort(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("DRAGONFLY_PORT", "bad")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "DRAGONFLY_PORT must be a valid integer")
}

// A typo and an overflow are different operator problems and must not read identically.
// The four strconv sites drop %w so a mis-wired secretKeyRef cannot echo a credential into
// the pod log (strconv's *NumError carries the rejected input in .Num). Unwrapping first
// keeps the sentinel — which contains no input — so the cause survives both in the message
// and for errors.Is.
func TestLoad_StrconvFailuresPreserveTheirCause(t *testing.T) {
	for _, tc := range []struct {
		name, envKey, value string
		wantSentinel        error
		wantText            string
	}{
		{"syntax", "DRAGONFLY_PORT", "6379x", strconv.ErrSyntax, "invalid syntax"},
		{"range", "DRAGONFLY_PORT", "99999999999999999999", strconv.ErrRange, "value out of range"},
		{"syntax on PORT", "PORT", "80x", strconv.ErrSyntax, "invalid syntax"},
		{"range on app id", "SLIPPY_GITHUB_APP_ID", "99999999999999999999", strconv.ErrRange, "value out of range"},
		{"syntax on depth", "SLIPPY_ANCESTRY_DEPTH", "25x", strconv.ErrSyntax, "invalid syntax"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", testReadKey)
			t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			t.Setenv(tc.envKey, tc.value)

			_, err := Load()
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.wantSentinel,
				"the cause must survive so callers can distinguish a typo from an overflow")
			assert.Contains(t, err.Error(), tc.wantText,
				"the operator-facing message must say which failure it was")
			assert.Contains(t, err.Error(), tc.envKey, "the message must name the variable")
			assert.NotContains(t, err.Error(), tc.value,
				"the rejected input must never reach the log — it may be a mis-wired credential")
		})
	}
}

// strconv.Atoi accepts these happily, and DRAGONFLY_PORT has no late backstop the way PORT
// does — an unusable value only fails the Redis ping, which is logged as ordinary optional
// dependency degradation while silently taking the slip-creation dedup lock down with it.
func TestLoad_OutOfRangeDragonflyPort(t *testing.T) {
	for _, port := range []string{"0", "-1", "99999"} {
		t.Run(port, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", testReadKey)
			t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			t.Setenv("DRAGONFLY_PORT", port)

			cfg, err := Load()
			assert.Nil(t, cfg)
			assert.ErrorContains(t, err, "DRAGONFLY_PORT must be between 1 and 65535")
		})
	}
}

func TestLoad_BoundaryDragonflyPortsAccepted(t *testing.T) {
	for _, port := range []string{"1", "6379", "65535"} {
		t.Run(port, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", testReadKey)
			t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			t.Setenv("DRAGONFLY_PORT", port)

			cfg, err := Load()
			require.NoError(t, err)
			assert.Equal(t, port, strconv.Itoa(cfg.DragonflyPort))
		})
	}
}

func TestLoad_InvalidCacheTTL(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
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
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_GITHUB_APP_ID is required")
}

func TestLoad_MissingGitHubPrivateKey(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_GITHUB_APP_PRIVATE_KEY is required")
}

func TestLoad_GitHubConfig(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
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
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "not-a-number")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_GITHUB_APP_ID must be a valid integer")
}

func TestLoad_InvalidAncestryDepth(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_ANCESTRY_DEPTH", "abc")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_ANCESTRY_DEPTH must be a valid integer")
}

func TestLoad_AncestryDepthTooSmall(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_ANCESTRY_DEPTH", "0")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_ANCESTRY_DEPTH must be at least 1")
}

func TestLoad_WriteAPIKey(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, testWriteKey, cfg.WriteAPIKey)
}

func TestLoad_MissingWriteAPIKey(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
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
	t.Setenv("SLIPPY_API_KEY", "identical-key-for-both-0000000000000000000000000000000000000000")
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_WRITE_API_KEY", "identical-key-for-both-0000000000000000000000000000000000000000")

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
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)

	cfg, err := Load()

	require.NoError(t, err)
	assert.Equal(t, testReadKey, cfg.APIKey)
	assert.Equal(t, testWriteKey, cfg.WriteAPIKey)
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
		{"read key trailing newline", testReadKey + "\n", testWriteKey, "SLIPPY_API_KEY"},
		{"write key trailing newline", testReadKey, testWriteKey + "\n", "SLIPPY_WRITE_API_KEY"},
		{"read key leading space", " " + testReadKey, testWriteKey, "SLIPPY_API_KEY"},
		{
			"read key is write key plus newline",
			testWriteKey + "\n", testWriteKey, "SLIPPY_API_KEY",
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

// testReadKey and testWriteKey are realistic-length credentials. Fixtures must clear
// keyMinLength or they exercise the length refusal instead of the case under test.
const (
	testReadKey  = "test-read-key-0000000000000000000000000000000000000000000000000"
	testWriteKey = "test-write-key-000000000000000000000000000000000000000000000000"
)

// TestLoad_ShortKeyRefused pins the entropy floor.
//
// Both keys are compared with subtle.ConstantTimeCompare against an attacker-supplied
// bearer token, and there is no rate limiting anywhere in the service — so a short key is
// brute-forceable online. Nothing previously stopped SLIPPY_API_KEY=x from booting.
//
// The floor is 60: the deployed read key is 62 characters and the write key is 64, so this
// clears production with margin while rejecting anything resembling a placeholder.
func TestLoad_ShortKeyRefused(t *testing.T) {
	tests := []struct {
		name  string
		read  string
		write string
		field string
	}{
		{"tiny read key", "xy", testWriteKey, "SLIPPY_API_KEY"},
		{"tiny write key", testReadKey, "xy", "SLIPPY_WRITE_API_KEY"},
		{"one below the floor", strings.Repeat("a", 59), testWriteKey, "SLIPPY_API_KEY"},
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
			assert.Contains(t, err.Error(), "60")
			// Never echo the key material.
			assert.NotContains(t, err.Error(), tt.read)
			assert.NotContains(t, err.Error(), tt.write)
		})
	}
}

// TestLoad_KeyAtFloorAccepted is the boundary control: exactly 60 must pass.
func TestLoad_KeyAtFloorAccepted(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", strings.Repeat("a", 60))
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_WRITE_API_KEY", strings.Repeat("b", 60))

	cfg, err := Load()

	require.NoError(t, err)
	assert.NotNil(t, cfg)
}

func TestLoad_RateLimitEnabled(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{{"true", true}, {"1", true}, {"false", false}, {"0", false}} {
		t.Run(tc.value, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", testReadKey)
			t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			t.Setenv("SLIPPY_RATE_LIMIT_ENABLED", tc.value)

			cfg, err := Load()
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.RateLimitEnabled)
		})
	}
}

// Ships inert: the control is enabled deliberately after the resolved client address has
// been confirmed against real traffic, not by defaulting on.
func TestLoad_RateLimitDefaultsOff(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")

	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.RateLimitEnabled)
	assert.Equal(t, defaultXFFDepth, cfg.XFFDepth)
}

func TestLoad_InvalidRateLimitEnabled(t *testing.T) {
	clearEnv(t)
	t.Setenv("SLIPPY_API_KEY", testReadKey)
	t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
	t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
	t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
	t.Setenv("SLIPPY_RATE_LIMIT_ENABLED", "yes-please")

	cfg, err := Load()
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "SLIPPY_RATE_LIMIT_ENABLED must be a boolean")
	assert.NotContains(t, err.Error(), "yes-please", "the rejected value must not reach the log")
}

// A wrong depth fails silently and in the worst direction either way — too small collapses
// every external caller onto the Cloudflare edge, too large lands on an attacker-supplied
// entry — so it is range-checked at boot rather than discovered in production.
func TestLoad_XFFDepth(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		wantErr     string
		want        int
	}{
		{name: "accepted", value: "1", want: 1},
		{name: "accepted upper bound", value: "10", want: 10},
		{name: "zero rejected", value: "0", wantErr: "between 1 and 10"},
		{name: "negative rejected", value: "-1", wantErr: "between 1 and 10"},
		{name: "absurd rejected", value: "99", wantErr: "between 1 and 10"},
		{name: "non-numeric rejected", value: "two", wantErr: "must be a valid integer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("SLIPPY_API_KEY", testReadKey)
			t.Setenv("SLIPPY_WRITE_API_KEY", testWriteKey)
			t.Setenv("SLIPPY_GITHUB_APP_ID", "99")
			t.Setenv("SLIPPY_GITHUB_APP_PRIVATE_KEY", "pem")
			t.Setenv("SLIPPY_XFF_DEPTH", tc.value)

			cfg, err := Load()
			if tc.wantErr != "" {
				assert.Nil(t, cfg)
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.XFFDepth)
		})
	}
}
