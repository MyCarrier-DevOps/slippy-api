package config

import (
	"crypto/subtle"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

const defaultAncestryDepth = 25

// Config holds all application configuration loaded from environment variables.
type Config struct {
	// Port is the HTTP server listen port (default: 8080)
	Port int

	// APIKey is the bearer token required for authenticated endpoints
	APIKey string

	// DragonflyHost is the Dragonfly/Redis host address
	DragonflyHost string

	// DragonflyPort is the Dragonfly/Redis port (default: 6379)
	DragonflyPort int

	// DragonflyPassword is the Dragonfly/Redis password (optional)
	DragonflyPassword string

	// CacheTTL is how long cached query results live (default: 10m)
	CacheTTL time.Duration

	// GitHubAppID is the GitHub App ID for commit ancestry resolution
	GitHubAppID int64

	// GitHubPrivateKey is the PEM-encoded private key (or file path) for the GitHub App
	GitHubPrivateKey string

	// GitHubEnterpriseURL is the base URL for GitHub Enterprise Server (optional)
	GitHubEnterpriseURL string

	// AncestryDepth is how many commits to walk when resolving ancestry (default: 25)
	AncestryDepth int

	// SlipDatabase is the ClickHouse database containing routing_slips (default: "ci")
	SlipDatabase string

	// WriteAPIKey is the bearer token for write endpoints (required).
	WriteAPIKey string
}

// Load reads configuration from environment variables.
// Required: SLIPPY_API_KEY, SLIPPY_WRITE_API_KEY, SLIPPY_GITHUB_APP_ID, SLIPPY_GITHUB_APP_PRIVATE_KEY
// Optional: PORT, DRAGONFLY_HOST, DRAGONFLY_PORT, DRAGONFLY_PASSWORD, CACHE_TTL,
//
//	SLIPPY_GITHUB_ENTERPRISE_URL, SLIPPY_ANCESTRY_DEPTH
func Load() (*Config, error) {
	cfg := &Config{
		Port:          8080,
		DragonflyPort: 6379,
		CacheTTL:      10 * time.Minute,
		AncestryDepth: defaultAncestryDepth,
		SlipDatabase:  slippy.DefaultConfig().Database,
	}

	// Required
	cfg.APIKey = os.Getenv("SLIPPY_API_KEY")
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("SLIPPY_API_KEY is required")
	}

	// Optional: PORT
	if v := os.Getenv("PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("PORT must be a valid integer: %w", err)
		}
		cfg.Port = port
	}

	// Optional: DRAGONFLY_HOST
	if v := os.Getenv("DRAGONFLY_HOST"); v != "" {
		cfg.DragonflyHost = v
	}

	// Optional: DRAGONFLY_PORT
	if v := os.Getenv("DRAGONFLY_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("DRAGONFLY_PORT must be a valid integer: %w", err)
		}
		cfg.DragonflyPort = port
	}

	// Optional: DRAGONFLY_PASSWORD
	cfg.DragonflyPassword = os.Getenv("DRAGONFLY_PASSWORD")

	// Optional: CACHE_TTL (Go duration string, e.g. "5m", "15m")
	if v := os.Getenv("CACHE_TTL"); v != "" {
		ttl, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("CACHE_TTL must be a valid duration (e.g. 10m): %w", err)
		}
		cfg.CacheTTL = ttl
	}

	// Required: SLIPPY_GITHUB_APP_ID
	if v := os.Getenv("SLIPPY_GITHUB_APP_ID"); v == "" {
		return nil, fmt.Errorf("SLIPPY_GITHUB_APP_ID is required")
	} else {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_GITHUB_APP_ID must be a valid integer: %w", err)
		}
		cfg.GitHubAppID = id
	}

	// Required: SLIPPY_GITHUB_APP_PRIVATE_KEY
	cfg.GitHubPrivateKey = os.Getenv("SLIPPY_GITHUB_APP_PRIVATE_KEY")
	if cfg.GitHubPrivateKey == "" {
		return nil, fmt.Errorf("SLIPPY_GITHUB_APP_PRIVATE_KEY is required")
	}

	// Optional: SLIPPY_GITHUB_ENTERPRISE_URL
	cfg.GitHubEnterpriseURL = os.Getenv("SLIPPY_GITHUB_ENTERPRISE_URL")

	// Optional: SLIPPY_ANCESTRY_DEPTH
	if v := os.Getenv("SLIPPY_ANCESTRY_DEPTH"); v != "" {
		depth, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_ANCESTRY_DEPTH must be a valid integer: %w", err)
		}
		if depth < 1 {
			return nil, fmt.Errorf("SLIPPY_ANCESTRY_DEPTH must be at least 1")
		}
		cfg.AncestryDepth = depth
	}

	// Required: SLIPPY_WRITE_API_KEY
	cfg.WriteAPIKey = os.Getenv("SLIPPY_WRITE_API_KEY")
	if cfg.WriteAPIKey == "" {
		return nil, fmt.Errorf("SLIPPY_WRITE_API_KEY is required")
	}

	// Refuse rather than warn. Identical keys collapse the read/write tier boundary,
	// and the collapse is invisible once the process is serving: the middleware's
	// tiering still evaluates correctly, it just returns the same answer for both keys,
	// so no request is rejected and nothing is logged. A pod that will not start is
	// loud, bounded, and leaves the previous ReplicaSet serving; a silently collapsed
	// authorization boundary is none of those and would be discovered only by audit.
	if cfg.TiersCollapsed() {
		return nil, fmt.Errorf(
			"SLIPPY_API_KEY and SLIPPY_WRITE_API_KEY must be distinct: identical values make the " +
				"read/write tier split inert, so every read-key holder can mutate routing slips")
	}

	return cfg, nil
}

// CacheEnabled returns true if Dragonfly configuration is provided.
func (c *Config) CacheEnabled() bool {
	return c.DragonflyHost != ""
}

// TiersCollapsed reports whether both API keys are set to the same value, which makes
// the read/write tier split inert: every read-key holder can mutate slips. Load refuses
// to return a Config in that state.
//
// The condition is invisible from inside the request path. The middleware's tiering
// still evaluates correctly — it just produces the same outcome for both keys — so no
// request is rejected and nothing is logged. The one runtime tell is that the
// auth.access_level span attribute reads "write" for read-tier operations too, which is
// indistinguishable from legitimate write traffic without knowing to look.
//
// The two key populations are meant to be disjoint: SLIPPY_API_KEY is fanned out to
// service repositories through the GitHub Actions workflow templates in admin/, while
// SLIPPY_WRITE_API_KEY belongs to in-cluster pipeline components. Collapsing them
// silently grants the Actions-runner population write access to the slip state machine.
//
// Comparison is constant-time for the same reason the middleware's is. An unset key
// returns false rather than matching another unset key: Load rejects an empty key
// first with a more specific message, and a hand-built Config should not report a
// collapsed tier it does not have.
func (c *Config) TiersCollapsed() bool {
	if c.APIKey == "" || c.WriteAPIKey == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(c.APIKey), []byte(c.WriteAPIKey)) == 1
}
