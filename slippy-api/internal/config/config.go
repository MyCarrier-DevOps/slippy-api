package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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
}

// Load reads configuration from environment variables.
// Required: SLIPPY_API_KEY, SLIPPY_GITHUB_APP_ID, SLIPPY_GITHUB_APP_PRIVATE_KEY
// Optional: PORT, DRAGONFLY_HOST, DRAGONFLY_PORT, DRAGONFLY_PASSWORD, CACHE_TTL,
//
//	SLIPPY_GITHUB_ENTERPRISE_URL, SLIPPY_ANCESTRY_DEPTH
func Load() (*Config, error) {
	cfg := &Config{
		Port:          8080,
		DragonflyPort: 6379,
		CacheTTL:      10 * time.Minute,
		AncestryDepth: defaultAncestryDepth,
		SlipDatabase:  "ci",
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

	// SlipDatabase: derive from K8S_NAMESPACE — use "ci_test" for dev/test namespaces, "ci" otherwise.
	if ns := os.Getenv("K8S_NAMESPACE"); strings.HasSuffix(ns, "-test") || strings.HasSuffix(ns, "-dev") {
		cfg.SlipDatabase = "ci_test"
	}

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

	return cfg, nil
}

// CacheEnabled returns true if Dragonfly configuration is provided.
func (c *Config) CacheEnabled() bool {
	return c.DragonflyHost != ""
}
