package config

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

const defaultAncestryDepth = 25

// Watchdog modes for SLIPPY_WATCHDOG_MODE.
const (
	// WatchdogModeOff disables the stuck-step watchdog entirely (default).
	WatchdogModeOff = "off"
	// WatchdogModeAlert runs the sweep and emits metrics/logs/traces for stuck
	// steps but does not mutate slip state.
	WatchdogModeAlert = "alert"
	// WatchdogModeEnforce runs the sweep and fails stuck steps via SlipWriter.FailStep.
	WatchdogModeEnforce = "enforce"
)

// Watchdog defaults.
const (
	defaultStepRunningMaxDuration = 2 * time.Hour
	defaultWatchdogSweepInterval  = 5 * time.Minute
	defaultWatchdogBatchLimit     = 100
)

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

	// SkipMigrations controls whether ClickHouse schema migrations run at startup (default: false).
	// Set SLIPPY_SKIP_MIGRATIONS=true to disable migrations — useful when running multiple replicas.
	SkipMigrations bool

	// WatchdogMode controls the stuck-step watchdog: "off" (default — sweep never
	// runs), "alert" (detect + emit metric/log/trace, no mutation), or "enforce"
	// (detect + fail the stuck step via SlipWriter.FailStep). Defense-in-depth for
	// lost terminal callbacks that strand a step in "running" forever.
	WatchdogMode string

	// StepRunningMaxDuration is the staleness threshold: a step whose slip has had
	// no state transition (updated_at) for at least this long while still "running"
	// is considered stuck (default: 2h).
	StepRunningMaxDuration time.Duration

	// WatchdogSweepInterval is how often the watchdog sweep runs (default: 5m).
	WatchdogSweepInterval time.Duration

	// WatchdogBatchLimit caps the number of stuck slips processed per sweep so a
	// backlog cannot make one sweep run unbounded (default: 100).
	WatchdogBatchLimit int
}

// Load reads configuration from environment variables.
// Required: SLIPPY_API_KEY, SLIPPY_WRITE_API_KEY, SLIPPY_GITHUB_APP_ID, SLIPPY_GITHUB_APP_PRIVATE_KEY
// Optional: PORT, DRAGONFLY_HOST, DRAGONFLY_PORT, DRAGONFLY_PASSWORD, CACHE_TTL,
//
//	SLIPPY_GITHUB_ENTERPRISE_URL, SLIPPY_ANCESTRY_DEPTH, SLIPPY_SKIP_MIGRATIONS,
//	SLIPPY_WATCHDOG_MODE, SLIPPY_STEP_RUNNING_MAX_DURATION, SLIPPY_WATCHDOG_SWEEP_INTERVAL,
//	SLIPPY_WATCHDOG_BATCH_LIMIT
func Load() (*Config, error) {
	cfg := &Config{
		Port:                   8080,
		DragonflyPort:          6379,
		CacheTTL:               10 * time.Minute,
		AncestryDepth:          defaultAncestryDepth,
		SlipDatabase:           slippy.DefaultConfig().Database,
		WatchdogMode:           WatchdogModeOff,
		StepRunningMaxDuration: defaultStepRunningMaxDuration,
		WatchdogSweepInterval:  defaultWatchdogSweepInterval,
		WatchdogBatchLimit:     defaultWatchdogBatchLimit,
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

	// Optional: SLIPPY_SKIP_MIGRATIONS (default: true for backward compatibility)
	if v := os.Getenv("SLIPPY_SKIP_MIGRATIONS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_SKIP_MIGRATIONS must be a valid boolean: %w", err)
		}
		cfg.SkipMigrations = b
	}

	// Optional: SLIPPY_WATCHDOG_MODE (off | alert | enforce, default: off)
	if v := os.Getenv("SLIPPY_WATCHDOG_MODE"); v != "" {
		switch v {
		case WatchdogModeOff, WatchdogModeAlert, WatchdogModeEnforce:
			cfg.WatchdogMode = v
		default:
			return nil, fmt.Errorf(
				"SLIPPY_WATCHDOG_MODE must be one of off, alert, enforce: got %q", v)
		}
	}

	// Optional: SLIPPY_STEP_RUNNING_MAX_DURATION (Go duration string, e.g. "2h")
	if v := os.Getenv("SLIPPY_STEP_RUNNING_MAX_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_STEP_RUNNING_MAX_DURATION must be a valid duration (e.g. 2h): %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("SLIPPY_STEP_RUNNING_MAX_DURATION must be greater than 0")
		}
		cfg.StepRunningMaxDuration = d
	}

	// Optional: SLIPPY_WATCHDOG_SWEEP_INTERVAL (Go duration string, e.g. "5m")
	if v := os.Getenv("SLIPPY_WATCHDOG_SWEEP_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_WATCHDOG_SWEEP_INTERVAL must be a valid duration (e.g. 5m): %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("SLIPPY_WATCHDOG_SWEEP_INTERVAL must be greater than 0")
		}
		cfg.WatchdogSweepInterval = d
	}

	// Optional: SLIPPY_WATCHDOG_BATCH_LIMIT (positive integer)
	if v := os.Getenv("SLIPPY_WATCHDOG_BATCH_LIMIT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_WATCHDOG_BATCH_LIMIT must be a valid integer: %w", err)
		}
		if n < 1 {
			return nil, fmt.Errorf("SLIPPY_WATCHDOG_BATCH_LIMIT must be at least 1")
		}
		// The watchdog passes this to ClickHouse as a UInt32 via uint32(batchLimit)
		// (watchdog.detectStuckSteps). Reject anything above MaxInt32 so a large
		// config value can't silently wrap to a tiny — or zero — batch at the cast.
		if n > math.MaxInt32 {
			return nil, fmt.Errorf("SLIPPY_WATCHDOG_BATCH_LIMIT must not exceed %d", math.MaxInt32)
		}
		cfg.WatchdogBatchLimit = n
	}

	return cfg, nil
}

// CacheEnabled returns true if Dragonfly configuration is provided.
func (c *Config) CacheEnabled() bool {
	return c.DragonflyHost != ""
}

// WatchdogEnabled returns true when the stuck-step watchdog should run (mode is
// not "off"). Mirrors CacheEnabled.
func (c *Config) WatchdogEnabled() bool {
	return c.WatchdogMode != WatchdogModeOff
}

// WatchdogEnforces returns true when the watchdog should actually fail stuck
// steps (mode == "enforce"). In "alert" mode it only emits observability signals.
func (c *Config) WatchdogEnforces() bool {
	return c.WatchdogMode == WatchdogModeEnforce
}
