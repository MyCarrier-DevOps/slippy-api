package config

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/MyCarrier-DevOps/goLibMyCarrier/slippy"
)

const defaultAncestryDepth = 25

// defaultXFFDepth indexes X-Forwarded-For from the right when resolving a client address.
// See middleware.clientIdentity for why the right-hand end is the only safe one to index.
const defaultXFFDepth = 2

// keyMinLength is the minimum length of either API key.
//
// Both keys are compared with subtle.ConstantTimeCompare against a caller-supplied bearer
// token, and the service has no rate limiting — so a short key is brute-forceable online.
// Nothing previously stopped SLIPPY_API_KEY=x from booting.
//
// 60 is chosen against the deployed values (read key 62 characters, write key 64), so it
// clears production with margin while rejecting anything resembling a placeholder. It is a
// length floor, not an entropy measure: it cannot tell a random 60-character key from a
// repeated character. Real assurance comes from minting keys with a CSPRNG, which belongs
// to the provisioning path rather than to this process.
const keyMinLength = 60

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

	// RateLimitEnabled turns on per-identity Fibonacci backoff for failed authentication.
	// Defaults to false so the control ships inert and is enabled deliberately, per the
	// rollout in docs/superpowers/specs/2026-08-12-auth-rate-limiting-design.md.
	RateLimitEnabled bool

	// XFFDepth indexes X-Forwarded-For from the RIGHT when resolving the client. Default 2,
	// measured against the real edge: Cloudflare appends the true client and the gateway
	// appends Cloudflare, so the caller sits second from the right. Configurable because it
	// is a property of the deployment topology and the value most likely to need changing
	// if a proxy is added or removed in front.
	XFFDepth int

	// TrustedProxies are the edge ranges whose forwarded headers the rate limiter may
	// believe. Only when a request's gateway-appended (rightmost) hop falls inside one of
	// these is CF-Connecting-IP / a depth-indexed XFF entry trusted as the client; off that
	// path only the unforgeable rightmost hop is used. Empty is safe but coarse — see the
	// middleware. Parsed from SLIPPY_TRUSTED_PROXY_CIDRS (comma-separated CIDRs).
	TrustedProxies []*net.IPNet
}

// parseTrustedProxies parses a comma-separated CIDR list into networks.
//
// These are the edge ranges whose forwarded headers the rate limiter may believe. Parsed at
// boot so a malformed entry fails loudly here rather than silently disabling per-client
// attribution at request time.
func parseTrustedProxies(raw string) ([]*net.IPNet, error) {
	if raw == "" {
		return nil, nil
	}
	var out []*net.IPNet
	for part := range strings.SplitSeq(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		_, cidr, err := net.ParseCIDR(part)
		if err != nil {
			// The offending token is operator-supplied network config, not a credential, so
			// naming it is safe and saves a round of guessing which entry was wrong.
			return nil, fmt.Errorf("SLIPPY_TRUSTED_PROXY_CIDRS entry %q is not a valid CIDR", part)
		}
		out = append(out, cidr)
	}
	return out, nil
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
		XFFDepth:      defaultXFFDepth,
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
			return nil, fmt.Errorf("PORT must be a valid integer: %w", errors.Unwrap(err))
		}
		cfg.Port = port
	}

	// Optional: DRAGONFLY_HOST
	if v := os.Getenv("DRAGONFLY_HOST"); v != "" {
		cfg.DragonflyHost = v
	}

	// Optional: DRAGONFLY_PORT
	//
	// Range-checked because this variable has no late backstop and its failure is silent.
	// PORT eventually self-diagnoses at ListenAndServe ("address 99999: invalid port"), but
	// DRAGONFLY_PORT only ever builds a dial string, so an unusable value fails the Redis
	// ping, connectCache logs "caching disabled" and returns a nil client, and run() then
	// skips the Locker entirely — "slip-creation dedup lock disabled (no cache)". A
	// one-character typo would silently turn off both the cache and the duplicate-webhook
	// protection that stops two routing slips being created for one commit, and both log
	// lines read like ordinary optional-dependency degradation.
	if v := os.Getenv("DRAGONFLY_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("DRAGONFLY_PORT must be a valid integer: %w", errors.Unwrap(err))
		}
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("DRAGONFLY_PORT must be between 1 and 65535")
		}
		cfg.DragonflyPort = port
	}

	// Optional: DRAGONFLY_PASSWORD
	cfg.DragonflyPassword = os.Getenv("DRAGONFLY_PASSWORD")

	// Optional: CACHE_TTL (Go duration string, e.g. "5m", "15m")
	if v := os.Getenv("CACHE_TTL"); v != "" {
		ttl, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("CACHE_TTL must be a valid duration (e.g. 10m)")
		}
		cfg.CacheTTL = ttl
	}

	// Required: SLIPPY_GITHUB_APP_ID
	if v := os.Getenv("SLIPPY_GITHUB_APP_ID"); v == "" {
		return nil, fmt.Errorf("SLIPPY_GITHUB_APP_ID is required")
	} else {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_GITHUB_APP_ID must be a valid integer: %w", errors.Unwrap(err))
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
			return nil, fmt.Errorf("SLIPPY_ANCESTRY_DEPTH must be a valid integer: %w", errors.Unwrap(err))
		}
		if depth < 1 {
			return nil, fmt.Errorf("SLIPPY_ANCESTRY_DEPTH must be at least 1")
		}
		cfg.AncestryDepth = depth
	}

	// Optional: SLIPPY_RATE_LIMIT_ENABLED
	if v := os.Getenv("SLIPPY_RATE_LIMIT_ENABLED"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_RATE_LIMIT_ENABLED must be a boolean")
		}
		cfg.RateLimitEnabled = enabled
	}

	// Optional: SLIPPY_XFF_DEPTH
	//
	// Range-checked for the same reason DRAGONFLY_PORT is: a wrong value here fails silently
	// and in the worst direction. Too small and every external caller collapses onto the
	// Cloudflare edge address, so one attacker throttles the whole fleet; too large and the
	// index lands on an attacker-supplied entry, so the limiter never fires at all.
	if v := os.Getenv("SLIPPY_XFF_DEPTH"); v != "" {
		depth, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("SLIPPY_XFF_DEPTH must be a valid integer: %w", errors.Unwrap(err))
		}
		if depth < 1 || depth > 10 {
			return nil, fmt.Errorf("SLIPPY_XFF_DEPTH must be between 1 and 10")
		}
		cfg.XFFDepth = depth
	}

	// Optional: SLIPPY_TRUSTED_PROXY_CIDRS
	proxies, err := parseTrustedProxies(os.Getenv("SLIPPY_TRUSTED_PROXY_CIDRS"))
	if err != nil {
		return nil, err
	}
	cfg.TrustedProxies = proxies

	// Required: SLIPPY_WRITE_API_KEY
	cfg.WriteAPIKey = os.Getenv("SLIPPY_WRITE_API_KEY")
	if cfg.WriteAPIKey == "" {
		return nil, fmt.Errorf("SLIPPY_WRITE_API_KEY is required")
	}

	// Reject a padded key before comparing the two. The middleware trims the presented
	// bearer token (extractBearerToken), so a key carrying surrounding whitespace is not
	// the value that comparison actually sees. Two consequences, both bad: a read key
	// equal to the write key plus a trailing newline is byte-different here — passing the
	// distinctness check below — while still authenticating at the write tier; and a
	// padded WRITE key can never authenticate at all, because the trimmed token never
	// equals the untrimmed key. A Secret created from a file routinely ends in a newline,
	// so this is an ordinary operator slip, not a contrived one.
	//
	// Refusing is better than trimming for the caller: silently accepting a padded value
	// would mean the credential the operator issued is not the credential that works.
	for _, key := range []struct{ name, value string }{
		{"SLIPPY_API_KEY", cfg.APIKey},
		{"SLIPPY_WRITE_API_KEY", cfg.WriteAPIKey},
	} {
		if strings.TrimSpace(key.value) != key.value {
			return nil, fmt.Errorf(
				"%s has leading or trailing whitespace: the bearer token is trimmed before "+
					"comparison, so a padded key either fails to authenticate or silently matches a "+
					"differently-padded key", key.name)
		}
	}

	for _, key := range []struct{ name, value string }{
		{"SLIPPY_API_KEY", cfg.APIKey},
		{"SLIPPY_WRITE_API_KEY", cfg.WriteAPIKey},
	} {
		if len(key.value) < keyMinLength {
			return nil, fmt.Errorf(
				"%s is shorter than the %d-character minimum: the key is compared against a "+
					"caller-supplied bearer token and the service applies no rate limiting, so a "+
					"short key is brute-forceable", key.name, keyMinLength)
		}
	}

	// Refuse rather than warn. Identical keys collapse the read/write tier boundary,
	// and the collapse is invisible once the process is serving: the middleware's
	// tiering still evaluates correctly, it just returns the same answer for both keys,
	// so no request is rejected and nothing is logged. A pod that will not start is
	// loud and bounded; a silently collapsed authorization boundary is neither, and
	// would be discovered only by audit.
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
// Values are trimmed before comparison so this agrees with the credential the middleware
// actually compares — extractBearerToken trims the presented bearer token, which makes
// two keys differing only by surrounding whitespace the same credential in practice.
// Load rejects padded keys outright, so on a loaded Config the trim is a no-op; it
// matters for a hand-built Config, which must not miss a collapse it does have.
//
// Comparison is constant-time by convention rather than necessity — both operands are
// local, and no remote caller can time a startup check. An unset key returns false
// rather than matching another unset key: Load rejects an empty key first with a more
// specific message.
func (c *Config) TiersCollapsed() bool {
	read, write := strings.TrimSpace(c.APIKey), strings.TrimSpace(c.WriteAPIKey)
	if read == "" || write == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(read), []byte(write)) == 1
}
