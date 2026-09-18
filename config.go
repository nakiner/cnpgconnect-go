package cnpgconnectgo

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"
)

// Config describes discovery, independently from PostgreSQL authentication/TLS.
// Address is a gRPC target, normally dns:///discovery.example.com:443.
type Config struct {
	Address   string
	Namespace string
	Cluster   string
	// Network optionally pins an advertised endpoint map entry. When omitted,
	// adapters try the member's internal address, then its external address.
	Network string
	// Token is optional, for discovery servers configured with bearer auth.
	Token     string
	TLSConfig *tls.Config
	// Insecure explicitly enables plaintext discovery for local development.
	// It cannot be combined with a token or TLS configuration.
	Insecure       bool
	StartupTimeout time.Duration
	ReconnectMin   time.Duration
	ReconnectMax   time.Duration
	// MaxSnapshotTTL bounds accepted freshness even if the server advertises
	// a longer lifetime. It is measured from the server's observation time.
	MaxSnapshotTTL time.Duration
}

func (c Config) normalized() (Config, error) {
	if strings.TrimSpace(c.Address) == "" || !resourceName(c.Namespace) || !resourceName(c.Cluster) {
		return c, fmt.Errorf("cnpgconnect-go: discovery address and valid namespace/cluster names are required")
	}
	if c.StartupTimeout == 0 {
		c.StartupTimeout = 10 * time.Second
	}
	if c.ReconnectMin == 0 {
		c.ReconnectMin = 100 * time.Millisecond
	}
	if c.ReconnectMax == 0 {
		c.ReconnectMax = 5 * time.Second
	}
	if c.MaxSnapshotTTL == 0 {
		c.MaxSnapshotTTL = 30 * time.Second
	}
	if c.StartupTimeout < 0 || c.ReconnectMin < 0 || c.ReconnectMax < c.ReconnectMin || c.MaxSnapshotTTL < time.Millisecond {
		return c, fmt.Errorf("cnpgconnect-go: timeouts must be positive and reconnect maximum must not be below minimum")
	}
	if c.Insecure {
		if c.Token != "" || c.TLSConfig != nil {
			return c, fmt.Errorf("cnpgconnect-go: insecure discovery cannot carry TLS configuration or credentials")
		}
	} else {
		if strings.ContainsAny(c.Token, "\r\n\t ") {
			return c, fmt.Errorf("cnpgconnect-go: bearer token must not contain whitespace")
		}
		if c.TLSConfig == nil {
			c.TLSConfig = &tls.Config{}
		} else {
			c.TLSConfig = c.TLSConfig.Clone()
		}
		if c.TLSConfig.InsecureSkipVerify {
			return c, fmt.Errorf("cnpgconnect-go: discovery TLS verification must be enabled")
		}
		if c.TLSConfig.MinVersion < tls.VersionTLS12 {
			c.TLSConfig.MinVersion = tls.VersionTLS12
		}
	}
	return c, nil
}

func resourceName(s string) bool {
	if len(s) == 0 || len(s) > 253 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '.') {
			return false
		}
	}
	for _, part := range strings.Split(s, ".") {
		if len(part) == 0 || len(part) > 63 || part[0] == '-' || part[len(part)-1] == '-' {
			return false
		}
	}
	return true
}
