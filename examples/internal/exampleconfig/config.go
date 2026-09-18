// Package exampleconfig loads the configuration shared by the runnable examples.
package exampleconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	native "github.com/jackc/pgx/v5/pgxpool"
	"github.com/nakiner/cnpgconnect-go"
	"github.com/nakiner/cnpgconnect-go/pgxpool"
)

func Load() (pgxpool.Config, error) {
	var cfg pgxpool.Config
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		return cfg, errors.New("PG_DSN is required")
	}
	postgres, err := native.ParseConfig(dsn)
	if err != nil {
		return cfg, fmt.Errorf("parse PG_DSN: %w", err)
	}
	tokenFile := os.Getenv("CNPG_DISCOVERY_TOKEN_FILE")
	if tokenFile == "" {
		return cfg, errors.New("CNPG_DISCOVERY_TOKEN_FILE is required")
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return cfg, fmt.Errorf("read discovery token: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: os.Getenv("CNPG_DISCOVERY_SERVER_NAME"),
	}
	if caFile := os.Getenv("CNPG_DISCOVERY_CA"); caFile != "" {
		ca, err := os.ReadFile(caFile)
		if err != nil {
			return cfg, fmt.Errorf("read discovery CA: %w", err)
		}
		tlsConfig.RootCAs = x509.NewCertPool()
		if !tlsConfig.RootCAs.AppendCertsFromPEM(ca) {
			return cfg, errors.New("discovery CA contains no certificates")
		}
	}
	return pgxpool.Config{
		Discovery: cnpgconnectgo.Config{
			Address:   os.Getenv("CNPG_DISCOVERY_ADDRESS"),
			Namespace: os.Getenv("CNPG_NAMESPACE"),
			Cluster:   os.Getenv("CNPG_CLUSTER"),
			Network:   os.Getenv("CNPG_NETWORK"),
			Token:     strings.TrimSpace(string(token)),
			TLSConfig: tlsConfig,
		},
		ConnConfig: postgres,
	}, nil
}
