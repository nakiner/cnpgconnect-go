package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nakiner/cnpgconnect-go"
	"github.com/nakiner/cnpgconnect-go/examples/internal/exampleconfig"
	"github.com/nakiner/cnpgconnect-go/pgxpool"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := exampleconfig.Load()
	if err != nil {
		return err
	}

	// Share one discovery stream between the writer and reader.
	discoveryConfig := cfg.Discovery
	discoveryConfig.Address = cfg.Address
	discoveryConfig.Namespace = cfg.Namespace
	discoveryConfig.Cluster = cfg.Cluster
	discovery, err := cnpgconnectgo.New(ctx, discoveryConfig)
	if err != nil {
		return err
	}
	defer discovery.Close()
	cfg.Resolver = discovery

	cfg.Policy = cnpgconnectgo.Policy{Role: cnpgconnectgo.Primary}
	writer, err := pgxpool.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect primary: %w", err)
	}
	defer writer.Close()

	cfg.Policy = cnpgconnectgo.Policy{Role: cnpgconnectgo.SyncReplica}
	reader, err := pgxpool.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect sync replica: %w", err)
	}
	defer reader.Close()

	if err := printServer(ctx, "primary", writer); err != nil {
		return err
	}
	return printServer(ctx, "sync", reader)
}

func printServer(ctx context.Context, role string, pool *pgxpool.Pool) error {
	var address string
	var inRecovery bool
	err := pool.QueryRow(ctx, "select inet_server_addr()::text, pg_is_in_recovery()").Scan(&address, &inRecovery)
	if err != nil {
		return fmt.Errorf("query %s: %w", role, err)
	}
	fmt.Printf("role=%s server=%s recovery=%t\n", role, address, inRecovery)
	return nil
}
