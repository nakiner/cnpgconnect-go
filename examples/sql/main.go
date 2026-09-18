package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nakiner/cnpgconnect-go/examples/internal/exampleconfig"
	"github.com/nakiner/cnpgconnect-go/stdlib"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	config := exampleconfig.Load()
	db, err := stdlib.Open(context.Background(), config)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var server string
	if err := db.QueryRowContext(ctx, "select inet_server_addr()::text").Scan(&server); err != nil {
		return err
	}
	fmt.Println("Connected to", server)
	return nil
}
