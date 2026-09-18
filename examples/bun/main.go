package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nakiner/cnpgconnect-go/examples/internal/exampleconfig"
	"github.com/nakiner/cnpgconnect-go/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	config := exampleconfig.Load()
	sqldb, err := stdlib.Open(context.Background(), config)
	if err != nil {
		return err
	}
	db := bun.NewDB(sqldb, pgdialect.New())
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var server string
	if err := db.NewSelect().ColumnExpr("inet_server_addr()::text").Scan(ctx, &server); err != nil {
		return err
	}
	fmt.Println("Connected to", server)
	return nil
}
