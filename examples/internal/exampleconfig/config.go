// Package exampleconfig loads the configuration shared by the runnable examples.
package exampleconfig

import (
	"os"

	"github.com/nakiner/cnpgconnect-go/pgxpool"
)

func Load() pgxpool.Config {
	return pgxpool.Config{
		Address:   os.Getenv("CNPG_DISCOVERY_ADDRESS"),
		Namespace: os.Getenv("CNPG_NAMESPACE"),
		Cluster:   os.Getenv("CNPG_CLUSTER"),
		Username:  os.Getenv("PGUSER"),
		Password:  os.Getenv("PGPASSWORD"),
	}
}
