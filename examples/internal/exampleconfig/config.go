// Package exampleconfig loads the configuration shared by the runnable examples.
package exampleconfig

import (
	"errors"
	"os"
	"strconv"

	"github.com/nakiner/cnpgconnect-go/pgxpool"
)

func Load() (pgxpool.Config, error) {
	config := pgxpool.Config{
		Address:   os.Getenv("CNPG_DISCOVERY_ADDRESS"),
		Namespace: os.Getenv("CNPG_NAMESPACE"),
		Cluster:   os.Getenv("CNPG_CLUSTER"),
		Username:  os.Getenv("PGUSER"),
		Password:  os.Getenv("PGPASSWORD"),
	}
	if value := os.Getenv("CNPG_DISCOVERY_INSECURE"); value != "" {
		insecure, err := strconv.ParseBool(value)
		if err != nil {
			return pgxpool.Config{}, errors.New("CNPG_DISCOVERY_INSECURE must be a boolean")
		}
		config.Discovery.Insecure = insecure
	}
	return config, nil
}
