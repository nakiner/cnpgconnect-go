package cnpgconnectgo

import (
	"context"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
)

func acceptSnapshot(t *testing.T, c *Client, s *connectv1.Snapshot, now time.Time) {
	t.Helper()
	if err := c.accept(s, now); err != nil {
		t.Fatal(err)
	}
}

func resolveTarget(t *testing.T, c *Client, policy Policy) Target {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	target, err := c.Resolve(ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	return target
}
