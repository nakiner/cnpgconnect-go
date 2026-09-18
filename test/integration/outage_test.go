//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	native "github.com/jackc/pgx/v5/pgxpool"
	"github.com/nakiner/cnpgconnect-go"
	managed "github.com/nakiner/cnpgconnect-go/pgxpool"
	"github.com/nakiner/cnpgconnect-go/stdlib"
)

func TestLiveDiscoveryOutage(t *testing.T) {
	if os.Getenv("CNPGCONNECT_GO_E2E_DISCOVERY_OUTAGE") != "1" {
		t.Skip("opt in with CNPGCONNECT_GO_E2E_DISCOVERY_OUTAGE=1")
	}
	kubeconfig := os.Getenv("CNPGCONNECT_GO_E2E_KUBECONFIG")
	if kubeconfig == "" {
		t.Fatal("CNPGCONNECT_GO_E2E_KUBECONFIG is required for the outage test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	h := &harness{kubeconfig: kubeconfig}
	if err := h.guard(ctx); err != nil {
		t.Fatal(err)
	}
	initial, err := h.cluster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.uid = initial.Metadata.UID
	if h.uid == "" || initial.Metadata.Name != clusterName || initial.Metadata.Namespace != namespace {
		t.Fatal("fixed integration Cluster has no valid identity")
	}
	deployment, err := h.discoveryDeployment(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Metadata.UID == "" || deployment.Spec.Replicas < 1 || len(deployment.Spec.Selector.MatchLabels) == 0 {
		t.Fatal("discovery Deployment must have a UID, positive replica count, and a Pod selector")
	}
	ttl, err := deployment.snapshotTTL()
	if err != nil {
		t.Fatal(err)
	}
	config := h.configuration(t, ctx)
	startupCtx, startupCancel := context.WithTimeout(ctx, 45*time.Second)
	defer startupCancel()
	discovery, err := cnpgconnectgo.New(startupCtx, config.Discovery)
	if err != nil {
		t.Fatal(err)
	}
	defer discovery.Close()
	writer, err := managed.Open(startupCtx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	db, err := stdlib.Open(startupCtx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	stmt, err := db.PrepareContext(startupCtx, serverQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	before := waitWriters(t, ctx, writer, db, stmt, "", "primary before discovery outage")
	primary, err := discovery.Resolve(startupCtx, cnpgconnectgo.Policy{Role: cnpgconnectgo.Primary})
	if err != nil {
		t.Fatal(err)
	}
	// A direct control connection establishes that PostgreSQL itself remains
	// reachable when the managed adapters reject expired discovery information.
	directConfig := config.ConnConfig.Copy()
	directConfig.ConnConfig.Host = primary.Endpoint.Host
	directConfig.ConnConfig.Port = primary.Endpoint.Port
	directConfig.ConnConfig.TLSConfig.ServerName = primary.Endpoint.ServerName
	directConfig.ConnConfig.Fallbacks = nil
	direct, err := native.NewWithConfig(startupCtx, directConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	if err := direct.Ping(startupCtx); err != nil {
		t.Fatal(err)
	}
	startupCancel()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := h.scaleDiscovery(cleanupCtx, deployment.Metadata.UID, deployment.Spec.Replicas); err != nil {
			t.Errorf("restore discovery Deployment replicas: %v", err)
		}
	})
	if err := h.scaleDiscovery(ctx, deployment.Metadata.UID, 0); err != nil {
		t.Fatal(err)
	}
	if err := h.waitDiscoveryStopped(ctx, deployment); err != nil {
		t.Fatal(err)
	}
	// Once every producer Pod is gone, no client can receive a newer snapshot.
	// Waiting the configured TTL plus a margin covers independently received
	// final observations on the three discovery streams.
	if err := pause(ctx, ttl+2*time.Second); err != nil {
		t.Fatal(err)
	}
	state := discovery.Status()
	if state.Ready || time.Now().Before(state.ValidUntil) {
		t.Fatal("discovery still advertises fresh topology after producer shutdown and TTL expiry")
	}
	controlCtx, controlCancel := context.WithTimeout(ctx, 3*time.Second)
	if err := direct.Ping(controlCtx); err != nil {
		controlCancel()
		t.Fatalf("PostgreSQL control connection is unhealthy: %v", err)
	}
	controlCancel()
	var address string
	var recovery bool
	for _, probe := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"pgx", func(ctx context.Context) error { return writer.QueryRow(ctx, serverQuery).Scan(&address, &recovery) }},
		{"sql", func(ctx context.Context) error { return db.QueryRowContext(ctx, serverQuery).Scan(&address, &recovery) }},
		{"prepared_sql", func(ctx context.Context) error { return stmt.QueryRowContext(ctx).Scan(&address, &recovery) }},
	} {
		probeCtx, probeCancel := context.WithTimeout(ctx, time.Second)
		err := probe.run(probeCtx)
		probeCancel()
		if err == nil {
			t.Fatalf("%s executed using expired topology", probe.name)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s did not wait for fresh routing until its deadline: %v", probe.name, err)
		}
	}
	t.Log("pgx, SQL, and prepared SQL rejected expired routes while PostgreSQL remained healthy")
	if err := h.scaleDiscovery(ctx, deployment.Metadata.UID, deployment.Spec.Replicas); err != nil {
		t.Fatal(err)
	}
	after := waitWriters(t, ctx, writer, db, stmt, "", "discovery restart")
	if before != after {
		t.Fatalf("discovery restart changed the PostgreSQL server: before=%s after=%s", before, after)
	}
	t.Log("same handles resumed on the same primary after discovery restarted")
}

type deployment struct {
	Metadata metadata
	Spec     struct {
		Replicas int32
		Selector struct{ MatchLabels map[string]string }
		Template struct {
			Spec struct{ Containers []struct{ Args []string } }
		}
	}
}

func (d deployment) snapshotTTL() (time.Duration, error) {
	for _, container := range d.Spec.Template.Spec.Containers {
		for i, arg := range container.Args {
			value, found := strings.CutPrefix(arg, "--ttl=")
			if arg == "--ttl" && i+1 < len(container.Args) {
				value, found = container.Args[i+1], true
			}
			if found {
				ttl, err := time.ParseDuration(value)
				if err != nil || ttl <= 0 || ttl > time.Minute {
					return 0, errors.New("outage test requires a positive discovery --ttl no greater than one minute")
				}
				return ttl, nil
			}
		}
	}
	return 0, errors.New("outage test requires an explicit --ttl argument on the discovery Deployment")
}

func (h *harness) discoveryDeployment(ctx context.Context) (deployment, error) {
	var value deployment
	if _, err := h.guardedCluster(ctx); err != nil {
		return value, err
	}
	data, err := h.command(ctx, nil, "-n", "cnpg-system", "get", "deployment", "connect", "-o", "json")
	if err == nil {
		err = json.Unmarshal(data, &value)
	}
	if err == nil && (value.Metadata.Name != "connect" || value.Metadata.Namespace != "cnpg-system") {
		err = errors.New("refusing access to an unexpected discovery Deployment")
	}
	return value, err
}

func (h *harness) scaleDiscovery(ctx context.Context, uid string, replicas int32) error {
	for attempt := 0; attempt < 5; attempt++ {
		current, err := h.discoveryDeployment(ctx)
		if err != nil {
			return err
		}
		if current.Metadata.UID != uid {
			return errors.New("refusing mutation: discovery Deployment identity changed")
		}
		if current.Spec.Replicas == replicas {
			return nil
		}
		patch, err := json.Marshal([]map[string]any{
			{"op": "test", "path": "/metadata/uid", "value": uid},
			{"op": "test", "path": "/metadata/resourceVersion", "value": current.Metadata.ResourceVersion},
			{"op": "test", "path": "/spec/replicas", "value": current.Spec.Replicas},
			{"op": "replace", "path": "/spec/replicas", "value": replicas},
		})
		if err != nil {
			return err
		}
		_, err = h.command(ctx, nil, "-n", "cnpg-system", "patch", "deployment", "connect", "--type=json", "-p", string(patch))
		if err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "Conflict") && !strings.Contains(err.Error(), "test failed") && !strings.Contains(err.Error(), "request is invalid") {
			return err
		}
		if err := pause(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	return errors.New("discovery Deployment scale conflicted repeatedly")
}

func (h *harness) waitDiscoveryStopped(parent context.Context, expected deployment) error {
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	var selectors []string
	for key, value := range expected.Spec.Selector.MatchLabels {
		selectors = append(selectors, key+"="+value)
	}
	sort.Strings(selectors)
	for {
		current, err := h.discoveryDeployment(ctx)
		if err != nil {
			return err
		}
		if current.Metadata.UID != expected.Metadata.UID || current.Spec.Replicas != 0 {
			return errors.New("discovery Deployment changed while waiting for shutdown")
		}
		data, err := h.command(ctx, nil, "-n", "cnpg-system", "get", "pods", "-l", strings.Join(selectors, ","), "-o", "json")
		if err != nil {
			return err
		}
		var pods struct{ Items []json.RawMessage }
		if err := json.Unmarshal(data, &pods); err != nil {
			return err
		}
		if len(pods.Items) == 0 {
			return nil
		}
		if err := pause(ctx, time.Second); err != nil {
			return fmt.Errorf("wait for discovery Pods to stop: %w", err)
		}
	}
}
