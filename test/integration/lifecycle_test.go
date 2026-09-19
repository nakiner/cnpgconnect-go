//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	native "github.com/jackc/pgx/v5/pgxpool"
	"github.com/nakiner/cnpgconnect-go"
	managed "github.com/nakiner/cnpgconnect-go/pgxpool"
	"github.com/nakiner/cnpgconnect-go/stdlib"
)

const (
	expectedContext = "kind-cnpg-connect-test"
	namespace       = "databases"
	clusterName     = "app-db"
	// Port-forward reaches each Pod over loopback, so its IP alone is not a
	// distinct server identity. Include the server process start timestamp.
	serverQuery = "select inet_server_addr()::text || '@' || pg_postmaster_start_time()::text, pg_is_in_recovery()"
)

// TestLiveLifecycle mutates only the explicitly selected, fixed kind test
// Cluster. Deployments, local port-forwards, and environment teardown belong to
// the caller. Ordinary go test never includes this file.
func TestLiveLifecycle(t *testing.T) {
	kubeconfig := os.Getenv("CNPGCONNECT_GO_E2E_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("set CNPGCONNECT_GO_E2E_KUBECONFIG to opt into isolated kind integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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
	originalSync := initial.Spec.PostgreSQL.Synchronous
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := h.patchSync(cleanupCtx, originalSync); err != nil {
			t.Errorf("restore synchronous policy: %v", err)
		}
	})
	config := h.configuration(t, ctx)
	discovery, err := cnpgconnectgo.New(ctx, config.Discovery)
	if err != nil {
		t.Fatal(err)
	}
	defer discovery.Close()
	// The adapters own separate discovery clients, matching ordinary application
	// startup. This additional client is used only to observe promotion targets.
	writer, err := managed.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	db, err := stdlib.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Exercise the SQL ResetSession guard even when a caller adds a second
	// idle-connection layer. The documented default remains zero idle SQL conns.
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	stmt, err := db.PrepareContext(ctx, serverQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	oldAddress := waitWriters(t, ctx, writer, db, stmt, "", "initial primary")
	t.Logf("native pgx, SQL, and prepared SQL use primary %s", oldAddress)

	table := pgx.Identifier{fmt.Sprintf("cnpgconnect_go_e2e_%d", time.Now().UnixNano())}.Sanitize()
	if _, err := writer.Exec(ctx, "create table "+table+" (id integer primary key)"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := db.ExecContext(cleanupCtx, "drop table if exists "+table); err != nil {
			t.Logf("test-table cleanup failed: %v", err)
		}
	}()
	if _, err := writer.Exec(ctx, "insert into "+table+" values (1)"); err != nil {
		t.Fatal(err)
	}
	checkCount(t, ctx, db, table, 1)

	if err := h.patchSync(ctx, json.RawMessage(`{"method":"first","number":1,"maxStandbyNamesFromCluster":1,"dataDurability":"required"}`)); err != nil {
		t.Fatal(err)
	}
	phaseCtx, phaseCancel := context.WithTimeout(ctx, 3*time.Minute)
	_, err = discovery.Resolve(phaseCtx, cnpgconnectgo.Policy{Role: cnpgconnectgo.SyncReplica})
	phaseCancel()
	if err != nil {
		t.Fatalf("wait for synchronous replica: %v", err)
	}
	syncConfig := config
	syncConfig.Policy = cnpgconnectgo.Policy{Role: cnpgconnectgo.SyncReplica}
	syncConfig.Resolver = discovery
	reader, err := managed.Open(ctx, syncConfig)
	if err != nil {
		t.Fatal(err)
	}
	var replicaAddress string
	var inRecovery bool
	err = reader.QueryRow(ctx, serverQuery).Scan(&replicaAddress, &inRecovery)
	reader.Close() // Promotion must not retain a test-owned standby connection.
	if err != nil || !inRecovery || replicaAddress == oldAddress {
		t.Fatalf("synchronous replica query: address=%q recovery=%v err=%v", replicaAddress, inRecovery, err)
	}
	t.Logf("strict synchronous pool queried standby %s", replicaAddress)

	// The synchronous member can change between acquisitions. Bind the expected
	// promoted server to this exact target, independently of the pool check.
	phaseCtx, phaseCancel = context.WithTimeout(ctx, 3*time.Minute)
	target, err := discovery.Resolve(phaseCtx, cnpgconnectgo.Policy{Role: cnpgconnectgo.SyncReplica})
	phaseCancel()
	if err != nil {
		t.Fatalf("select synchronous promotion target: %v", err)
	}
	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	direct, err := connectTarget(probeCtx, config, target)
	if err != nil {
		probeCancel()
		t.Fatalf("connect to promotion target %s: %v", target.Name, err)
	}
	err = direct.QueryRow(probeCtx, serverQuery).Scan(&replicaAddress, &inRecovery)
	_ = direct.Close(probeCtx)
	probeCancel()
	if err != nil || !inRecovery || replicaAddress == oldAddress {
		t.Fatalf("promotion target %s: address=%q recovery=%v err=%v", target.Name, replicaAddress, inRecovery, err)
	}
	t.Logf("promoting synchronous target %s (%s)", target.Name, replicaAddress)

	recoveryStarted := time.Now()
	if err := h.promote(ctx, target); err != nil {
		t.Fatal(err)
	}
	newAddress := waitWriters(t, ctx, writer, db, stmt, oldAddress, "planned switchover", recoveryStarted)
	if newAddress != replicaAddress {
		t.Fatalf("promoted address %q, want selected standby %q", newAddress, replicaAddress)
	}
	// A write occurs once after the read-only readiness checks. The harness does
	// not replay failed writes, matching the library's execution contract.
	if _, err := db.ExecContext(ctx, "insert into "+table+" values (2)"); err != nil {
		t.Fatal(err)
	}
	checkCount(t, ctx, db, table, 2)
	t.Logf("same pgx, SQL, and prepared-statement handles recovered on %s", newAddress)

	if os.Getenv("CNPGCONNECT_GO_E2E_FAILOVER") == "1" {
		// Restore asynchronous mode first so deleting the primary cannot leave a
		// test write waiting for a standby whose local port-forward has stopped.
		if err := h.patchSync(ctx, nil); err != nil {
			t.Fatal(err)
		}
		phaseCtx, phaseCancel := context.WithTimeout(ctx, 3*time.Minute)
		primary, err := discovery.Resolve(phaseCtx, cnpgconnectgo.Policy{Role: cnpgconnectgo.Primary})
		phaseCancel()
		if err != nil {
			t.Fatal(err)
		}
		recoveryStarted = time.Now()
		if err := h.deletePrimary(ctx, primary); err != nil {
			t.Fatal(err)
		}
		failedOver := waitWriters(t, ctx, writer, db, stmt, newAddress, "primary Pod deletion", recoveryStarted)
		if _, err := writer.Exec(ctx, "insert into "+table+" values (3)"); err != nil {
			t.Fatal(err)
		}
		checkCount(t, ctx, db, table, 3)
		t.Logf("same handles recovered after primary deletion on %s", failedOver)
	}
}

func connectTarget(ctx context.Context, config managed.Config, target cnpgconnectgo.Target) (*pgx.Conn, error) {
	cc := config.ConnConfig.ConnConfig.Copy()
	cc.Host, cc.Port, cc.Fallbacks = target.Endpoint.Host, target.Endpoint.Port, nil
	if cc.TLSConfig != nil {
		cc.TLSConfig.ServerName = target.Endpoint.ServerName
	}
	cc.RuntimeParams["application_name"] += "_probe"
	return pgx.ConnectConfig(ctx, cc)
}

func waitWriters(t *testing.T, parent context.Context, writer *managed.Pool, db *sql.DB, stmt *sql.Stmt, previous, description string, transitionStart ...time.Time) string {
	t.Helper()
	started := time.Now()
	if len(transitionStart) != 0 {
		started = transitionStart[0]
	}
	budget := 3 * time.Minute
	if configured := os.Getenv("CNPGCONNECT_GO_E2E_RECOVERY_SLO"); configured != "" {
		var err error
		budget, err = time.ParseDuration(configured)
		if err != nil || budget <= 0 {
			t.Fatal("CNPGCONNECT_GO_E2E_RECOVERY_SLO must be a positive duration")
		}
	}
	ctx, cancel := context.WithDeadline(parent, started.Add(budget))
	defer cancel()
	var last error
	probes, failures := 0, 0
	for {
		probes++
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		var pgxAddress, sqlAddress, preparedAddress string
		var pgxRecovery, sqlRecovery, preparedRecovery bool
		err := writer.QueryRow(probeCtx, serverQuery).Scan(&pgxAddress, &pgxRecovery)
		if err == nil {
			err = db.QueryRowContext(probeCtx, serverQuery).Scan(&sqlAddress, &sqlRecovery)
		}
		if err == nil {
			err = stmt.QueryRowContext(probeCtx).Scan(&preparedAddress, &preparedRecovery)
		}
		probeCancel()
		if err == nil && !pgxRecovery && !sqlRecovery && !preparedRecovery && pgxAddress != previous && pgxAddress != "" && pgxAddress == sqlAddress && pgxAddress == preparedAddress {
			sample, _ := json.Marshal(map[string]any{"phase": description, "seconds": time.Since(started).Seconds(), "probes": probes, "failed_probes": failures})
			t.Logf("CNPG_RECOVERY_SAMPLE %s", sample)
			return pgxAddress
		}
		failures++
		last = err
		if err == nil {
			last = fmt.Errorf("addresses pgx=%s sql=%s prepared=%s; recovery=%v/%v/%v", pgxAddress, sqlAddress, preparedAddress, pgxRecovery, sqlRecovery, preparedRecovery)
		}
		if err := pause(ctx, time.Second); err != nil {
			t.Fatalf("wait for %s: %v; last=%v", description, err, last)
		}
	}
}

func checkCount(t *testing.T, parent context.Context, db *sql.DB, table string, expected int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	var count int
	if err := db.QueryRowContext(ctx, "select count(*) from "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("stored rows=%d, want %d", count, expected)
	}
}

type harness struct {
	kubeconfig string
	uid        string
}

type metadata struct {
	Name            string
	Namespace       string
	UID             string
	ResourceVersion string
	Labels          map[string]string
	OwnerReferences []struct{ UID, Kind, Name string }
}

type cluster struct {
	Metadata metadata
	Spec     struct {
		PostgreSQL struct{ Synchronous json.RawMessage }
	}
	Status struct{ CurrentPrimary, TargetPrimary string }
}

func (h *harness) command(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	args = append([]string{"--kubeconfig", h.kubeconfig, "--context", expectedContext}, args...)
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = bytes.NewReader(stdin)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("kubectl: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return output, nil
}

func (h *harness) guard(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", h.kubeconfig, "config", "current-context").Output()
	if err != nil {
		return fmt.Errorf("read explicit kubeconfig context: %w", err)
	}
	if strings.TrimSpace(string(output)) != expectedContext {
		return fmt.Errorf("refusing integration access outside %s", expectedContext)
	}
	return nil
}

func (h *harness) cluster(ctx context.Context) (cluster, error) {
	var result cluster
	data, err := h.command(ctx, nil, "-n", namespace, "get", "clusters.postgresql.cnpg.io", clusterName, "-o", "json")
	if err == nil {
		err = json.Unmarshal(data, &result)
	}
	return result, err
}

func (h *harness) guardedCluster(ctx context.Context) (cluster, error) {
	if err := h.guard(ctx); err != nil {
		return cluster{}, err
	}
	value, err := h.cluster(ctx)
	if err == nil && (value.Metadata.UID != h.uid || value.Metadata.Name != clusterName || value.Metadata.Namespace != namespace) {
		err = errors.New("refusing mutation: fixed integration Cluster identity changed")
	}
	return value, err
}

func (h *harness) patch(ctx context.Context, status bool, build func(cluster) map[string]any) error {
	for attempt := 0; attempt < 5; attempt++ {
		value, err := h.guardedCluster(ctx)
		if err != nil {
			return err
		}
		patch := build(value)
		patch["metadata"] = map[string]any{"resourceVersion": value.Metadata.ResourceVersion, "uid": h.uid}
		encoded, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		args := []string{"-n", namespace, "patch", "clusters.postgresql.cnpg.io", clusterName, "--type=merge", "-p", string(encoded)}
		if status {
			args = append(args, "--subresource=status")
		}
		_, err = h.command(ctx, nil, args...)
		if err == nil || !strings.Contains(err.Error(), "Conflict") {
			return err
		}
		if err := pause(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	return errors.New("Cluster patch conflicted repeatedly")
}

func (h *harness) patchSync(ctx context.Context, value json.RawMessage) error {
	return h.patch(ctx, false, func(cluster) map[string]any {
		return map[string]any{"spec": map[string]any{"postgresql": map[string]any{"synchronous": value}}}
	})
}

func (h *harness) verifyPod(ctx context.Context, target cnpgconnectgo.Target) error {
	if _, err := h.guardedCluster(ctx); err != nil {
		return err
	}
	if target.ClusterUID != h.uid || !strings.HasPrefix(target.Name, clusterName+"-") || target.MemberID == "" {
		return errors.New("refusing mutation: target is outside the fixed integration Cluster")
	}
	data, err := h.command(ctx, nil, "-n", namespace, "get", "pod", target.Name, "-o", "json")
	if err != nil {
		return err
	}
	var pod struct{ Metadata metadata }
	if err := json.Unmarshal(data, &pod); err != nil {
		return err
	}
	if pod.Metadata.UID != target.MemberID || pod.Metadata.Labels["cnpg.io/cluster"] != clusterName {
		return errors.New("refusing mutation: target Pod identity changed")
	}
	for _, owner := range pod.Metadata.OwnerReferences {
		if owner.UID == h.uid && owner.Name == clusterName && owner.Kind == "Cluster" {
			return nil
		}
	}
	return errors.New("refusing mutation: target Pod does not belong to the integration Cluster")
}

func (h *harness) promote(ctx context.Context, target cnpgconnectgo.Target) error {
	if err := h.verifyPod(ctx, target); err != nil {
		return err
	}
	// CNPG 1.30 kubectl-cnpg promote uses this optimistic status patch. The
	// operator remains responsible for fencing and promoting PostgreSQL.
	return h.patch(ctx, true, func(cluster) map[string]any {
		return map[string]any{"status": map[string]any{
			"targetPrimary": target.Name, "targetPrimaryTimestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"phase": "Switchover in progress", "phaseReason": "Switching over to " + target.Name,
		}}
	})
}

func (h *harness) deletePrimary(ctx context.Context, target cnpgconnectgo.Target) error {
	if err := h.verifyPod(ctx, target); err != nil {
		return err
	}
	current, err := h.guardedCluster(ctx)
	if err != nil {
		return err
	}
	if current.Status.CurrentPrimary != target.Name || current.Status.TargetPrimary != target.Name {
		return errors.New("refusing deletion: selected Pod is no longer the stable primary")
	}
	options, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 1,
		"preconditions": map[string]string{"uid": target.MemberID},
	})
	if err != nil {
		return err
	}
	_, err = h.command(ctx, options, "delete", "--raw", "/api/v1/namespaces/"+namespace+"/pods/"+target.Name, "-f", "-")
	return err
}

func (h *harness) secret(t *testing.T, ctx context.Context, ns, name string) map[string][]byte {
	t.Helper()
	data, err := h.command(ctx, nil, "-n", ns, "get", "secret", name, "-o", "json")
	if err != nil {
		t.Fatalf("read test Secret %s/%s: %v", ns, name, err)
	}
	var secret struct{ Data map[string][]byte }
	if err := json.Unmarshal(data, &secret); err != nil {
		t.Fatal("decode test Secret")
	}
	return secret.Data
}

func (h *harness) configuration(t *testing.T, ctx context.Context) managed.Config {
	t.Helper()
	token := h.secret(t, ctx, "cnpg-system", "cnpg-connect-auth")["token"]
	discoveryCA := h.secret(t, ctx, "cnpg-system", "connect-application-tls")["ca.crt"]
	postgresCA := h.secret(t, ctx, namespace, "app-db-ca")["ca.crt"]
	credentials := h.secret(t, ctx, namespace, "app-db-app")
	username := credentials["username"]
	if len(username) == 0 {
		username = credentials["user"]
	}
	if len(token) == 0 || len(username) == 0 || len(credentials["password"]) == 0 || len(credentials["dbname"]) == 0 {
		t.Fatal("test Secrets have missing credentials")
	}
	discoveryRoots := x509.NewCertPool()
	postgresRoots := x509.NewCertPool()
	if !discoveryRoots.AppendCertsFromPEM(discoveryCA) || !postgresRoots.AppendCertsFromPEM(postgresCA) {
		t.Fatal("test Secrets have invalid CA certificates")
	}
	postgres, err := native.ParseConfig("host=localhost dbname=postgres sslmode=verify-full")
	if err != nil {
		t.Fatal(err)
	}
	postgres.ConnConfig.User = string(username)
	postgres.ConnConfig.Password = string(credentials["password"])
	postgres.ConnConfig.Database = string(credentials["dbname"])
	postgres.ConnConfig.TLSConfig.RootCAs = postgresRoots
	postgres.ConnConfig.ConnectTimeout = 3 * time.Second
	postgres.MaxConns = 2
	address := os.Getenv("CNPGCONNECT_GO_E2E_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7443"
	}
	serverName := os.Getenv("CNPGCONNECT_GO_E2E_SERVER_NAME")
	if serverName == "" {
		serverName = "connect-api.cnpg-system.svc"
	}
	return managed.Config{
		Discovery: cnpgconnectgo.Config{
			Address: address, Namespace: namespace, Cluster: clusterName, Network: "external", Token: strings.TrimSpace(string(token)),
			TLSConfig: &tls.Config{RootCAs: discoveryRoots, ServerName: serverName, MinVersion: tls.VersionTLS12},
		},
		ConnConfig: postgres,
	}
}

func pause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
