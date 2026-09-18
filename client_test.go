package cnpgconnectgo

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type watchFunc func(grpc.ServerStreamingServer[connectv1.Snapshot]) error

type watchServer struct {
	connectv1.UnimplementedTopologyServiceServer
	watch watchFunc
}

func (s watchServer) WatchTopology(req *connectv1.WatchTopologyRequest, stream grpc.ServerStreamingServer[connectv1.Snapshot]) error {
	if req.Namespace != "db" || req.Name != "app" {
		return status.Error(codes.InvalidArgument, "wrong cluster")
	}
	return s.watch(stream)
}

func startServer(t *testing.T, f watchFunc) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	connectv1.RegisterTopologyServiceServer(server, watchServer{watch: f})
	go server.Serve(ln)
	t.Cleanup(func() { server.Stop(); _ = ln.Close() })
	return ln.Addr().String()
}
func localConfig(address string) Config {
	return Config{Address: address, Namespace: "db", Cluster: "app", Insecure: true, StartupTimeout: time.Second, ReconnectMin: 10 * time.Millisecond, ReconnectMax: 30 * time.Millisecond}
}

func TestStreamReconnectAndClose(t *testing.T) {
	var calls atomic.Int32
	address := startServer(t, func(s grpc.ServerStreamingServer[connectv1.Snapshot]) error {
		n := calls.Add(1)
		snap := snapshot(time.Now())
		if n > 1 {
			snap.Cluster.Uid = "cluster-recreated"
		}
		if err := s.Send(snap); err != nil {
			return err
		}
		if n == 1 {
			return status.Error(codes.Unavailable, "restart")
		}
		<-s.Context().Done()
		return s.Context().Err()
	})
	client, err := New(context.Background(), localConfig(address))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	deadline := time.Now().Add(time.Second)
	for client.Status().Members[0].ClusterUID != "cluster-recreated" {
		if time.Now().After(deadline) {
			t.Fatal("stream did not reconnect")
		}
		time.Sleep(time.Millisecond)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := client.Close(); err != nil {
				t.Errorf("close: %v", err)
			}
		}()
	}
	wg.Wait()
	if _, err := client.Resolve(context.Background(), Policy{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("resolve closed: %v", err)
	}
}

func TestRapidTopologyUpdatesDoNotWaitForSubscribers(t *testing.T) {
	start := make(chan struct{})
	var calls atomic.Int32
	const updates = 64
	address := startServer(t, func(stream grpc.ServerStreamingServer[connectv1.Snapshot]) error {
		calls.Add(1)
		if err := stream.Send(transitionSnapshot(time.Now(), Primary)); err != nil {
			return err
		}
		select {
		case <-start:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		for i := 1; i <= updates; i++ {
			role := Primary
			if i%2 == 0 {
				role = SyncReplica
			}
			snapshot := transitionSnapshot(time.Now(), role)
			snapshot.Revision = strconv.Itoa(i)
			if i%3 == 0 {
				snapshot.Available = false
				snapshot.Reason = "primary_transition"
			}
			if err := stream.Send(snapshot); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})
	cfg := localConfig(address)
	// A stream reconnect would exceed the deadline below. Updates on an open
	// stream must not inherit transport backoff or wait for adapter callbacks.
	cfg.ReconnectMin, cfg.ReconnectMax = 5*time.Second, 5*time.Second
	client, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	oldPrimary, err := client.Resolve(context.Background(), Policy{})
	if err != nil {
		t.Fatal(err)
	}
	// Leave the subscription's initial notification unread to simulate a busy
	// pool adapter while promotions and temporary unavailable states arrive.
	_, unsubscribe := client.Subscribe()
	defer unsubscribe()
	close(start)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for client.Status().Revision != strconv.Itoa(updates) {
		select {
		case <-ctx.Done():
			t.Fatal("busy subscriber delayed the latest topology")
		case <-time.After(time.Millisecond):
		}
	}
	current, err := client.Resolve(ctx, Policy{})
	if err != nil || current.MemberID != "other" {
		t.Fatalf("latest primary = %+v, %v; want other", current, err)
	}
	if client.Valid(Policy{}, oldPrimary) {
		t.Fatal("superseded primary connection remains eligible")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("role changes restarted the discovery stream %d times", got-1)
	}
}

func TestStalledStreamExpiresAndReconnects(t *testing.T) {
	var calls atomic.Int32
	address := startServer(t, func(s grpc.ServerStreamingServer[connectv1.Snapshot]) error {
		calls.Add(1)
		snap := snapshot(time.Now())
		snap.ValidUntil = timestamppb.New(snap.ObservedAt.AsTime().Add(40 * time.Millisecond))
		if err := s.Send(snap); err != nil {
			return err
		}
		<-s.Context().Done()
		return s.Context().Err()
	})
	cfg := localConfig(address)
	cfg.MaxSnapshotTTL = 200 * time.Millisecond
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	target, err := c.Resolve(context.Background(), Policy{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(70 * time.Millisecond)
	if c.Valid(Policy{}, target) {
		t.Fatal("stalled stream renewed freshness")
	}
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("stalled stream not restarted")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStartupAuthenticationFailure(t *testing.T) {
	address := startServer(t, func(grpc.ServerStreamingServer[connectv1.Snapshot]) error {
		return status.Error(codes.Unauthenticated, "denied")
	})
	start := time.Now()
	_, err := New(context.Background(), localConfig(address))
	if status.Code(errors.Unwrap(err)) != codes.Unauthenticated {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("authentication failure was hidden until timeout")
	}
}

func TestConfigurationDoesNotAllowCredentialLeak(t *testing.T) {
	cfg := localConfig("localhost:1234")
	cfg.Token = "secret"
	if _, err := cfg.normalized(); err == nil {
		t.Fatal("accepted token over insecure transport")
	}
}

func TestDiscoveryDefaultsToVerifiedTLSWithoutToken(t *testing.T) {
	cfg, err := (Config{Address: "discovery.example.com:443", Namespace: "dev", Cluster: "rent"}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "" || cfg.TLSConfig == nil || cfg.TLSConfig.InsecureSkipVerify || cfg.TLSConfig.RootCAs != nil || cfg.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("default discovery does not use verified system-trust TLS without a token")
	}
}
