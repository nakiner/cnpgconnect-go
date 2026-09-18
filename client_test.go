package cnpgconnectgo

import (
	"context"
	"errors"
	"net"
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
