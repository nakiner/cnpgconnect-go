//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nakiner/cnpgconnect-go"
	managed "github.com/nakiner/cnpgconnect-go/pgxpool"
	"github.com/nakiner/cnpgconnect-go/stdlib"
)

func TestLiveDiscoveryStall(t *testing.T) {
	if os.Getenv("CNPGCONNECT_GO_E2E_STALL") != "1" {
		t.Skip("opt in with CNPGCONNECT_GO_E2E_STALL=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	h := &harness{kubeconfig: os.Getenv("CNPGCONNECT_GO_E2E_KUBECONFIG")}
	if err := h.guard(ctx); err != nil {
		t.Fatal(err)
	}
	config := h.configuration(t, ctx)
	proxy := newStallProxy(t, config.Discovery.Address)
	config.Discovery.Address = proxy.listener.Addr().String()
	discovery, err := cnpgconnectgo.New(ctx, config.Discovery)
	if err != nil {
		t.Fatal(err)
	}
	defer discovery.Close()
	config.Resolver = discovery
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
	stmt, err := db.PrepareContext(ctx, serverQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	before := waitWriters(t, ctx, writer, db, stmt, "", "before stalled discovery")
	target, err := discovery.Resolve(ctx, cnpgconnectgo.Policy{Role: cnpgconnectgo.Primary})
	if err != nil {
		t.Fatal(err)
	}
	directConfig := config.ConnConfig.ConnConfig.Copy()
	directConfig.Host, directConfig.Port = target.Endpoint.Host, target.Endpoint.Port
	directConfig.TLSConfig.ServerName = target.Endpoint.ServerName
	directConfig.Fallbacks = nil
	direct, err := pgx.ConnectConfig(ctx, directConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close(context.Background())
	proxy.pause()
	// Keep TCP sockets open but withhold both directions: unlike a Deployment
	// outage this exercises receipt deadlines, not EOF-triggered reconnection.
	for discovery.Status().Ready {
		if err := pause(ctx, 100*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	for discovery.Statistics().WatchTimeouts == 0 {
		if err := pause(ctx, 10*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if err := direct.Ping(ctx); err != nil {
		t.Fatalf("PostgreSQL itself failed during discovery-only stall: %v", err)
	}
	for _, probe := range []func(context.Context) error{writer.Ping, db.PingContext} {
		attempt, stop := context.WithTimeout(ctx, 250*time.Millisecond)
		err := probe(attempt)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stalled discovery did not fail closed: %v", err)
		}
	}
	started := time.Now()
	proxy.resume()
	after := waitWriters(t, ctx, writer, db, stmt, "", "stalled stream recovery", started)
	if after != before {
		t.Fatal("discovery-only stall changed PostgreSQL identity")
	}
	t.Logf("open-socket stall expired safely; same pgx/SQL/prepared handles recovered; discovery statistics=%+v", discovery.Statistics())
}

// stallProxy buffers at most one fixed-size chunk per direction. It never
// drops encrypted bytes, terminates TLS, or retries application traffic.
type stallProxy struct {
	listener net.Listener
	mu       sync.Mutex
	gate     chan struct{}
	paused   bool
	ctx      context.Context
	group    sync.WaitGroup
}

func newStallProxy(t *testing.T, target string) *stallProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := &stallProxy{listener: listener, ctx: ctx, gate: make(chan struct{})}
	close(proxy.gate)
	proxy.group.Go(func() {
		for {
			downstream, err := listener.Accept()
			if err != nil {
				return
			}
			proxy.group.Go(func() {
				defer downstream.Close()
				upstream, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", target)
				if err != nil {
					return
				}
				defer upstream.Close()
				closeBoth := func() { _ = downstream.Close(); _ = upstream.Close() }
				stop := context.AfterFunc(ctx, closeBoth)
				defer stop()
				finished := make(chan struct{})
				go func() { defer close(finished); proxy.relay(upstream, downstream); closeBoth() }()
				proxy.relay(downstream, upstream)
				closeBoth()
				<-finished
			})
		}
	})
	t.Cleanup(func() { cancel(); _ = listener.Close(); proxy.group.Wait() })
	return proxy
}
func (p *stallProxy) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		p.paused = true
		p.gate = make(chan struct{})
	}
}
func (p *stallProxy) resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused {
		p.paused = false
		close(p.gate)
	}
}
func (p *stallProxy) relay(destination net.Conn, source net.Conn) {
	buffer := make([]byte, 32<<10)
	for {
		n, err := source.Read(buffer)
		if n > 0 {
			p.mu.Lock()
			gate := p.gate
			p.mu.Unlock()
			select {
			case <-gate:
			case <-p.ctx.Done():
				return
			}
			if _, writeErr := io.Copy(destination, bytes.NewReader(buffer[:n])); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
