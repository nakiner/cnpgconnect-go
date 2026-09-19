package cnpgconnectgo

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var errOutOfOrder = errors.New("cnpgconnect-go: older observation ignored")

// Client owns one reconnecting discovery stream and a shared routing view.
// The constructor context controls startup only; Close stops the client.
type Client struct {
	counters           clientCounters
	cfg                Config
	conn               *grpc.ClientConn
	topology           connectv1.TopologyServiceClient
	ctx                context.Context
	cancel             context.CancelFunc
	done               chan struct{}
	mu                 sync.Mutex
	state              Status
	clusterUID         string
	withdrawn          bool
	observationMembers map[string]struct{}
	closed             bool
	generation         uint64
	changed            chan struct{}
	subscribers        map[chan struct{}]struct{}
}

// New waits for the first fresh available topology. Credentials and discovery
// stay private to this client; adapters use the Resolver interface.
func New(ctx context.Context, config Config) (*Client, error) {
	cfg, err := config.normalized()
	if err != nil {
		return nil, err
	}
	var transport credentials.TransportCredentials
	if cfg.Insecure {
		transport = insecure.NewCredentials()
	} else {
		transport = credentials.NewTLS(cfg.TLSConfig)
	}
	conn, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(transport), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)))
	if err != nil {
		return nil, fmt.Errorf("cnpgconnect-go: discovery client: %w", err)
	}
	life, cancel := context.WithCancel(context.Background())
	c := &Client{
		cfg:         cfg,
		conn:        conn,
		topology:    connectv1.NewTopologyServiceClient(conn),
		ctx:         life,
		cancel:      cancel,
		done:        make(chan struct{}),
		changed:     make(chan struct{}),
		subscribers: make(map[chan struct{}]struct{}),
	}
	go c.run()
	startup, stop := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer stop()
	if err = c.waitReady(startup); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.state.Ready = false
		c.state.Connected = false
		c.state.LastError = ErrClosed
		c.generation++
		c.signalLocked()
		for ch := range c.subscribers {
			close(ch)
			delete(c.subscribers, ch)
		}
	}
	c.mu.Unlock()
	c.cancel()
	err := c.conn.Close()
	<-c.done
	if errors.Is(err, grpc.ErrClientConnClosing) {
		return nil
	}
	return err
}

func (c *Client) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(time.Now())
	s := c.state
	s.Members = append([]Target(nil), s.Members...)
	return s
}

func (c *Client) Subscribe() (<-chan struct{}, func()) {
	c.mu.Lock()
	ch := make(chan struct{}, 1)
	if c.closed {
		close(ch)
	} else {
		c.subscribers[ch] = struct{}{}
		ch <- struct{}{}
	}
	c.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if _, ok := c.subscribers[ch]; ok {
				delete(c.subscribers, ch)
				close(ch)
			}
		})
	}
}

func (c *Client) signalLocked() {
	c.signalWaitersLocked()
	for ch := range c.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (c *Client) waitReady(ctx context.Context) error {
	for {
		c.mu.Lock()
		c.expireLocked(time.Now())
		ready, closed, last, ch := c.state.Ready, c.closed, c.state.LastError, c.changed
		c.mu.Unlock()
		if closed {
			return ErrClosed
		}
		if ready {
			return nil
		}
		if code := status.Code(last); code == codes.Unauthenticated || code == codes.PermissionDenied || code == codes.InvalidArgument {
			return fmt.Errorf("cnpgconnect-go: discovery rejected request: %w", last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrUnavailable, ctx.Err())
		case <-ch:
		}
	}
}

func (c *Client) run() {
	defer close(c.done)
	var expiry sync.WaitGroup
	expiry.Go(c.runExpiry)
	defer expiry.Wait()
	delay := c.cfg.ReconnectMin
	for c.ctx.Err() == nil {
		started := time.Now()
		got, err := c.watch()
		if c.ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		c.state.Connected = false
		c.state.LastError = err
		code := status.Code(err)
		if code == codes.NotFound || code == codes.Unauthenticated || code == codes.PermissionDenied || code == codes.InvalidArgument {
			c.invalidateLocked()
		}
		c.signalWaitersLocked()
		c.mu.Unlock()
		// A flapping server can send its initial snapshot before closing every
		// stream. Require a sustained connection before resetting backoff so
		// those snapshots cannot keep a whole fleet retrying at the minimum.
		if got && time.Since(started) >= c.cfg.ReconnectMax {
			delay = c.cfg.ReconnectMin
		}
		// Jitter avoids synchronized reconnects after a shared load balancer outage.
		jitter := time.Duration(rand.Int64N(max(int64(delay/2), 1)))
		timer := time.NewTimer(delay/2 + jitter)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if delay > c.cfg.ReconnectMax/2 {
			delay = c.cfg.ReconnectMax
		} else {
			delay *= 2
		}
	}
}

// Expiry notifies idle pools even when a stream stays open but stalls. Arm one
// timer for the current deadline rather than waking every client periodically.
// c.changed also signals freshness-only refreshes, so a newer observation can
// extend or shorten the deadline without resetting healthy pool connections.
func (c *Client) runExpiry() {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		c.mu.Lock()
		c.expireLocked(time.Now())
		ready, until, changed := c.state.Ready, c.state.ValidUntil, c.changed
		c.mu.Unlock()
		var deadline <-chan time.Time
		if ready {
			timer.Reset(time.Until(until))
			deadline = timer.C
		}
		select {
		case <-c.ctx.Done():
			return
		case <-changed:
			timer.Stop()
		case <-deadline:
		}
	}
}

func (c *Client) watch() (bool, error) {
	c.counters.watchAttempts.Add(1)
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	watchdog := time.AfterFunc(c.watchTimeout(), func() {
		c.counters.watchTimeouts.Add(1)
		cancel()
	})
	defer watchdog.Stop()
	if c.cfg.Token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.cfg.Token)
	}
	stream, err := c.topology.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: c.cfg.Namespace, Name: c.cfg.Cluster})
	if err != nil {
		return false, err
	}
	got := false
	for {
		snapshot, err := stream.Recv()
		if err != nil {
			return got, err
		}
		if err = c.accept(snapshot, time.Now()); err != nil {
			if errors.Is(err, errOutOfOrder) {
				continue // Do not renew the stream watchdog on obsolete observations.
			}
			c.mu.Lock()
			c.state.LastError = err
			c.invalidateLocked()
			c.mu.Unlock()
			return got, err
		}
		got = true
		watchdog.Reset(c.watchTimeout())
	}
}

// Bound each watch by usable cached routing, even before its first response.
// Unavailable or already expired views use the idle timeout to avoid turning an
// unhealthy cluster into a tight reconnect loop. The accepted deadline includes
// replay and TTL caps, so wire timestamps cannot extend it here.
func (c *Client) watchTimeout() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if remaining := time.Until(c.state.ValidUntil); c.state.Ready && remaining > 0 {
		return min(c.cfg.MaxSnapshotTTL, remaining)
	}
	return c.cfg.MaxSnapshotTTL
}

// Wake startup/routing waiters without resetting healthy pools for transport
// changes. A valid observation may remain usable until its original expiry.
func (c *Client) signalWaitersLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *Client) invalidateLocked() {
	if c.state.Ready {
		c.state.Ready = false
		c.generation++
		c.signalLocked()
	}
}
func (c *Client) expireLocked(now time.Time) {
	if !now.Before(c.state.ValidUntil) {
		if c.state.Ready {
			c.counters.expirations.Add(1)
		}
		c.invalidateLocked()
	}
}
