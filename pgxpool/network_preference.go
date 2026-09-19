package pgxpool

import (
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nakiner/cnpgconnect-go"
)

const (
	networkPreferenceTTL  = time.Minute
	maxNetworkPreferences = 128
)

// Preferences are pool-local and keyed by the entire routing identity, including
// generation, endpoints and CA. They affect address order, never eligibility.
func (p *Pool) preferredTarget(target cnpgconnectgo.Target, now time.Time) cnpgconnectgo.Target {
	p.mu.Lock()
	defer p.mu.Unlock()
	if until, ok := p.preferences[target]; ok {
		if now.Before(until) {
			target.Endpoint, target.FallbackEndpoint = target.FallbackEndpoint, target.Endpoint
		} else {
			delete(p.preferences, target)
		}
	}
	return target
}

func (p *Pool) rememberPath(target cnpgconnectgo.Target, path uint8, now time.Time) {
	if target.FallbackEndpoint.Host == "" || path == 0 || path == 3 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if path == 1 {
		delete(p.preferences, target)
		return
	}
	if now.Before(p.preferences[target]) {
		return
	} // Do not slide the re-probe deadline.
	if p.preferences == nil {
		p.preferences = make(map[cnpgconnectgo.Target]time.Time)
	}
	var oldest cnpgconnectgo.Target
	var oldestTime time.Time
	for key, until := range p.preferences {
		if !now.Before(until) {
			delete(p.preferences, key)
			continue
		}
		if oldestTime.IsZero() || until.Before(oldestTime) {
			oldest, oldestTime = key, until
		}
	}
	if len(p.preferences) >= maxNetworkPreferences {
		delete(p.preferences, oldest)
	}
	p.preferences[target] = now.Add(networkPreferenceTTL)
}

// pgx resolves every advertised host before trying addresses. Record the dialed
// address, not net.Conn.RemoteAddr: custom transports/proxies can change the
// latter. Ambiguous shared addresses do not teach a preference. TLS, role and
// topology checks must succeed before the caller commits this observation.
type dialPaths struct {
	mu        sync.Mutex
	endpoints [2]cnpgconnectgo.Endpoint
	routes    map[string]uint8
	last      uint8
}

func (d *dialPaths) resolved(host string, addresses []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.routes == nil {
		d.routes = make(map[string]uint8)
	}
	for i, endpoint := range d.endpoints {
		if endpoint.Host != host {
			continue
		}
		for _, address := range addresses {
			host, port := address, endpoint.Port
			if suppliedHost, suppliedPort, err := net.SplitHostPort(address); err == nil {
				parsed, err := strconv.ParseUint(suppliedPort, 10, 16)
				if err != nil {
					continue // pgx rejects this answer before dialing it.
				}
				host, port = suppliedHost, uint16(parsed)
			}
			_, dialAddress := pgconn.NetworkAddress(host, port)
			d.routes[dialAddress] |= 1 << i
		}
	}
}
func (d *dialPaths) dialed(address string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.last = d.routes[address]
}
func (d *dialPaths) chosen() uint8 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.last
}
