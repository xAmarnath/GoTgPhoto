package gpmc

import (
	"context"
	"net"
	"sync"
	"time"
)

// dnsCacheDialer caches lookups and falls back to public resolvers: the system resolvers on
// flaky links intermittently answer "no such host", which used to fail whole downloads.
type dnsCacheDialer struct {
	d        net.Dialer
	fallback *net.Resolver

	mu    sync.Mutex
	cache map[string]dnsEntry
}

type dnsEntry struct {
	addrs []string
	at    time.Time
}

const dnsFreshTTL = 5 * time.Minute

func newDNSCacheDialer() *dnsCacheDialer {
	return &dnsCacheDialer{
		d:     net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second},
		cache: map[string]dnsEntry{},
		fallback: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				for _, s := range []string{"1.1.1.1:53", "8.8.8.8:53"} {
					if c, err := d.DialContext(ctx, "udp", s); err == nil {
						return c, nil
					}
				}
				return nil, net.ErrClosed
			},
		},
	}
}

func (c *dnsCacheDialer) lookup(ctx context.Context, host string) ([]string, error) {
	c.mu.Lock()
	e, ok := c.cache[host]
	c.mu.Unlock()
	if ok && time.Since(e.at) < dnsFreshTTL {
		return e.addrs, nil
	}
	var lastErr error
	for _, r := range []*net.Resolver{net.DefaultResolver, net.DefaultResolver, c.fallback} {
		lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ips, err := r.LookupHost(lctx, host)
		cancel()
		if err == nil && len(ips) > 0 {
			c.mu.Lock()
			c.cache[host] = dnsEntry{addrs: ips, at: time.Now()}
			c.mu.Unlock()
			return ips, nil
		}
		lastErr = err
	}
	if ok {
		return e.addrs, nil
	}
	return nil, lastErr
}

func (c *dnsCacheDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) != nil {
		return c.d.DialContext(ctx, network, addr)
	}
	ips, err := c.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, ip := range ips {
		conn, err := c.d.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}
