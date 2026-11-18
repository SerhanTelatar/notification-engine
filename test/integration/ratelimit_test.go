//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	redisadapter "github.com/SerhanTelatar/notification-engine/internal/adapters/redis"
	"github.com/SerhanTelatar/notification-engine/internal/app"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// timerTolerance absorbs Go timer jitter: slots are reserved exactly 1/limit apart, but a sleeping
// goroutine wakes a few milliseconds late. A window of (1s - tolerance) is still strictly checked.
const timerTolerance = 15 * time.Millisecond

func newRedisClient(t testing.TB, addr string) *goredis.Client {
	t.Helper()
	c := goredis.NewClient(&goredis.Options{Addr: addr, DialTimeout: 500 * time.Millisecond,
		ReadTimeout: 500 * time.Millisecond, WriteTimeout: 500 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func flushLimiterKeys(t testing.TB) {
	t.Helper()
	c := newRedisClient(t, env.RedisAddr)
	if err := c.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}

// maxInWindow returns the most events in any half-open window [t, t+w).
func maxInWindow(times []time.Time, w time.Duration) int {
	sorted := append([]time.Time(nil), times...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Before(sorted[j]) })
	best, lo := 0, 0
	for hi := range sorted {
		for sorted[hi].Sub(sorted[lo]) >= w {
			lo++
		}
		best = max(best, hi-lo+1)
	}
	return best
}

// acquireFor runs workers that take permits until ctx ends and returns the grant times.
func acquireFor(ctx context.Context, limiters []*redisadapter.RateLimiter, perLimiter int, ch domain.Channel) []time.Time {
	var (
		mu    sync.Mutex
		times []time.Time
		wg    sync.WaitGroup
	)
	for _, l := range limiters {
		for range perLimiter {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil {
					if err := l.Acquire(ctx, ch); err != nil {
						return
					}
					mu.Lock()
					times = append(times, time.Now())
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	return times
}

func TestRateLimiterIsSharedAcrossInstances(t *testing.T) {
	flushLimiterKeys(t)
	// Two "workers", each with its own client, sharing one limit of 20/s.
	limiters := []*redisadapter.RateLimiter{
		redisadapter.NewRateLimiter(newRedisClient(t, env.RedisAddr), 20),
		redisadapter.NewRateLimiter(newRedisClient(t, env.RedisAddr), 20),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	times := acquireFor(ctx, limiters, 5, domain.ChannelSMS)

	peak := maxInWindow(times, time.Second-timerTolerance)
	t.Logf("%d permits in 3 s across 2 instances; peak %d in any 1 s window", len(times), peak)
	if peak > 20 {
		t.Fatalf("peak %d permits in a 1 s window, limit 20 (SC-003)", peak)
	}
	if len(times) < 54 {
		t.Fatalf("%d permits in 3 s, want >= 54 (>= 90%% of the limit, SC-003 lower bound)", len(times))
	}
}

func TestRateLimiterChannelsAreIndependent(t *testing.T) {
	flushLimiterKeys(t)
	l := redisadapter.NewRateLimiter(newRedisClient(t, env.RedisAddr), 10)
	// Book a second of sms slots.
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = l.Acquire(ctx, domain.ChannelSMS)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Acquire(ctx, domain.ChannelEmail); err != nil {
		t.Fatal(err)
	}
	if waited := time.Since(start); waited > 50*time.Millisecond {
		t.Fatalf("email waited %v behind an sms backlog: channels must be independent", waited)
	}
	wg.Wait()
}

func TestRateLimiterRespectsTheCallerDeadline(t *testing.T) {
	flushLimiterKeys(t)
	l := redisadapter.NewRateLimiter(newRedisClient(t, env.RedisAddr), 1) // one slot per second
	ctx := context.Background()
	if err := l.Acquire(ctx, domain.ChannelPush); err != nil {
		t.Fatal(err)
	}
	// The next slot is ~1 s away: a 200 ms deadline must not reserve it, and must return in time.
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := l.Acquire(short, domain.ChannelPush); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > 400*time.Millisecond {
		t.Fatal("Acquire overran the caller's deadline")
	}
	// The unreserved slot is still free for a patient caller.
	patient, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	if err := l.Acquire(patient, domain.ChannelPush); err != nil {
		t.Fatalf("slot was wasted by the abandoned caller: %v", err)
	}
}

func TestRateLimiterFailsClosed(t *testing.T) {
	// Nothing listens on this port: an unreachable Redis must deny, not allow.
	l := redisadapter.NewRateLimiter(newRedisClient(t, "127.0.0.1:1"), 100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Acquire(ctx, domain.ChannelSMS); err == nil {
		t.Fatal("Acquire succeeded without Redis: the limiter must fail closed")
	}
}

// toggleProxy forwards TCP to Redis and can be cut and restored on the same address, simulating a
// Redis outage without disturbing the shared container.
type toggleProxy struct {
	t      testing.TB
	target string
	addr   string
	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
}

func newToggleProxy(t testing.TB, target string) *toggleProxy {
	t.Helper()
	p := &toggleProxy{t: t, target: target, addr: "127.0.0.1:0", conns: map[net.Conn]struct{}{}}
	p.Start()
	t.Cleanup(p.Stop)
	return p
}

func (p *toggleProxy) Start() {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", p.addr)
	if err != nil {
		p.t.Fatalf("proxy listen: %v", err)
	}
	p.mu.Lock()
	p.ln, p.addr = ln, ln.Addr().String()
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.pipe(c)
		}
	}()
}

func (p *toggleProxy) pipe(c net.Conn) {
	up, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.conns[c], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	_, _ = io.Copy(c, up)
	_ = c.Close()
}

// Stop closes the listener and every open connection.
func (p *toggleProxy) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

func TestRateLimiterOutageRequeuesThenRecovers(t *testing.T) {
	flushLimiterKeys(t)
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	proxy := newToggleProxy(t, env.RedisAddr)
	limiter := redisadapter.NewRateLimiter(newRedisClient(t, proxy.addr), 100)

	cfg := defaultPipeline(mock.SendURL())
	cfg.limiter, cfg.limiterMaxWait = limiter, 2*time.Second
	p := startPipeline(t, cfg)

	proxy.Stop() // Redis unreachable
	n := p.create(t, app.CreateRequest{})
	waitFor(t, 10*time.Second, "a limiter timeout", func() bool { return p.metrics.get("limiter_timeout") >= 1 })
	if st, _ := statusOf(t, n.ID); st != "queued" || len(mock.Received()) != 0 {
		t.Fatalf("status %s, provider calls %d: with Redis down nothing may be sent (fail closed)", st, len(mock.Received()))
	}

	proxy.Start() // Redis back on the same address
	waitFor(t, 15*time.Second, "sent after Redis recovers", statusIs(t, n.ID, "sent"))
	if got := len(mock.Received()); got != 1 {
		t.Fatalf("provider calls %d, want 1", got)
	}
}

func TestRateLimitEndToEndTwoWorkers(t *testing.T) {
	const (
		limit = 20
		burst = 80
	)
	flushLimiterKeys(t)
	env.Truncate(t)
	resetBroker(t)
	mock := testenv.NewMockProvider(t)
	for range 2 { // two worker processes, each with its own Redis client
		cfg := defaultPipeline(mock.SendURL())
		cfg.limiter = redisadapter.NewRateLimiter(newRedisClient(t, env.RedisAddr), limit)
		cfg.limiterMaxWait = 10 * time.Second
		startPipeline(t, cfg)
	}
	insertBurst(t, domain.ChannelSMS, "+905551234567", burst)

	start := time.Now()
	waitFor(t, 30*time.Second, "burst delivered", func() bool { return countStatus(t, "sent") == burst })
	elapsed := time.Since(start)

	var arrivals []time.Time
	for _, r := range mock.Received() {
		arrivals = append(arrivals, r.At)
	}
	// At the provider, network and DB latency add a little jitter on top of exactly spaced permits.
	peak := maxInWindow(arrivals, time.Second-timerTolerance)
	rate := float64(burst) / elapsed.Seconds()
	t.Logf("%d sms via 2 workers in %v (%.1f/s); peak %d at the provider in any 1 s window", burst, elapsed.Round(time.Millisecond), rate, peak)
	if peak > limit+1 {
		t.Fatalf("provider saw %d in a 1 s window, limit %d (+1 for transit jitter)", peak, limit)
	}
	if len(arrivals) != burst {
		t.Fatalf("provider calls %d, want %d", len(arrivals), burst)
	}
	if minDuration := time.Duration(float64(burst-limit) / limit * float64(time.Second)); elapsed < minDuration {
		t.Fatalf("drained in %v, faster than the limit allows (>= %v)", elapsed, minDuration)
	}
}
