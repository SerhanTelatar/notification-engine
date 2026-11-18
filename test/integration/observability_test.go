//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	goredis "github.com/redis/go-redis/v9"

	httpapi "github.com/SerhanTelatar/notification-engine/internal/adapters/http"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	redisadapter "github.com/SerhanTelatar/notification-engine/internal/adapters/redis"
	"github.com/SerhanTelatar/notification-engine/internal/platform/health"
	"github.com/SerhanTelatar/notification-engine/internal/platform/logger"
	"github.com/SerhanTelatar/notification-engine/internal/platform/metrics"
	"github.com/SerhanTelatar/notification-engine/test/integration/testenv"
)

// syncBuffer is a log sink shared by concurrent goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p) //nolint:wrapcheck // test sink
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type logLine struct {
	raw    string
	fields map[string]any
}

func parseLines(t testing.TB, s string) []logLine {
	t.Helper()
	var out []logLine
	sc := bufio.NewScanner(strings.NewReader(s))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var f map[string]any
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			t.Fatalf("log line is not JSON: %q", sc.Text())
		}
		out = append(out, logLine{raw: sc.Text(), fields: f})
	}
	return out
}

// SC-011: one correlation id ties the API request to every worker line for that notification,
// each line carries it exactly once, and the recipient and content never reach a log line, not
// even through a provider error body that echoes them.
func TestCorrelationIDOnEveryLineAndNoPII(t *testing.T) {
	env.Truncate(t)
	resetBroker(t)
	const recipient, content = "+905559871234", "your one-time code is 918273"
	mock := testenv.NewMockProvider(t)
	mock.SetScript(func(n int, r testenv.Received) testenv.Response {
		if n == 0 { // a provider error that echoes the request
			return testenv.Response{Status: http.StatusServiceUnavailable,
				Body: `{"error":"busy","to":"` + r.To + `","content":"` + r.Content + `"}`}
		}
		return testenv.Response{Status: http.StatusAccepted}
	})

	var apiOut, workerOut syncBuffer
	apiLog := logger.NewWithWriter(&apiOut, "api", "debug")
	workerLog := logger.NewWithWriter(&workerOut, "worker", "debug")
	cfg := defaultPipeline(mock.SendURL())
	cfg.log = workerLog
	startPipeline(t, cfg)
	api := newAPIWith(t, apiLog)

	res := postJSON(t, api.URL+"/api/v1/notifications",
		`{"recipient":"`+recipient+`","channel":"sms","content":"`+content+`"}`,
		map[string]string{"X-Correlation-ID": "t-1"})
	if res.code != http.StatusAccepted {
		t.Fatalf("create: %d %v", res.code, res.body)
	}
	id, _ := res.body["id"].(string)
	waitFor(t, 10*time.Second, "sent after one retry", func() bool { st, _ := statusOf(t, id); return st == "sent" })

	apiLines, workerLines := parseLines(t, apiOut.String()), parseLines(t, workerOut.String())
	if len(apiLines) == 0 {
		t.Fatal("no api log lines")
	}
	for _, l := range apiLines {
		if l.fields["correlation_id"] != "t-1" {
			t.Errorf("api line without the correlation id: %s", l.raw)
		}
	}
	msgs := map[string]bool{}
	for _, l := range workerLines {
		if l.fields["notification_id"] != id {
			continue
		}
		msgs[l.fields["msg"].(string)] = true
		if l.fields["correlation_id"] != "t-1" {
			t.Errorf("worker line without the correlation id: %s", l.raw)
		}
	}
	for _, want := range []string{"notification queued", "delivery failed, will retry", "notification sent"} {
		if !msgs[want] {
			t.Errorf("worker line %q missing for the notification (have %v)", want, msgs)
		}
	}
	dup := regexp.MustCompile(`"(correlation_id|notification_id)":`)
	for _, l := range append(apiLines, workerLines...) {
		seen := map[string]int{}
		for _, m := range dup.FindAllStringSubmatch(l.raw, -1) {
			seen[m[1]]++
		}
		for k, n := range seen {
			if n > 1 {
				t.Errorf("%s appears %d times: %s", k, n, l.raw)
			}
		}
		if strings.Contains(l.raw, recipient) || strings.Contains(l.raw, content) {
			t.Errorf("recipient or content logged: %s", l.raw)
		}
	}
}

// contractMetrics reads the metric names per binary from contracts/observability.md.
func contractMetrics(t testing.TB) map[string][]string {
	t.Helper()
	b, err := os.ReadFile("../../specs/001-notification-engine/contracts/observability.md")
	if err != nil {
		t.Fatal(err)
	}
	row := regexp.MustCompile("(?m)^\\| `([a-z0-9_]+)` \\| \\w+ \\| [^|]+\\| (api|worker)")
	out := map[string][]string{}
	for _, m := range row.FindAllStringSubmatch(string(b), -1) {
		out[m[2]] = append(out[m[2]], m[1])
	}
	if len(out["api"]) != 4 || len(out["worker"]) != 13 {
		t.Fatalf("contract table not parsed: %v", out)
	}
	return out
}

func scrape(t testing.TB, u string) string {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// Every metric in the contract is exposed on /metrics of the binary that owns it.
func TestMetricsEndpointsExposeTheContract(t *testing.T) {
	want := contractMetrics(t)

	apiReg := metrics.NewRegistry()
	apiMetrics := metrics.NewAPI(apiReg)
	api := newAPIWith(t, quietLog, httpapi.WithMetrics(apiMetrics))
	opsAPI := httptest.NewServer(promhttp.HandlerFor(apiReg, promhttp.HandlerOpts{}))
	t.Cleanup(opsAPI.Close)
	_ = scrape(t, api.URL+"/api/v1/notifications?limit=1") // one routed request for the latency histogram

	workerReg := metrics.NewRegistry()
	metrics.NewWorker(workerReg)
	opsWorker := httptest.NewServer(promhttp.HandlerFor(workerReg, promhttp.HandlerOpts{}))
	t.Cleanup(opsWorker.Close)

	for bin, u := range map[string]string{"api": opsAPI.URL, "worker": opsWorker.URL} {
		body := scrape(t, u)
		for _, name := range want[bin] {
			if !strings.Contains(body, "# TYPE "+name+" ") {
				t.Errorf("%s /metrics lacks %s", bin, name)
			}
		}
	}
}

func readyz(t testing.TB, c *health.Checker) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	c.ReadinessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var body struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body.Status
}

// The worker needs all three dependencies; the API only needs Postgres and degrades without Redis.
func TestReadinessPolicies(t *testing.T) {
	// Worker: RabbitMQ through a proxy we can cut.
	u, err := url.Parse(env.AMQPURL)
	if err != nil {
		t.Fatal(err)
	}
	amqpProxy := newToggleProxy(t, u.Host)
	u.Host = amqpProxy.addr
	conn, err := rabbitmq.Dial(u.String(), "test-readiness")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	rdb := goredis.NewClient(&goredis.Options{Addr: env.RedisAddr})
	t.Cleanup(func() { _ = rdb.Close() })

	worker := health.ForWorker(0, postgres.Ping(env.Pool), rabbitmq.Alive(conn), redisadapter.Ping(rdb))
	if code, _ := readyz(t, worker); code != http.StatusOK {
		t.Fatalf("worker ready = %d before the outage", code)
	}
	amqpProxy.Stop()
	waitFor(t, 15*time.Second, "worker not ready without rabbitmq", func() bool {
		code, _ := readyz(t, worker)
		return code == http.StatusServiceUnavailable
	})
	rec := httptest.NewRecorder()
	health.LivenessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("worker liveness = %d during the outage, want 200", rec.Code)
	}

	// API: Redis through a proxy we can cut.
	redisProxy := newToggleProxy(t, env.RedisAddr)
	apiRedis := goredis.NewClient(&goredis.Options{Addr: redisProxy.addr, MaxRetries: -1,
		DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = apiRedis.Close() })
	api := health.ForAPI(0, postgres.Ping(env.Pool), redisadapter.Ping(apiRedis))
	if code, st := readyz(t, api); code != http.StatusOK || st != string(health.StatusOK) {
		t.Fatalf("api ready = %d %s before the outage", code, st)
	}
	redisProxy.Stop()
	waitFor(t, 5*time.Second, "api degraded without redis", func() bool {
		code, st := readyz(t, api)
		return code == http.StatusOK && st == string(health.StatusDegraded)
	})
}
