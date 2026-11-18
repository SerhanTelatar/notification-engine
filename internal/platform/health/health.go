// Package health implements liveness and readiness (Constitution IV, contracts/observability.md).
// Liveness only reports that the process is up. Readiness checks the real dependencies: a failed
// required one makes the process unavailable (503), a failed optional one only degrades it (200).
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Status values match the Health schema in api/openapi.yaml.
type Status string

const (
	StatusOK          Status = "ok"
	StatusDegraded    Status = "degraded"
	StatusUnavailable Status = "unavailable"
)

// DefaultTimeout bounds every dependency check.
const DefaultTimeout = time.Second

// Check probes one dependency; a nil error means healthy.
type Check func(ctx context.Context) error

// Result is the JSON body of /healthz and /readyz.
type Result struct {
	Status Status            `json:"status"`
	Checks map[string]Status `json:"checks,omitempty"`
}

type dep struct {
	name     string
	check    Check
	required bool
}

// Checker aggregates dependency checks. Register them before serving traffic.
type Checker struct {
	timeout time.Duration
	deps    []dep
}

// New returns a Checker; timeout <= 0 means DefaultTimeout.
func New(timeout time.Duration) *Checker {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Checker{timeout: timeout}
}

// Require registers a dependency whose failure makes the process not ready.
func (c *Checker) Require(name string, check Check) *Checker {
	c.deps = append(c.deps, dep{name, check, true})
	return c
}

// Optional registers a dependency whose failure only degrades readiness.
func (c *Checker) Optional(name string, check Check) *Checker {
	c.deps = append(c.deps, dep{name, check, false})
	return c
}

// Ready runs every check concurrently, each with its own timeout.
func (c *Checker) Ready(ctx context.Context) Result {
	res := Result{Status: StatusOK, Checks: make(map[string]Status, len(c.deps))}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, d := range c.deps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			st := StatusOK
			if err := d.check(cctx); err != nil {
				st = StatusDegraded
				if d.required {
					st = StatusUnavailable
				}
			}
			mu.Lock()
			defer mu.Unlock()
			res.Checks[d.name] = st
			res.Status = worst(res.Status, st)
		}()
	}
	wg.Wait()
	return res
}

// Live is the liveness result: it never touches dependencies.
func Live() Result { return Result{Status: StatusOK} }

// HTTPStatus maps a result to its response code: 503 only when unavailable.
func (r Result) HTTPStatus() int {
	if r.Status == StatusUnavailable {
		return http.StatusServiceUnavailable
	}
	return http.StatusOK
}

// Names returns the registered dependency names, sorted (for logs and tests).
func (c *Checker) Names() []string {
	out := make([]string, 0, len(c.deps))
	for _, d := range c.deps {
		out = append(out, d.name)
	}
	sort.Strings(out)
	return out
}

// LivenessHandler serves GET /healthz for binaries without a generated server (the worker).
func LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { write(w, Live()) })
}

// ReadinessHandler serves GET /readyz for binaries without a generated server (the worker).
func (c *Checker) ReadinessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { write(w, c.Ready(r.Context())) })
}

func write(w http.ResponseWriter, res Result) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(res.HTTPStatus())
	_ = json.NewEncoder(w).Encode(res)
}

func worst(a, b Status) Status {
	rank := map[Status]int{StatusOK: 0, StatusDegraded: 1, StatusUnavailable: 2}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

// ForAPI is the API's readiness policy (contracts/observability.md): Postgres is required; Redis
// only carries live status events, so without it the API stays ready but reports degraded.
func ForAPI(timeout time.Duration, postgres, redis Check) *Checker {
	return New(timeout).Require("postgres", postgres).Optional("redis", redis)
}

// ForWorker is the worker's readiness policy: it can't deliver without Postgres, RabbitMQ or
// Redis (the rate limiter fails closed), so each one is required.
func ForWorker(timeout time.Duration, postgres, rabbitmq, redis Check) *Checker {
	return New(timeout).Require("postgres", postgres).Require("rabbitmq", rabbitmq).Require("redis", redis)
}
