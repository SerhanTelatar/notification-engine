package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// SampleInterval is how often the backlog gauges are refreshed (research R16).
const SampleInterval = 5 * time.Second

// BacklogSource counts non-terminal notifications by channel, priority and status.
type BacklogSource interface {
	BacklogCounts(ctx context.Context) ([]ports.BacklogCount, error)
}

// SamplerDeps are the Sampler's collaborators.
type SamplerDeps struct {
	Repo    BacklogSource
	Queues  ports.QueueInspector
	Metrics ports.MetricsRecorder
	Log     *slog.Logger
}

// Sampler refreshes notification_backlog from the database (the source of truth) and
// rabbitmq_queue_messages from the broker (a secondary signal) on each tick (FR-031).
type Sampler struct {
	d    SamplerDeps
	last map[ports.BacklogCount]struct{} // combinations (Count zeroed) reported last tick
}

// NewSampler returns a Sampler. It isn't safe for concurrent ticks; run it from one loop.
func NewSampler(d SamplerDeps) *Sampler {
	return &Sampler{d: d, last: map[ports.BacklogCount]struct{}{}}
}

// Tick samples once. A combination that had a count last tick but no longer appears (its backlog
// drained) is reported as an explicit 0; otherwise the gauge would keep its last value forever.
// A broker error doesn't stop the database gauges: both are attempted and the errors joined.
func (s *Sampler) Tick(ctx context.Context) error {
	var errs []error
	counts, err := s.d.Repo.BacklogCounts(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("backlog counts: %w", err))
	} else {
		now := make(map[ports.BacklogCount]struct{}, len(counts))
		for _, c := range counts {
			c.Count = 0
			now[c] = struct{}{}
		}
		for k := range s.last {
			if _, ok := now[k]; !ok {
				counts = append(counts, k) // k.Count is 0
			}
		}
		s.d.Metrics.Backlog(counts)
		s.last = now
	}

	if s.d.Queues != nil {
		depths, err := s.d.Queues.Depths(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("queue depths: %w", err))
		}
		for q, n := range depths {
			s.d.Metrics.QueueDepth(q, n)
		}
	}
	return errors.Join(errs...)
}
