package app

import (
	"context"
	"fmt"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// SummaryWindow is the period GET /api/v1/metrics/summary covers (research R16).
const SummaryWindow = 5 * time.Minute

// SummaryDeps are the summary's collaborators.
type SummaryDeps struct {
	Repo  ports.SummaryRepository
	Clock ports.Clock
}

// Summary computes an operational summary from the database (bonus, T116). Prometheus stays the
// primary signal; this is a dependency-free JSON view of the same picture.
type Summary struct{ d SummaryDeps }

// NewSummary returns the summary use case.
func NewSummary(d SummaryDeps) *Summary { return &Summary{d: d} }

// MetricsSummary is the backlog now plus each channel's last SummaryWindow.
type MetricsSummary struct {
	WindowSeconds int
	Backlog       []ports.BacklogCount
	Channels      []ChannelSummary // every channel, in domain.Channels order
}

// ChannelSummary is one channel's window. SuccessRate is successful provider calls over all
// finished calls; it and the percentiles are nil when there's nothing to measure.
type ChannelSummary struct {
	Channel           domain.Channel
	Sent              int
	Failed            int
	Retried           int
	SuccessRate       *float64
	E2ELatencyMs      *ports.Percentiles
	ProviderLatencyMs *ports.Percentiles
}

// Get computes the summary.
func (s *Summary) Get(ctx context.Context) (MetricsSummary, error) {
	backlog, err := s.d.Repo.BacklogCounts(ctx)
	if err != nil {
		return MetricsSummary{}, fmt.Errorf("summary backlog: %w", err)
	}
	stats, err := s.d.Repo.ChannelStats(ctx, s.d.Clock.Now().Add(-SummaryWindow))
	if err != nil {
		return MetricsSummary{}, fmt.Errorf("summary stats: %w", err)
	}
	by := make(map[domain.Channel]ports.ChannelStats, len(stats))
	for _, st := range stats {
		by[st.Channel] = st
	}
	out := MetricsSummary{WindowSeconds: int(SummaryWindow / time.Second), Backlog: backlog,
		Channels: make([]ChannelSummary, 0, len(domain.Channels))}
	if out.Backlog == nil {
		out.Backlog = []ports.BacklogCount{}
	}
	for _, ch := range domain.Channels {
		st := by[ch]
		c := ChannelSummary{Channel: ch, Sent: st.Sent, Failed: st.Failed, Retried: st.Retried,
			E2ELatencyMs: st.E2E, ProviderLatencyMs: st.Provider}
		if st.Attempts > 0 {
			rate := float64(st.Successes) / float64(st.Attempts)
			c.SuccessRate = &rate
		}
		out.Channels = append(out.Channels, c)
	}
	return out, nil
}
