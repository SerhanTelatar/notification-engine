package app

import (
	"context"
	"testing"
	"time"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

type fakeSummaryRepo struct {
	since   time.Time
	backlog []ports.BacklogCount
	stats   []ports.ChannelStats
}

func (r *fakeSummaryRepo) BacklogCounts(context.Context) ([]ports.BacklogCount, error) {
	return r.backlog, nil
}

func (r *fakeSummaryRepo) ChannelStats(_ context.Context, since time.Time) ([]ports.ChannelStats, error) {
	r.since = since
	return r.stats, nil
}

func TestSummaryCoversEveryChannelOverFiveMinutes(t *testing.T) {
	clock := newClock()
	repo := &fakeSummaryRepo{
		backlog: []ports.BacklogCount{{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Status: domain.StatusPending, Count: 3}},
		stats: []ports.ChannelStats{{
			Channel: domain.ChannelSMS, Sent: 8, Failed: 2, Retried: 5, Attempts: 16, Successes: 8,
			E2E:      &ports.Percentiles{P50: 120, P95: 900, P99: 1500},
			Provider: &ports.Percentiles{P50: 40, P95: 90, P99: 110},
		}},
	}
	s, err := NewSummary(SummaryDeps{Repo: repo, Clock: clock}).Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if s.WindowSeconds != 300 || !repo.since.Equal(clock.Now().Add(-5*time.Minute)) {
		t.Fatalf("window %ds since %v", s.WindowSeconds, repo.since)
	}
	if len(s.Backlog) != 1 || s.Backlog[0].Count != 3 {
		t.Fatalf("backlog %+v", s.Backlog)
	}
	if len(s.Channels) != 3 || s.Channels[0].Channel != domain.ChannelSMS || s.Channels[1].Channel != domain.ChannelEmail || s.Channels[2].Channel != domain.ChannelPush {
		t.Fatalf("channels %+v, want sms, email, push", s.Channels)
	}
	sms := s.Channels[0]
	if sms.Sent != 8 || sms.Failed != 2 || sms.Retried != 5 || sms.SuccessRate == nil || *sms.SuccessRate != 0.5 ||
		sms.E2ELatencyMs.P95 != 900 || sms.ProviderLatencyMs.P50 != 40 {
		t.Fatalf("sms %+v", sms)
	}
	email := s.Channels[1]
	if email.Sent != 0 || email.SuccessRate != nil || email.E2ELatencyMs != nil || email.ProviderLatencyMs != nil {
		t.Fatalf("a quiet channel is zeros with no rate or percentiles: %+v", email)
	}
}
