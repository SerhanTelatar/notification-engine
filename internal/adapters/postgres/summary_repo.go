package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres/sqlcgen"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// SummaryRepo implements ports.SummaryRepository on PostgreSQL.
type SummaryRepo struct {
	notifications *NotificationRepo
	q             *sqlcgen.Queries
}

var _ ports.SummaryRepository = (*SummaryRepo)(nil)

// NewSummaryRepo returns a repository on pool.
func NewSummaryRepo(pool *pgxpool.Pool) *SummaryRepo {
	return &SummaryRepo{notifications: NewNotificationRepo(pool), q: sqlcgen.New(pool)}
}

// BacklogCounts is the same view as the backlog gauge.
func (r *SummaryRepo) BacklogCounts(ctx context.Context) ([]ports.BacklogCount, error) {
	return r.notifications.BacklogCounts(ctx)
}

// ChannelStats merges sends, failures and attempts since since, per channel. Channels with no
// activity are absent; the use case fills them in.
func (r *SummaryRepo) ChannelStats(ctx context.Context, since time.Time) ([]ports.ChannelStats, error) {
	sent, err := r.q.SummarySent(ctx, since)
	if err != nil {
		return nil, translate(err, "summary sent")
	}
	failed, err := r.q.SummaryFailed(ctx, since)
	if err != nil {
		return nil, translate(err, "summary failed")
	}
	attempts, err := r.q.SummaryAttempts(ctx, since)
	if err != nil {
		return nil, translate(err, "summary attempts")
	}

	by := map[string]*ports.ChannelStats{}
	get := func(ch string) (*ports.ChannelStats, error) {
		if s, ok := by[ch]; ok {
			return s, nil
		}
		c, err := domain.ParseChannel(ch)
		if err != nil {
			return nil, fmt.Errorf("summary channel: %w", err)
		}
		by[ch] = &ports.ChannelStats{Channel: c}
		return by[ch], nil
	}
	for _, row := range sent {
		s, err := get(row.Channel)
		if err != nil {
			return nil, err
		}
		s.Sent = int(row.Sent)
		s.E2E = &ports.Percentiles{P50: row.P50, P95: row.P95, P99: row.P99}
	}
	for _, row := range failed {
		s, err := get(row.Channel)
		if err != nil {
			return nil, err
		}
		s.Failed = int(row.Failed)
	}
	for _, row := range attempts {
		s, err := get(row.Channel)
		if err != nil {
			return nil, err
		}
		s.Attempts, s.Successes, s.Retried = int(row.Attempts), int(row.Successes), int(row.Retried)
		s.Provider = &ports.Percentiles{P50: row.P50, P95: row.P95, P99: row.P99}
	}
	out := make([]ports.ChannelStats, 0, len(by))
	for _, s := range by {
		out = append(out, *s)
	}
	return out, nil
}
