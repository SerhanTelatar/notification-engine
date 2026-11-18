//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/SerhanTelatar/notification-engine/internal/adapters/postgres"
	"github.com/SerhanTelatar/notification-engine/internal/adapters/rabbitmq"
	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// The backlog query counts non-terminal rows only, grouped by channel, priority and status.
func TestRepoBacklogCounts(t *testing.T) {
	env.Truncate(t)
	repo := postgres.NewNotificationRepo(env.Pool)
	high := func(d *domain.Draft) { d.Priority = "high" }

	for range 2 {
		n := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, high))
		moveTo(t, n.ID, domain.StatusQueued, 0)
	}
	mustCreate(t, repo, newNotification(t, domain.ChannelEmail, func(d *domain.Draft) { d.Priority = "low" }))
	sent := mustCreate(t, repo, newNotification(t, domain.ChannelSMS, high))
	if _, err := env.Pool.Exec(context.Background(),
		`UPDATE notifications SET status = 'sent', sent_at = now() WHERE id = $1`, sent.ID); err != nil {
		t.Fatal(err)
	}

	counts, err := repo.BacklogCounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[ports.BacklogCount]bool{}
	for _, c := range counts {
		got[c] = true
	}
	want := []ports.BacklogCount{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusQueued, Count: 2},
		{Channel: domain.ChannelEmail, Priority: domain.PriorityLow, Status: domain.StatusPending, Count: 1},
	}
	if len(counts) != len(want) {
		t.Fatalf("counts = %+v, want %+v (sent rows excluded)", counts, want)
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("counts = %+v, missing %+v", counts, w)
		}
	}
}

// Depths reads every channel queue and the DLQ without declaring anything.
func TestDepthInspector(t *testing.T) {
	resetBroker(t)
	conn, err := rabbitmq.Dial(env.AMQPURL, "test-depth-inspector")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := ch.PublishWithContext(context.Background(), "", rabbitmq.DLQ, false, false, amqp.Publishing{Body: []byte("{}")}); err != nil {
			t.Fatal(err)
		}
	}
	_ = ch.Close()

	var depths map[string]int
	waitFor(t, 5*time.Second, "dlq depth 3", func() bool {
		depths, err = rabbitmq.NewDepthInspector(conn).Depths(context.Background())
		return err == nil && depths[rabbitmq.DLQ] == 3
	})
	for _, c := range domain.Channels {
		if v, ok := depths[rabbitmq.QueueName(c)]; !ok || v != 0 {
			t.Fatalf("%s depth = %d (%v), want an explicit 0", c, v, ok)
		}
	}
}
