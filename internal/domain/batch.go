package domain

import "time"

// Batch groups notifications submitted together (data-model.md § batches). Status totals are
// derived from its notifications, never stored.
type Batch struct {
	ID            string
	TotalAccepted int
	TotalRejected int
	CorrelationID string
	CreatedAt     time.Time
}

// Template is reusable content with named variables (data-model.md § templates). Parsing and
// rendering arrive with US8.
type Template struct {
	ID        string
	Name      string
	Body      string
	Variables []string
	CreatedAt time.Time
}
