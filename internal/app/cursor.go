package app

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/SerhanTelatar/notification-engine/internal/domain"
	"github.com/SerhanTelatar/notification-engine/internal/ports"
)

// wireCursor is the opaque keyset position (research R13): the last row's (created_at, id), newest
// first. It isn't signed: an edited but well-formed cursor only moves the starting point, which a
// client could do with from/to anyway.
type wireCursor struct {
	CreatedAt string `json:"c"`
	ID        string `json:"i"`
}

// EncodeCursor returns URL-safe base64 (no padding) of the position.
func EncodeCursor(c ports.Cursor) string {
	b, _ := json.Marshal(wireCursor{CreatedAt: c.CreatedAt.UTC().Format(time.RFC3339Nano), ID: c.ID}) //nolint:errchkjson // plain strings
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a cursor from EncodeCursor. Anything else is a ValidationError on cursor.
func DecodeCursor(s string) (ports.Cursor, error) {
	invalid := domain.NewFieldError("cursor", "is not a valid page cursor; use next_cursor from a previous page")
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) == 0 {
		return ports.Cursor{}, invalid
	}
	var w wireCursor
	if err := json.Unmarshal(raw, &w); err != nil {
		return ports.Cursor{}, invalid
	}
	at, err := time.Parse(time.RFC3339Nano, w.CreatedAt)
	if err != nil {
		return ports.Cursor{}, invalid
	}
	if _, err := uuid.Parse(w.ID); err != nil {
		return ports.Cursor{}, invalid
	}
	return ports.Cursor{CreatedAt: at.UTC(), ID: w.ID}, nil
}
