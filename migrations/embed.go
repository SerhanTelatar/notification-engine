// Package migrations embeds the goose SQL migrations so `api migrate` needs no files at runtime.
package migrations

import "embed"

// FS holds every migration at its root, as goose expects.
//
//go:embed *.sql
var FS embed.FS
