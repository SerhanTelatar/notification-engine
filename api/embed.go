// Package api embeds the canonical OpenAPI contract so the server can publish it at
// /openapi.yaml and Swagger UI can render it.
package api

import _ "embed"

// Spec is api/openapi.yaml as served to clients.
//
//go:embed openapi.yaml
var Spec []byte
