// Package migrations embeds the SQLite schema migrations. Files are applied in name order, each in one transaction.
package migrations

import "embed"

// FS holds the NNNN_name.sql migration files.
//
//go:embed *.sql
var FS embed.FS
