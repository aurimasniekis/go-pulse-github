// Package observability provides structured logging and Prometheus metrics.
package observability

import (
	"io"
	"log/slog"
	"strings"
	"time"
)

// Stable log field names (OpenSearch mappings depend on them).
const (
	FieldComponent      = "component"
	FieldAccount        = "account_key"
	FieldInstallation   = "installation_id"
	FieldCollector      = "collector"
	FieldFeed           = "feed"
	FieldDelivery       = "delivery_id"
	FieldObservation    = "observation_id"
	FieldPublication    = "publication_id"
	FieldEventType      = "event_type"
	FieldAttempt        = "attempt"
	FieldDurationMS     = "duration_ms"
	FieldErrorClass     = "error_class"
	FieldError          = "error"
	FieldSubject        = "subject"
	FieldGitHubEvent    = "github_event"
	FieldStatusCode     = "status_code"
	FieldRequestPath    = "request_path"
	FieldRetryAfterSecs = "retry_after_s"
)

// NewLogger builds the service logger: JSON (default) or text, UTC RFC 3339 timestamps, one line per entry.
func NewLogger(w io.Writer, level, format, version string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{
		Level: lv,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
				return slog.String(slog.TimeKey, a.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			if a.Key == slog.LevelKey {
				return slog.String(slog.LevelKey, strings.ToLower(a.Value.String()))
			}
			return a
		},
	}
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h).With("service", "pulse-github", "version", version)
}

// Secret is a string that never appears in logs.
type Secret string

// LogValue redacts the secret.
func (Secret) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }

// String redacts the secret in fmt output.
func (Secret) String() string { return "[REDACTED]" }

// Ms renders a duration as integer milliseconds (stable numeric type).
func Ms(d time.Duration) slog.Attr { return slog.Int64(FieldDurationMS, d.Milliseconds()) }

// Err renders an error as a string field (never as a nested object).
func Err(err error) slog.Attr {
	if err == nil {
		return slog.String(FieldError, "")
	}
	return slog.String(FieldError, err.Error())
}
