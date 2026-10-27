package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// Log lines are single-line JSON with UTC timestamps and stable keys; secrets render as [REDACTED].
func TestLogger(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "info", "json", "v1")
	log.Info("hello", "token", Secret("ghp_supersecret"), FieldAccount, "acct", Err(errors.New("multi\nline")))
	line := buf.String()
	if strings.Count(line, "\n") != 1 || strings.Contains(line, "ghp_supersecret") {
		t.Fatalf("%q", line)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatal(err)
	}
	if m["token"] != "[REDACTED]" || m["service"] != "pulse-github" || m["version"] != "v1" || m["level"] != "info" ||
		!strings.HasSuffix(m["time"].(string), "Z") || m["error"] != "multi\nline" {
		t.Fatalf("%v", m)
	}
}
