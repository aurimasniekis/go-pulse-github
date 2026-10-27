// Package events defines the versioned pulse-github payload, subject tokens, deterministic IDs and the normalization
// of GitHub source data into observations.
package events

import (
	"crypto/sha256"
	"encoding/base32"
	"regexp"
	"strings"
)

// NoAction is the subject token used when the source has no action (for example the push webhook).
const NoAction = "none"

// maxVerbatim bounds verbatim tokens; longer values are encoded.
const maxVerbatim = 48

var (
	verbatimRe = regexp.MustCompile(`^[a-z0-9_]+$`)
	reserved   = map[string]bool{NoAction: true}
	b32hex     = base32.HexEncoding.WithPadding(base32.NoPadding)
	b32        = base32.StdEncoding.WithPadding(base32.NoPadding)
)

// Token maps an arbitrary source value (event name, action) to a Pulse subject token, deterministically and without
// collisions:
//
//   - values matching [a-z0-9_]{1,48} that are not reserved are used verbatim;
//   - other values become "x-" + lowercase base32hex(value) when that fits in 64 bytes;
//   - longer values become "h-" + the first 26 base32 characters of SHA-256(value).
//
// Verbatim tokens never contain '-', so they cannot collide with encoded tokens. The original value is always in the
// payload.
func Token(v string) string {
	if len(v) <= maxVerbatim && verbatimRe.MatchString(v) && !reserved[v] {
		return v
	}
	enc := "x-" + strings.ToLower(b32hex.EncodeToString([]byte(v)))
	if len(enc) <= 64 {
		return enc
	}
	return "h-" + hash26(v)
}

// ActionToken is Token for actions, mapping the empty action to NoAction.
func ActionToken(a string) string {
	if a == "" {
		return NoAction
	}
	return Token(a)
}

func hash26(s string) string {
	sum := sha256.Sum256([]byte(s))
	return strings.ToLower(b32.EncodeToString(sum[:]))[:26]
}

// ObservationID derives the stable observation ID from its source and dedup key.
func ObservationID(source, dedupKey string) string { return "o" + hash26(source+"\x00"+dedupKey) }

// PublicationID derives the stable, account-scoped Pulse event ID (Nats-Msg-Id) of an observation.
func PublicationID(observationID, accountKey string) string {
	return "p" + hash26(observationID+"\x00"+accountKey)
}

// Fingerprint hashes the given parts into a short stable fingerprint.
func Fingerprint(parts ...string) string { return hash26(strings.Join(parts, "\x1f")) }
