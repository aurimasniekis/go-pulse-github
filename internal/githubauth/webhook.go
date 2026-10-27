// Package githubauth handles GitHub credentials: webhook signatures, GitHub App JWTs, installation tokens and personal
// access tokens.
package githubauth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

// ErrSignature is returned for a missing or invalid X-Hub-Signature-256.
var ErrSignature = errors.New("invalid webhook signature")

// WebhookVerifier checks X-Hub-Signature-256 against one or more secrets. Several secrets allow rotation: configure
// the new secret next to the old one, switch the App's secret on GitHub, then remove the old secret.
type WebhookVerifier struct {
	secrets [][]byte
}

// NewWebhookVerifier builds a verifier. Secrets must be non-empty.
func NewWebhookVerifier(secrets ...[]byte) (*WebhookVerifier, error) {
	if len(secrets) == 0 {
		return nil, errors.New("at least one webhook secret is required")
	}
	for _, s := range secrets {
		if len(s) == 0 {
			return nil, errors.New("webhook secrets must not be empty")
		}
	}
	return &WebhookVerifier{secrets: secrets}, nil
}

// Verify checks the signature header over the unmodified body. It returns the index of the matching secret. Every
// secret is compared (in constant time) so timing does not reveal which one matched.
func (v *WebhookVerifier) Verify(body []byte, header string) (int, error) {
	hexSig, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return -1, ErrSignature
	}
	sig, err := hex.DecodeString(hexSig)
	if err != nil || len(sig) != sha256.Size {
		return -1, ErrSignature
	}
	match := -1
	for i, s := range v.secrets {
		m := hmac.New(sha256.New, s)
		m.Write(body)
		if hmac.Equal(m.Sum(nil), sig) && match < 0 {
			match = i
		}
	}
	if match < 0 {
		return -1, ErrSignature
	}
	return match, nil
}

// Sign computes the X-Hub-Signature-256 header value (tests and doctor tooling).
func Sign(secret, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}
