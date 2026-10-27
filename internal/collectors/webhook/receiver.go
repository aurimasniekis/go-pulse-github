// Package webhook receives signed GitHub App webhook deliveries into the durable inbox and normalizes them into
// routed observations.
package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

var (
	deliveryRe = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
	eventRe    = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	numericRe  = regexp.MustCompile(`^[0-9]{1,20}$`)
)

// Admission decides whether new deliveries can be durably accepted (storage health and capacity).
type Admission interface {
	Admit(ctx context.Context) error
}

// Receiver is the webhook HTTP handler.
type Receiver struct {
	DB        *storage.DB
	Verifier  *githubauth.WebhookVerifier
	MaxBody   int64
	Admission Admission
	Metrics   *observability.Metrics
	Log       *slog.Logger
	Now       func() time.Time
	// Notify wakes the inbox processor after a commit.
	Notify func()
}

func (h *Receiver) count(result string) {
	if h.Metrics != nil {
		h.Metrics.WebhookRequests.WithLabelValues(result).Inc()
	}
}

func (h *Receiver) reject(w http.ResponseWriter, code int, result, msg string) {
	h.count(result)
	http.Error(w, msg, code)
}

// ServeHTTP verifies, validates and durably stores a delivery, then answers. It answers 2xx only after the delivery is
// committed to the state database.
func (h *Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.reject(w, http.StatusMethodNotAllowed, "rejected_method", "method not allowed")
		return
	}
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		h.reject(w, http.StatusUnsupportedMediaType, "rejected_content_type", "content type must be application/json")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.MaxBody))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			h.reject(w, http.StatusRequestEntityTooLarge, "rejected_size", "payload too large")
			return
		}
		h.reject(w, http.StatusBadRequest, "rejected_read", "could not read body")
		return
	}
	if _, err := h.Verifier.Verify(body, r.Header.Get("X-Hub-Signature-256")); err != nil {
		h.reject(w, http.StatusUnauthorized, "rejected_signature", "invalid signature")
		return
	}
	delivery := r.Header.Get("X-GitHub-Delivery")
	event := r.Header.Get("X-GitHub-Event")
	hookID := r.Header.Get("X-GitHub-Hook-ID")
	targetType := r.Header.Get("X-GitHub-Hook-Installation-Target-Type")
	targetID := r.Header.Get("X-GitHub-Hook-Installation-Target-ID")
	if !deliveryRe.MatchString(delivery) || !eventRe.MatchString(event) ||
		(hookID != "" && !numericRe.MatchString(hookID)) || (targetID != "" && !numericRe.MatchString(targetID)) ||
		len(targetType) > 64 {
		h.reject(w, http.StatusBadRequest, "rejected_metadata", "invalid delivery headers")
		return
	}
	if !isJSONObject(body) {
		h.reject(w, http.StatusBadRequest, "rejected_malformed", "body must be a JSON object")
		return
	}
	log := h.Log.With(observability.FieldDelivery, delivery, observability.FieldGitHubEvent, event)
	if err := h.Admission.Admit(r.Context()); err != nil {
		log.Warn("webhook admission closed", observability.Err(err))
		w.Header().Set("Retry-After", "60")
		h.reject(w, http.StatusServiceUnavailable, "unavailable", "temporarily unable to store deliveries")
		return
	}
	inserted, err := h.DB.InsertDelivery(r.Context(), storage.Delivery{
		DeliveryID: delivery, HookID: hookID, Event: event, TargetType: targetType, TargetID: targetID,
		Body: body, ReceivedAt: h.Now(),
	})
	if err != nil {
		if h.Metrics != nil {
			h.Metrics.SQLiteErrors.WithLabelValues("insert_delivery").Inc()
		}
		log.Error("could not store webhook delivery", observability.Err(err))
		w.Header().Set("Retry-After", "60")
		h.reject(w, http.StatusServiceUnavailable, "storage_error", "temporarily unable to store deliveries")
		return
	}
	if !inserted {
		h.count("duplicate")
		w.WriteHeader(http.StatusOK)
		return
	}
	h.count("accepted")
	log.Debug("webhook delivery stored", "bytes", len(body))
	if h.Notify != nil {
		h.Notify()
	}
	w.WriteHeader(http.StatusAccepted)
}

func isJSONObject(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return json.Valid(b)
		default:
			return false
		}
	}
	return false
}
