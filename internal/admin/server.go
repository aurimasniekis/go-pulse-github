package admin

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aurimasniekis/go-pulse-github/internal/observability"
)

// Status reports component health for /readyz bodies.
type Status interface {
	Connected() bool
}

// Handler builds the admin mux: /healthz (process), /readyz (safe admission), /metrics and optional /debug/pprof.
func Handler(g *Guard, m *observability.Metrics, nats Status, pprofToken []byte) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		body := map[string]any{"status": "ready", "storage": "ok", "nats_connected": nats != nil && nats.Connected()}
		code := http.StatusOK
		if err := g.Err(); err != nil {
			code = http.StatusServiceUnavailable
			body["status"], body["storage"] = "not_ready", err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry}))
	if len(pprofToken) > 0 {
		pp := http.NewServeMux()
		pp.HandleFunc("/debug/pprof/", pprof.Index)
		pp.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pp.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pp.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pp.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.Handle("/debug/pprof/", requireToken(pprofToken, pp))
	}
	return mux
}

func requireToken(token []byte, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), token) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
