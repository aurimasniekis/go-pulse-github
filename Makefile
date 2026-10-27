SHELL        := /usr/bin/env bash
GO           ?= go
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT       ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null)
DATE         ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG          := github.com/aurimasniekis/go-pulse-github/internal/buildinfo
GO_LDFLAGS   := -s -w -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).Date=$(DATE)
IMAGE        ?= pulse-github:local
GITLEAKS     ?= $(GO) run github.com/zricethezav/gitleaks/v8@v8.30.1
GOVULNCHECK  ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@v1.9.0

.DEFAULT_GOAL := help
.PHONY: help build image image-multiarch fmt-check vet test race lint vuln secrets check e2e-pulse site site-dev clean

help: ## List targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---- build -------------------------------------------------------------------

build: ## Build bin/pulse-github (CGO_ENABLED=0)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(GO_LDFLAGS)" -o bin/pulse-github ./cmd/pulse-github

image: ## Build the container image for the local platform ($(IMAGE))
	docker buildx build --load -t $(IMAGE) --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(DATE) .

image-multiarch: ## Build the image for linux/amd64 and linux/arm64 (no push)
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(DATE) -t $(IMAGE) .

# ---- checks ------------------------------------------------------------------

fmt-check: ## Fail when gofmt would change a file
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet: ## go vet
	$(GO) vet ./...

test: vet race ## go vet + race tests (embedded JetStream with Pulse-equivalent least-privilege creds)

race: ## Go tests with the race detector
	$(GO) test -race -count=1 ./...

lint: ## golangci-lint
	golangci-lint run ./...

vuln: ## govulncheck
	$(GOVULNCHECK) ./...

secrets: ## gitleaks over the git history (ignores user git config: forced colour breaks its parser)
	GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 $(GITLEAKS) git --redact --config .gitleaks.toml .

check: fmt-check lint test vuln secrets ## Everything CI runs (except e2e-pulse and the image build)

e2e-pulse: ## Opt-in: real pulsed + NATS in Docker, client created through the Pulse admin API
	./scripts/e2e-pulse.sh

# ---- documentation -----------------------------------------------------------

site: ## Build the documentation website (site/, VitePress) into site/.vitepress/dist
	cd site && pnpm install --frozen-lockfile && pnpm build

site-dev: ## Documentation website dev server
	cd site && pnpm install --frozen-lockfile && pnpm dev

clean: ## Remove build output
	rm -rf bin dist site/.vitepress/dist site/.vitepress/cache
