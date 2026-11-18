# notification-engine — developer entry points.
#
# Every Go target also runs inside the pinned Go image with DOCKER=1, so only Docker is needed
# (research R1). sqlc and golangci-lint always run from their pinned images.

SHELL := /bin/bash

GO_VERSION := 1.27.4
GO_IMAGE   := golang:$(GO_VERSION)
SQLC_IMAGE := sqlc/sqlc:1.31.1
LINT_IMAGE := golangci/golangci-lint:v2.15.0

DOCKER_RUN := docker run --rm -v "$(CURDIR)":/src -w /src \
	-v ne-gomod:/go/pkg/mod -v ne-gocache:/root/.cache/go-build
# Integration tests start containers through the host's Docker daemon (testcontainers-go).
DOCKER_RUN_TC := $(DOCKER_RUN) -v /var/run/docker.sock:/var/run/docker.sock \
	-e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal

ifeq ($(DOCKER),1)
GO    := $(DOCKER_RUN) $(GO_IMAGE) go
GO_TC := $(DOCKER_RUN_TC) $(GO_IMAGE) go
else
GO    := go
GO_TC := go
endif

GENERATED := internal/adapters/http/gen internal/adapters/postgres/sqlcgen

.PHONY: help gen gen-check build fmt lint test test-unit migrate up down logs demo-burst loadtest

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

gen: ## Regenerate HTTP server (oapi-codegen) and DB queries (sqlc)
	$(GO) tool oapi-codegen -config oapi-codegen.yaml api/openapi.yaml
	@if ls sql/queries/*.sql >/dev/null 2>&1; then \
		echo "sqlc generate"; docker run --rm -v "$(CURDIR)":/src -w /src $(SQLC_IMAGE) generate; \
	else echo "sqlc: no queries yet, skipping"; fi

gen-check: gen ## Fail if generated code is out of date with the contract or SQL
	git diff --exit-code -- $(GENERATED)
	@test -z "$$(git status --porcelain -- $(GENERATED))" || \
		(echo "untracked generated files:"; git status --porcelain -- $(GENERATED); exit 1)

build: ## Build all binaries into bin/
	$(GO) build -trimpath -o bin/ ./cmd/...

fmt: ## Format code
	$(GO) fmt ./...

lint: ## Run golangci-lint (pinned image)
	docker run --rm -v "$(CURDIR)":/src -w /src -v ne-gomod:/go/pkg/mod \
		-v ne-lintcache:/root/.cache $(LINT_IMAGE) golangci-lint run --build-tags integration ./...

test: ## Unit + integration tests with -race (integration needs Docker)
	$(GO_TC) test -race -count=1 -tags integration ./...

test-unit: ## Unit tests only (no Docker needed for the tests themselves)
	$(GO) test -race -count=1 ./...

migrate: ## Apply DB migrations against the compose stack
	docker compose run --rm migrate

up: ## Start the full stack
	docker compose up --build -d

down: ## Stop the stack and remove volumes
	docker compose down -v

logs: ## Follow api and worker logs
	docker compose logs -f api worker

demo-burst: ## Submit a 5,000-notification mixed-priority burst (quickstart #11)
	./scripts/demo-burst.sh

loadtest: ## SC-001 load check: 10,000 creates with vegeta against the running stack
	./scripts/loadtest.sh
