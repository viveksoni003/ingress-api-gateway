# ingress-api-gateway — developer tasks
# Run `make help` to list targets.

APP        := ingress-api-gateway
PKG        := ./...
BIN_DIR    := bin
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X main.version=$(VERSION)
DATABASE_URL ?= postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: tidy
tidy: ## Resolve and pin dependencies (creates go.sum)
	go mod tidy

.PHONY: build
build: ## Build the gateway binary into bin/
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/gateway ./cmd/gateway

.PHONY: build-tools
build-tools: ## Build the loadtester and admin-token helpers
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/loadtester ./cmd/loadtester
	go build -o $(BIN_DIR)/admin-token ./cmd/admin-token

.PHONY: run
run: ## Run the gateway locally (needs Redis + Postgres)
	go run ./cmd/gateway

.PHONY: test
test: ## Run all tests with the race detector
	go test -race -count=1 $(PKG)

.PHONY: test-short
test-short: ## Run unit tests only (skip slower ones)
	go test -short -count=1 $(PKG)

.PHONY: cover
cover: ## Run tests and open an HTML coverage report
	go test -coverprofile=coverage.out $(PKG)
	go tool cover -html=coverage.out

.PHONY: vet
vet: ## Run go vet
	go vet $(PKG)

.PHONY: fmt
fmt: ## Format the codebase
	gofmt -s -w .

.PHONY: lint
lint: ## Run golangci-lint if installed
	@command -v golangci-lint >/dev/null 2>&1 && golangci-lint run || echo "golangci-lint not installed; skipping"

.PHONY: docker-build
docker-build: ## Build the Docker image
	docker build --build-arg VERSION=$(VERSION) -t $(APP):$(VERSION) .

.PHONY: up
up: ## Start the full stack (gateway + redis + postgres + prometheus)
	docker compose up --build -d

.PHONY: down
down: ## Stop the stack
	docker compose down

.PHONY: logs
logs: ## Tail gateway logs
	docker compose logs -f gateway

.PHONY: migrate-up
migrate-up: ## Apply migrations with psql
	psql "$(DATABASE_URL)" -f migrations/0001_init.up.sql

.PHONY: migrate-down
migrate-down: ## Roll back migrations with psql
	psql "$(DATABASE_URL)" -f migrations/0001_init.down.sql

.PHONY: loadtest
loadtest: ## Run the built-in load tester against localhost:8080
	go run ./cmd/loadtester -url http://localhost:8080 -c 100

.PHONY: admin-token
admin-token: ## Print a short-lived admin JWT (reads JWT_SECRET)
	@go run ./cmd/admin-token

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage.out
