# Argus — market-surveillance engine
.DEFAULT_GOAL := help
BIN := bin
AUDIT_DIR ?= ./data/audit

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all Go binaries into ./bin
	go build -o $(BIN)/ ./cmd/...

.PHONY: test
test: ## Run the Go test suite (offline)
	go test ./...

.PHONY: test-race
test-race: ## Run tests with the race detector
	go test -race ./...

.PHONY: test-live
test-live: ## Run the live exchange smoke tests, Binance + OKX (needs internet)
	ARGUS_LIVE=1 go test ./internal/exchange/... -run 'TestLive.*Smoke' -v

.PHONY: bench
bench: ## Benchmark detection-engine throughput
	go test ./internal/detect/ -bench . -benchtime 2s -run XXX

.PHONY: test-ml
test-ml: ## Run the Python ML scorer offline test
	cd ml && python test_scorer.py

.PHONY: vet
vet: ## go vet
	go vet ./...

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w .

.PHONY: run
run: ## Run the all-in-one daemon with the synthetic scenario (dashboard :8080)
	go run ./cmd/argusd -audit-dir $(AUDIT_DIR)

.PHONY: run-live
run-live: ## Run the all-in-one daemon against the live Binance feed
	go run ./cmd/argusd -live -symbols BTCUSDT,ETHUSDT -audit-dir $(AUDIT_DIR)

.PHONY: verify
verify: ## Verify the tamper-evident audit trail
	go run ./cmd/auditverify -dir $(AUDIT_DIR)

.PHONY: up
up: ## docker compose: self-contained demo (synthetic tape)
	docker compose --profile demo up --build

.PHONY: up-live
up-live: ## docker compose: live Binance feed
	docker compose --profile live up --build

.PHONY: down
down: ## Tear down the docker stack and volumes
	docker compose --profile demo --profile live down -v

.PHONY: azure-deploy
azure-deploy: ## Deploy the full stack to Azure Container Apps (RG=<group> [LOC=eastus] [FEED=demo])
	./deploy/azure/deploy.sh $(RG) $(or $(LOC),eastus) $(or $(FEED),demo)

.PHONY: clean
clean: ## Remove build output and local data
	rm -rf $(BIN) data
