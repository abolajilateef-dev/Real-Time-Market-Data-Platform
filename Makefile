.PHONY: help up down logs ps kafka-topics tidy build test clean \
        run-feedhandler-coinbase run-feedhandler-binance \
        run-bookservice run-persister run-wspublisher

MODULE := $(shell go list -m)
COMPOSE := docker compose -f deploy/docker-compose.yml

help:
	@echo "Common targets:"
	@echo "  make up                        Bring up the infra stack (Redpanda, TimescaleDB, Prometheus, Grafana)"
	@echo "  make down                      Shut it all down"
	@echo "  make logs                      Tail infra logs"
	@echo "  make ps                        Show infra status"
	@echo "  make tidy                      Sync go.mod / go.sum"
	@echo "  make build                     Build all service binaries into ./bin/"
	@echo "  make test                      Run all tests"
	@echo "  make run-feedhandler-coinbase  Run the Coinbase feed handler"
	@echo "  make run-bookservice           Run the book service"
	@echo "  make clean                     Remove ./bin"

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

tidy:
	go mod tidy

build:
	@mkdir -p bin
	go build -o bin/feedhandler-coinbase ./cmd/feedhandler-coinbase
	go build -o bin/feedhandler-binance  ./cmd/feedhandler-binance
	go build -o bin/bookservice          ./cmd/bookservice
	go build -o bin/persister            ./cmd/persister
	go build -o bin/wspublisher          ./cmd/wspublisher

test:
	go test ./...

run-feedhandler-coinbase:
	go run ./cmd/feedhandler-coinbase

run-feedhandler-binance:
	go run ./cmd/feedhandler-binance

run-bookservice:
	go run ./cmd/bookservice

run-persister:
	go run ./cmd/persister

run-wspublisher:
	go run ./cmd/wspublisher

clean:
	rm -rf bin

kafka-topics:
	$(COMPOSE) exec redpanda rpk topic create trades --partitions 6 || true
	$(COMPOSE) exec redpanda rpk topic create book_snapshots --partitions 6 || true
	$(COMPOSE) exec redpanda rpk topic create book_updates --partitions 6 || true
	$(COMPOSE) exec redpanda rpk topic create book_tops --partitions 6 || true