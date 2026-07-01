# SPDX-License-Identifier: Apache-2.0
.PHONY: up down restart logs ps tail-topic clean \
	fmt fmt-check vet lint test build verify compose-build

COMPOSE ?= docker compose
TOPIC ?= gnmi.telemetry
GO ?= go
BIN ?= bin

## --- Go targets (used by CI) ---

fmt:
	$(GO) fmt ./...

fmt-check:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "The following files are not gofmt-ed:"; echo "$$out"; exit 1; \
	fi

vet:
	$(GO) vet ./...

lint:
	golangci-lint run ./...

test:
	$(GO) test -race -coverprofile=coverage.out ./...

build:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/driver ./cmd/driver
	CGO_ENABLED=0 $(GO) build -o $(BIN)/gateway ./cmd/gateway

## verify runs the full quality gate CI depends on
verify: fmt-check vet lint test build

## --- Docker Compose targets ---

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

restart: down up

compose-build:
	$(COMPOSE) build

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

tail-topic:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC) \
		--from-beginning \
		--max-messages 50 \
		--property print.key=true \
		--property key.separator=' | '

clean: down
	docker volume prune -f
