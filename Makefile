.PHONY: all test lint proto build bench bench-compare test-integration test-fuzz coverage

all: lint test build

proto:
	buf generate

build:
	go build -o bin/server ./cmd/server
	go build -o bin/winter ./cmd/winter

test:
	go test -race -count=1 ./...

test-integration:
	go test -tags=integration -race -count=1 ./test/integration/...

test-fuzz:
	go test -fuzz=Fuzz -fuzztime=60s ./internal/queue/...

bench:
	go test -bench=. -benchmem -count=10 ./internal/queue/... | tee bench.txt
	benchstat bench.txt

bench-compare:
	go test -bench=. -benchmem -count=10 ./internal/queue/... | tee bench-new.txt
	benchstat $(OLD) bench-new.txt

lint:
	golangci-lint run

coverage:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
