.PHONY: build test bench bench-redis bench-local bench-tiered vet fmt tidy clean

build:
	go build ./...

test:
	go test -v -count=1 ./...

# Runs all three benchmark suites (redis-only, local-only, tiered) against
# an in-process miniredis instance. Pass REDIS_ADDR=host:port to target a
# real redis server instead, e.g.:
#   make bench REDIS_ADDR=localhost:6379
bench:
	REDIS_ADDR=$(REDIS_ADDR) go test ./... -run '^$$' -bench . -benchmem

bench-redis:
	REDIS_ADDR=$(REDIS_ADDR) go test ./... -run '^$$' -bench BenchmarkRedisOnly -benchmem

bench-local:
	go test ./... -run '^$$' -bench BenchmarkLocalOnly -benchmem

bench-tiered:
	REDIS_ADDR=$(REDIS_ADDR) go test ./... -run '^$$' -bench BenchmarkTieredCache -benchmem

vet:
	go vet ./...

fmt:
	gofmt -l .

tidy:
	go mod tidy

clean:
	go clean ./...
