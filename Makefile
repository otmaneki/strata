.PHONY: build test bench bench-redis bench-local bench-tiered vet fmt tidy clean

build:
	go build ./...

# Everything except bench-local needs a real redis reachable at
# REDIS_ADDR=host:port, e.g.:
#   make test REDIS_ADDR=localhost:6379
test:
	REDIS_ADDR=$(REDIS_ADDR) go test -v -race -count=1 ./...

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
