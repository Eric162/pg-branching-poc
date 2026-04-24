VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS  = -ldflags "-X github.com/pg-branch/pg-branch/internal/cli.Version=$(VERSION) -X github.com/pg-branch/pg-branch/internal/cli.Commit=$(COMMIT)"

.PHONY: build test test-v bench install clean

build:
	go build $(LDFLAGS) -o pg-branch ./cmd/pg-branch

test:
	go test ./... -count=1

test-v:
	go test ./... -count=1 -v

# Runs the Postgres-bound benchmarks gated behind the `bench` build tag.
# Requires a local Postgres on port 5432 (override via PG_BRANCH_TEST_URL).
bench:
	go test -tags=bench -bench=. -benchtime=1x -run=^$$ -timeout=15m \
		./internal/diff ./internal/merge ./internal/branch

install:
	go install $(LDFLAGS) ./cmd/pg-branch

clean:
	rm -f pg-branch
