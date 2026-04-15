VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS  = -ldflags "-X github.com/pg-branch/pg-branch/internal/cli.Version=$(VERSION) -X github.com/pg-branch/pg-branch/internal/cli.Commit=$(COMMIT)"

.PHONY: build test install clean

build:
	go build $(LDFLAGS) -o pg-branch ./cmd/pg-branch

test:
	go test ./... -count=1

test-v:
	go test ./... -count=1 -v

install:
	go install $(LDFLAGS) ./cmd/pg-branch

clean:
	rm -f pg-branch
