OUTPUT ?= bin
BEZORG_OUTPUT ?= $(OUTPUT)/bezorg
BEZORGD_OUTPUT ?= $(OUTPUT)/bezorgd
SEMVER ?= 0.1.0
VERSION ?= $(SEMVER)-dev
LDFLAGS = -ldflags "-s -w -X main.Version=$(VERSION)"
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: all build test vet race check clean

all: build

build:
	mkdir -p $(dir $(BEZORG_OUTPUT)) $(dir $(BEZORGD_OUTPUT))
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(LDFLAGS) -o $(BEZORG_OUTPUT) ./cmd/bezorg
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(LDFLAGS) -o $(BEZORGD_OUTPUT) ./cmd/bezorgd

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

check: vet test

clean:
	rm -f $(BEZORG_OUTPUT) $(BEZORGD_OUTPUT)
