OUTPUT ?= bin
BEZORG_OUTPUT ?= $(OUTPUT)/bezorg
BEZORGD_OUTPUT ?= $(OUTPUT)/bezorgd
SEMVER ?= 0.1.0
VERSION ?= $(SEMVER)-dev
RELEASE_TAG ?= v$(SEMVER)
LDFLAGS = -ldflags "-s -w -X main.Version=$(VERSION)"
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: all build test vet race check clean release

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

release:
	@test -z "$$(git status --porcelain)" || { echo "Refusing to release with a dirty working tree."; exit 1; }
	@if git rev-parse --verify --quiet "refs/tags/$(RELEASE_TAG)" >/dev/null; then echo "Tag $(RELEASE_TAG) already exists."; exit 1; fi
	git tag "$(RELEASE_TAG)"
	git push origin "$(RELEASE_TAG)"
