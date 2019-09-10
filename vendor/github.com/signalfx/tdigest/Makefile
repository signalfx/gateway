GO_VERSION = 1.12
GOLANGCI_LINT_VERSION=1.17.1

# set goos by detecting environment
# this is useful for cross compiling in linux container for local os
ifeq ($(GOOS), "")
	ifeq ($(OS), Windows_NT)
		GOOS=windows
	else
		ifeq ($(shell uname -s),Linux)
			GOOS=linux
		else
			GOOS=darwin
		endif
	endif
endif

.PHONY: deafult
default: build

.PHONY: install-gobuild
install-gobuild:
	go get github.com/jstemmer/go-junit-report
	go get github.com/signalfx/gobuild

.PHONY: install-golangci-lint
install-golangci-lint:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(shell go env GOPATH)/bin v${GOLANGCI_LINT_VERSION}

.PHONY: install-build-tools
install-build-tools: install-gobuild install-golangci-lint
#	# install tools used for building
#	# we must explicitly turn on GO111MODULE to install packages with a version number
#	# see: https://github.com/golang/go/issues/29415

.PHONY: vendor
vendor:
	# vendors modules
	docker run --rm -t \
		-e GOOS=$(GOOS) \
		-e GO111MODULE=on \
		-v $(CURDIR):/go/src/github.com/signalfx/tdigest \
		-w /go/src/github.com/signalfx/tdigest \
		golang:$(GO_VERSION) /bin/bash -c "go mod tidy; go mod vendor"

.PHONY: test
test:
	GO111MODULE=on go test -timeout 60s -cpu 8

.PHONY: cover
cover:
	GO111MODULE=on go test -timeout 60s -cpu 4 -coverprofile /tmp/cover.out . && go tool cover -html=/tmp/cover.out -o /tmp/cover.html && open /tmp/cover.html

.PHONY: test-in-container
test-in-container:
	docker run --rm -t \
		-e GO111MODULE=on \
		-v $(CURDIR):/go/src/github.com/signalfx/tdigest \
        -w /go/src/github.com/signalfx/tdigest \
        golang:$(GO_VERSION) /bin/bash -c "make install-gobuild; make test"

.PHONY: lint
lint:
	golangci-lint run

.PHONY: lint-in-container
lint-in-container:
	docker run --rm -t \
		-v $(CURDIR):/go/src/github.com/signalfx/tdigest \
		-w /go/src/github.com/signalfx/tdigest \
		golang:$(GO_VERSION) /bin/bash -c "make install-golangci-lint; make lint"

.PHONY: clean
clean:
	# remove previous output
	rm -rf $(CURDIR)/output/*

