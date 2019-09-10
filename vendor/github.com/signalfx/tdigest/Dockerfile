ARG GO_VERSION=1.12

# download build tools
FROM golang:${GO_VERSION} AS build-tools
ENV GO111MODULE=on
COPY ./Makefile /usr/src/github.com/signalfx/tdigest/Makefile
WORKDIR /usr/src/github.com/signalfx/tdigest
RUN go version
RUN make install-build-tools

# lint using golangci-lint
FROM build-tools AS lint
COPY . /usr/src/github.com/signalfx/tdigest
WORKDIR /usr/src/github.com/signalfx/tdigest
RUN gobuild list
RUN make lint

# test using gobuild
FROM build-tools AS test
COPY . /usr/src/github.com/signalfx/tdigest
WORKDIR /usr/src/github.com/signalfx/tdigest
RUN gobuild list
RUN gobuild test

# build
FROM build-tools AS build
ENV BUILDER=docker
COPY . /usr/src/github.com/signalfx/tdigest
WORKDIR /usr/src/github.com/signalfx/tdigest
RUN go version
RUN BUILDER=docker make build-info
RUN cat buildInfo.json
RUN make build
