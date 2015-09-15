FROM phusion/baseimage:0.9.17
MAINTAINER Jack Lindamood <jack@signalfx.com>

ENV DEBIAN_FRONTEND noninteractive

ENV GOLANG_VERSION 1.5.1
RUN curl -sSL https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz | tar -v -C /usr/local -xz
ENV PATH /usr/local/go/bin:$PATH

RUN mkdir -p /go

# Invalidate cache so "go get" gets the latest code
RUN mkdir -p /opt/go/src/github.com/signalfx/metricproxy/

ADD . /opt/go/src/github.com/signalfx/metricproxy

ENV GOPATH /opt/go:/opt/go/src/github.com/signalfx/metricproxy/Godeps/_workspace
RUN go env && go version

# Getting and running tests increases image size, but makes sure the tests work
# even inside the image
RUN go test -cpu 2 -parallel 8 github.com/signalfx/metricproxy/...
RUN go install -v -x github.com/signalfx/metricproxy
RUN touch /opt/go/bin/metricproxy

ENV PATH /opt/go/bin:$PATH

# Add run command
VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy
WORKDIR /opt/go
USER root
CMD ["/opt/go/bin/metricproxy", "-configfile", "/var/config/sfproxy/sfdbproxy.conf"]
