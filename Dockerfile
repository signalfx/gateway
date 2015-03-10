FROM phusion/baseimage:0.9.16
MAINTAINER Jack Lindamood <jack@signalfx.com>

ENV DEBIAN_FRONTEND noninteractive

# Clean/refresh apt-get
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && apt-get update && apt-get -y install git mercurial gcc libc6-dev make

# Borrow from docker-library/golang
ENV GOLANG_VERSION 1.4.1
RUN curl -sSL https://golang.org/dl/go$GOLANG_VERSION.src.tar.gz | tar -v -C /usr/src -xz
RUN cd /usr/src/go/src && ./make.bash --no-clean 2>&1
ENV PATH /usr/src/go/bin:$PATH


RUN mkdir -p /go

# Invalidate cache so "go get" gets the latest code
RUN mkdir -p /opt/go/src/github.com/signalfx/metricproxy/

ADD . /opt/go/src/github.com/signalfx/metricproxy

ENV GOPATH /opt/go
RUN go env && go version

# Getting and running tests increases image size, but makes sure the tests work
# even inside the image
RUN go get -t -d -v github.com/signalfx/metricproxy/...
RUN go test -cpu 2 -parallel 8 github.com/signalfx/metricproxy/...
RUN go install -v -x github.com/signalfx/metricproxy
RUN touch /opt/go/bin/metricproxy

ENV PATH $GOPATH/bin:$PATH

# Add run command
VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy
WORKDIR /opt/go
USER root
CMD ["/opt/go/bin/metricproxy", "-configfile", "/var/config/sfproxy/sfdbproxy.conf"]
