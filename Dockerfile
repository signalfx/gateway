FROM phusion/baseimage:0.9.16
MAINTAINER Jack Lindamood <jack@signalfuse.com>

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
RUN mkdir -p /opt/go/src/github.com/signalfuse/signalfxproxy/

ADD config /opt/go/src/github.com/signalfuse/signalfxproxy/config
ADD core /opt/go/src/github.com/signalfuse/signalfxproxy/core
ADD forwarder /opt/go/src/github.com/signalfuse/signalfxproxy/forwarder
ADD listener /opt/go/src/github.com/signalfuse/signalfxproxy/listener
ADD stats /opt/go/src/github.com/signalfuse/signalfxproxy/stats
ADD protocoltypes /opt/go/src/github.com/signalfuse/signalfxproxy/protocoltypes
ADD jsonengines /opt/go/src/github.com/signalfuse/signalfxproxy/jsonengines
ADD statuspage /opt/go/src/github.com/signalfuse/signalfxproxy/statuspage

ADD signalfxproxy.go /opt/go/src/github.com/signalfuse/signalfxproxy/
ADD signalfxproxy_test.go /opt/go/src/github.com/signalfuse/signalfxproxy/

ADD exampleSfdbproxy.conf /opt/go/src/github.com/signalfuse/signalfxproxy/
ADD travis_check.sh /opt/go/src/github.com/signalfuse/signalfxproxy/
ADD install.sh /opt/go/src/github.com/signalfuse/signalfxproxy/
ADD signalfxproxy /opt/go/src/github.com/signalfuse/signalfxproxy/
RUN ln -s /opt/go/src/github.com/signalfuse/signalfxproxy/signalfxproxy /etc/init.d/signalfxproxy

ADD README.md /opt/go/src/github.com/signalfuse/signalfxproxy/

ENV GOPATH /opt/go
RUN go env && go version

# Getting and running tests increases image size, but makes sure the tests work
# even inside the image
RUN go get -t -v github.com/signalfuse/signalfxproxy/...
RUN go test -cpu 2 -parallel 8 github.com/signalfuse/signalfxproxy/...

ENV PATH $GOPATH/bin:$PATH

# Add run command
VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy
WORKDIR /opt/go
USER root
CMD ["/opt/go/bin/signalfxproxy", "-configfile", "/var/config/sfproxy/sfdbproxy.conf"]
