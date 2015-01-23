FROM phusion/baseimage:0.9.16
MAINTAINER Jack Lindamood <jack@signalfuse.com>

ENV DEBIAN_FRONTEND noninteractive

# Clean/refresh apt-get
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN apt-get update
RUN apt-get -y install golang git mercurial

RUN mkdir -p /opt/sfproxy

# Invalidate cache so "go get" gets the latest code
RUN mkdir -p /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/

ADD config /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/config
ADD core /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/core
ADD forwarder /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/forwarder
ADD listener /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/listener
ADD stats /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/stats
ADD protocoltypes /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/protocoltypes
ADD jsonengines /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/jsonengines
ADD statuspage /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/statuspage

ADD signalfxproxy.go /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/
ADD signalfxproxy_test.go /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/

ADD exampleSfdbproxy.conf /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/
ADD travis_check.sh /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/
ADD install.sh /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/
ADD signalfxproxy /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/
RUN ln -s /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/signalfxproxy /etc/init.d/signalfxproxy

ADD README.md /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/

ENV GOPATH /opt/sfproxy
RUN go get github.com/golang/lint/golint
RUN go get code.google.com/p/go.tools/cmd/vet
RUN go get github.com/stretchr/testify/mock
RUN go get code.google.com/p/go.tools/cmd/cover
RUN go env && go version
RUN go get github.com/signalfuse/signalfxproxy
RUN go test github.com/signalfuse/signalfxproxy

ENV PATH $GOPATH/bin:$PATH

# Add run command
VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy
EXPOSE 6060
USER root
CMD ["/opt/sfproxy/bin/signalfxproxy", "-configfile", "/var/config/sfproxy/sfdbproxy.conf", "-signalfxproxypid", "/var/config/sfproxy/sfproxy.pid" ,"-logdir", "/var/log/sfproxy"]
