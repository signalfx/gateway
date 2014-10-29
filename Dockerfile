FROM phusion/baseimage:0.9.11
MAINTAINER Jack Lindamood <jack@signalfuse.com>

ENV DEBIAN_FRONTEND noninteractive

# Clean/refresh apt-get
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN apt-get update
RUN apt-get -y upgrade

# Install dependencies
RUN apt-get -y install golang git mercurial curl ruby ghc ghc-prof ghc-doc cabal-install
RUN cabal update
RUN cabal install shellcheck
RUN ln ~/.cabal/bin/shellcheck /usr/bin/shellcheck
RUN gem install mdl

RUN mkdir -p /opt/sfproxy

# Invalidate cache so "go get" gets the latest code
RUN mkdir -p /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/

ADD config /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/config
ADD core /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/core
ADD forwarder /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/forwarder
ADD listener /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/listener
ADD protocoltypes /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/protocoltypes

ADD signalfxproxy.go /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/
ADD signalfxproxy_test.go /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/

ADD travis_check.sh /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/

ADD README.md /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/

ENV GOPATH /opt/sfproxy
RUN go get github.com/golang/lint/golint
RUN go get code.google.com/p/go.tools/cmd/vet
RUN go get github.com/stretchr/testify/mock
RUN go get code.google.com/p/go.tools/cmd/cover
RUN go env
RUN go get github.com/signalfuse/signalfxproxy

ENV PATH $GOPATH/bin:$PATH

RUN cd /opt/sfproxy/src/github.com/signalfuse/signalfxproxy && /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/travis_check.sh

# Add run command
VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy
EXPOSE 6060
USER root
CMD ["/opt/sfproxy/bin/signalfxproxy", "-pprofaddr", "0.0.0.0:6060", "-configfile", "/var/config/sfproxy/sfdbproxy.conf", "-signalfxproxypid", "/var/config/sfproxy/sfproxy.pid" ,"-log_dir", "/var/log/sfproxy"]
