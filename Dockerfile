FROM phusion/baseimage:0.9.11
MAINTAINER Jack Lindamood <jack@signalfuse.com>

ENV DEBIAN_FRONTEND noninteractive

# Clean/refresh apt-get
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN apt-get update
RUN apt-get -y upgrade

# Install dependencies
RUN apt-get -y install golang git hg curl

RUN mkdir -p /opt/sfproxy
RUN cd /opt/sfproxy && env GOPATH=`pwd` go get -u github.com/signalfuse/signalfxproxy

# Add run command
ADD run.py /opt/docker/.docker/
ADD dockercommonpy /opt/docker/.docker/dockercommon.py
VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy
USER root
CMD ["/opt/sfproxy/bin/signalfxproxy", "-configfile", "/var/config/sfproxy/sfdbproxy.conf", "-signalfxproxypid", "/var/config/sfproxy/sfproxy.pid" ,"-log_dir", "/var/log/sfproxy"]
