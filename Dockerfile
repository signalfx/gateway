FROM scratch
MAINTAINER Jack Lindamood <jack@signalfx.com>

COPY ./metricproxy /metricproxy

VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy
CMD ["/metricproxy", "-configfile", "/var/config/sfproxy/sfdbproxy.conf"]
