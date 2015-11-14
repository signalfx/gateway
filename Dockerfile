FROM scratch
MAINTAINER Jack Lindamood <jack@signalfx.com>

COPY metricproxy /metricproxy
COPY ca-bundle.crt /etc/pki/tls/certs/ca-bundle.crt

VOLUME /var/log/sfproxy
VOLUME /var/config/sfproxy

CMD ["/metricproxy", "-configfile", "/var/config/sfproxy/sfdbproxy.conf"]
