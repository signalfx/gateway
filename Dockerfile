FROM scratch
MAINTAINER Matthew Pound <mwp@signalfx.com>

COPY ca-bundle.crt /etc/pki/tls/certs/ca-bundle.crt
COPY gateway /gateway
COPY buildInfo.json /buildInfo.json

VOLUME /var/log/gateway
VOLUME /var/config/gateway

CMD ["/gateway", "-configfile", "/var/config/gateway/gateway.conf"]
