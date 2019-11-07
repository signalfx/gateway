FROM golang:1.13.4 as builder

WORKDIR /usr/src/gateway
COPY / ./

ARG BUILDER=circle

RUN scripts/build.sh

FROM scratch
MAINTAINER Matthew Pound <mwp@signalfx.com>

COPY ca-bundle.crt /etc/pki/tls/certs/ca-bundle.crt
COPY --from=builder /usr/src/gateway/gateway /gateway
COPY --from=builder /usr/src/gateway/buildInfo.json /buildInfo.json

VOLUME /var/log/gateway
VOLUME /var/config/gateway

CMD ["/gateway", "-configfile", "/var/config/gateway/gateway.conf"]
