#!/bin/sh
set -e
mkdir -p /opt/sfproxy
cd /opt/sfproxy
yum install -y golang
env GOPATH=`pwd` go get -u github.com/signalfuse/signalfxproxy
ln -s /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/signalfxproxy /etc/init.d/signalfxproxy
if [ ! -f /opt/sfproxy/bin/signalfxproxy ]; then
    echo "Unable to install proxy"
    exit 1
fi
echo "Install ok!  To run execute '/etc/init.d/signalfxproxy start'"
