#!/bin/sh

YUM_CMD=$(which yum)
APT_GET_CMD=$(which apt-get)
GO_CMD=$(which go)
HG_CMD=$(which hg)
if [ ! -z "$GO_CMD" ] && [ ! -z "$HG_CMD" ]; then
  set -e
elif [ ! -z "$APT_GET_CMD" ]; then
  set -e
  apt-get install -y golang mercurial
elif [ ! -z "$YUM_CMD" ]; then
  set -e
  yum install -y golang mercurial
else
  echo "Unable to find package manager"
  exit 1
fi

mkdir -p /opt/sfproxy
cd /opt/sfproxy
env GOPATH="$(pwd)" go get -u github.com/signalfuse/signalfxproxy
ln -s /opt/sfproxy/src/github.com/signalfuse/signalfxproxy/signalfxproxy /etc/init.d/signalfxproxy
if [ ! -f /opt/sfproxy/bin/signalfxproxy ]; then
    echo "Unable to install proxy"
    exit 1
fi
echo "Install ok!  To run execute '/etc/init.d/signalfxproxy start'"
