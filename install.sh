#!/bin/sh
set -x

/etc/init.d/metricproxy stop || echo "Proxy not currently running"
YUM_CMD=$(which yum)
APT_GET_CMD=$(which apt-get)
GO_CMD=$(which go)
HG_CMD=$(which hg)
if [ ! -z "$GO_CMD" ] && [ ! -z "$HG_CMD" ]; then
  set -e
elif [ ! -z "$APT_GET_CMD" ]; then
  set -e
  apt-get install -y golang git mercurial
elif [ ! -z "$YUM_CMD" ]; then
  set -e
  yum install -y golang git mercurial
else
  echo "Unable to find package manager"
  exit 1
fi

rm -rf /opt/sfproxy
rm -f /etc/init.d/metricproxy
mkdir -p /opt/sfproxy
cd /opt/sfproxy
env GOPATH="$(pwd)" go get github.com/tools/godep
env GOPATH="$(pwd)" go get github.com/signalfx/metricproxy

cd /opt/sfproxy/src/github.com/signalfx/metricproxy
env GOPATH="/opt/sfproxy" /opt/sfproxy/bin/godep go install

ln -s /opt/sfproxy/src/github.com/signalfx/metricproxy/metricproxy /etc/init.d/metricproxy
if [ ! -f /opt/sfproxy/bin/metricproxy ]; then
    echo "Unable to install proxy"
    exit 1
fi
echo "Install ok!  To run execute 'sudo /etc/init.d/metricproxy start'"
/etc/init.d/metricproxy locations
