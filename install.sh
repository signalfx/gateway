#!/bin/sh
set -x

/etc/init.d/metricproxy stop || echo "Proxy not currently running"
YUM_CMD=$(which yum)
APT_GET_CMD=$(which apt-get)
GO_CMD=$(which go)
GIT_CMD=$(which git)
set -e
if [ ! -z "$GO_CMD" ] && [ ! -z "$GIT_CMD" ]; then
  echo "Both go and git already installed, continuing"
elif [ ! -z "$APT_GET_CMD" ]; then
  apt-get install -y golang git
elif [ ! -z "$YUM_CMD" ]; then
  yum install -y golang git
else
  echo "Unable to find package manager"
  exit 1
fi

which go
which git

rm -rf /opt/sfproxy
rm -f /etc/init.d/metricproxy
mkdir -p /opt/sfproxy
cd /opt/sfproxy
env GOPATH="$(pwd)" go get github.com/tools/godep

mkdir -p /opt/sfproxy/src/github.com/signalfx
cd /opt/sfproxy/src/github.com/signalfx
git clone https://github.com/signalfx/metricproxy.git

cd /opt/sfproxy/src/github.com/signalfx/metricproxy
env GOPATH="/opt/sfproxy" /opt/sfproxy/bin/godep go install

ln -s /opt/sfproxy/src/github.com/signalfx/metricproxy/metricproxy_initd /etc/init.d/metricproxy
if [ ! -f /opt/sfproxy/bin/metricproxy ]; then
    echo "Unable to install proxy"
    exit 1
fi
echo "Install ok!  To run execute 'sudo /etc/init.d/metricproxy start'"
/etc/init.d/metricproxy locations
