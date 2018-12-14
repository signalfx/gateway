#!/bin/sh
set -x

/etc/init.d/gateway stop || echo "Proxy not currently running"
YUM_CMD=$(which yum)
APT_GET_CMD=$(which apt-get)
GO_CMD=$(which go)
GIT_CMD=$(which git)
set -e
if [ ! -z "$GO_CMD" ] && [ ! -z "$GIT_CMD" ]; then
  echo "Both go and git already installed, continuing"
elif [ ! -z "$APT_GET_CMD" ]; then
  apt-get install -y golang git file
elif [ ! -z "$YUM_CMD" ]; then
  yum install -y golang git file
else
  echo "Unable to find package manager"
  exit 1
fi

which go
which git

rm -rf /opt/gateway
rm -f /etc/init.d/gateway
mkdir -p /opt/gateway
cd /opt/gateway

mkdir -p /opt/gateway/src/github.com/signalfx
cd /opt/gateway/src/github.com/signalfx
git clone https://github.com/signalfx/gateway.git

export GOPATH="/opt/gateway" 
cd /opt/gateway/src/github.com/signalfx/gateway

go install

ln -s /opt/gateway/src/github.com/signalfx/gateway/gateway /etc/init.d/gateway
if [ ! -f /opt/gateway/bin/gateway ]; then
    echo "Unable to install proxy"
    exit 1
fi
echo "Install ok!  To run execute 'sudo /etc/init.d/gateway start'"
/etc/init.d/gateway locations
