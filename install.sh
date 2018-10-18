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
  apt-get install -y golang git file
elif [ ! -z "$YUM_CMD" ]; then
  yum install -y golang git file
else
  echo "Unable to find package manager"
  exit 1
fi

PATH_TO_SAMPLING=$1

which go
which git

rm -rf /opt/sfproxy
rm -f /etc/init.d/metricproxy
mkdir -p /opt/sfproxy
cd /opt/sfproxy

mkdir -p /opt/sfproxy/src/github.com/signalfx
cd /opt/sfproxy/src/github.com/signalfx
git clone https://github.com/signalfx/metricproxy.git

export GOPATH="/opt/sfproxy" 
cd /opt/sfproxy/src/github.com/signalfx/metricproxy
if [ -n "$PATH_TO_SAMPLING" ]; then
  if [ -f "$PATH_TO_SAMPLING" ]; then
    file "$PATH_TO_SAMPLING" | grep "current ar archive"
    if [ ! "$(go version)" = "go version go1.11.1 linux/amd64" ]; then
      echo "Smart Gateway requires go 1.11.1, you're running $(go version)"
      echo "go 1.11.1 can be downloaded from https://dl.google.com/go/go1.11.1.linux-amd64.tar.gz" 
      exit 1
    fi
    echo "$GOPATH/pkg/$(go env GOOS)_$(go env GOARCH)/github.com/signalfx"
    SIGNALFX="$GOPATH/pkg/$(go env GOOS)_$(go env GOARCH)/github.com/signalfx"
    export SIGNALFX
    mkdir -p "$SIGNALFX/metricproxy"
    cp "$PATH_TO_SAMPLING" "$SIGNALFX/metricproxy/sampling.a"
    rm -f /opt/sfproxy/src/github.com/signalfx/metricproxy/sampling/*
    printf '//go:binary-only-package\n\npackage sampling' > sampling/sampling.go
  else 
    echo "Sampling module specified but does not exist at \"$PATH_TO_SAMPLING\""
    exit 1
  fi
fi

go install

ln -s /opt/sfproxy/src/github.com/signalfx/metricproxy/metricproxy_initd /etc/init.d/metricproxy
if [ ! -f /opt/sfproxy/bin/metricproxy ]; then
    echo "Unable to install proxy"
    exit 1
fi
echo "Install ok!  To run execute 'sudo /etc/init.d/metricproxy start'"
/etc/init.d/metricproxy locations
