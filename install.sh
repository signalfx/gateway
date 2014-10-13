#!/bin/bash
mkdir -p /opt/sfproxy
cd /opt/sfproxy
yum install -y golang
env GOPATH=`pwd` go get github.com/signalfuse/signalfxproxy
