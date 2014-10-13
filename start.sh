#!/bin/bash
mkdir -p /var/log/sfproxy
/opt/sfproxy/bin/signalfxproxy -configfile /var/sfdbproxy.conf -signalfxproxypid=/var/sfproxy.pid -log_dir=/var/log/sfproxy $@
