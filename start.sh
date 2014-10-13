#!/bin/bash
mkdir -p /var/log/sfproxy
/opt/sfproxy/bin/signalfxproxy -configfile /var/sfdbproxy.conf -log_dir=/var/log/sfproxy $@
