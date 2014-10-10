signalfxproxy
=============

The proxy is a multilingual datapoint demultiplexer that can accept time series data from the statsd,
carbon, or signalfuse protocols and emit those datapoints to a series of servers on the statsd, carbon,
or signalfuse protocol.  The proxy is ideally placed on the same server as either another aggregator,
such as statsd, or on a central server that is already receiving datapoints, such as graphite's carbon
database.

Install
=======

   cd /opt/proxy
   yum install -y golang
   env GOPATH=`pwd` go get github.com/signalfuse/signalfxproxy

Running
=======

   /opt/proxy/bin/signalfxproxy --configfile /tmp/sfdbproxy.conf

Debug logging
=============

   /opt/proxy/bin/signalfxproxy --configfile /tmp/sfdbproxy.conf -v=3

Configuration
=============

Use the file exampleSfdbproxy.conf as an example configuration.  Importantly, replace DefaultAuthToken with
your auth token and remove any listeners or forwarders you don't use.
