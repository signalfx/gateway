signalfxproxy
=============

The proxy is a multilingual datapoint demultiplexer that can accept time
series data from the statsd, carbon, or signalfuse protocols and emit
those datapoints to a series of servers on the statsd, carbon, or
signalfuse protocol.  The proxy is ideally placed on the same server as
either another aggregator, such as statsd, or on a central server that
is already receiving datapoints, such as graphite's carbon database.

Install
-------

```
  mkdir -p /opt/sfproxy
  cd /opt/sfproxy
  yum install -y golang
  env GOPATH=`pwd` go get -u github.com/signalfuse/signalfxproxy
 ```

Running
-------

```
   ./src/github.com/signalfuse/signalfxproxy/start.sh -logtostderr
 ```

Running as daemon
-----------------

```
   nohup ./src/github.com/signalfuse/signalfxproxy/start.sh &
 ```

Stopping the daemon
-------------------

```
   ./src/github.com/signalfuse/signalfxproxy/stop.sh
 ```

Debug logging
-------------

```
   /opt/proxy/bin/signalfxproxy --configfile /tmp/sfdbproxy.conf -v=3
 ```

Debugging
---------

```
  cd /var/log/sfproxy
  tail -F *
```

Configuration
-------------

Use the file exampleSfdbproxy.conf as an example configuration.  Importantly,
replace DefaultAuthToken with your auth token and remove any listeners or
forwarders you don't use.
