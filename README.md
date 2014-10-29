# signalfxproxy

The proxy is a multilingual datapoint demultiplexer that can accept time
series data from the statsd, carbon, or signalfuse protocols and emit
those datapoints to a series of servers on the statsd, carbon, or
signalfuse protocol.  The proxy is ideally placed on the same server as
either another aggregator, such as statsd, or on a central server that
is already receiving datapoints, such as graphite's carbon database.

## Install and upgrade

```
  ./install.sh
  # Config at    /etc/sfdbconfig.conf
  # Binary at    /opt/sfproxy/bin/signalfxproxy
  # Logs at      /var/log/sfproxy
  # PID file at  /var/run/signalfxproxy.pid
 ```

## Running

```
   /etc/init.d/signalfxproxy start
 ```

## Stopping the daemon

```
   /etc/init.d/signalfxproxy stop
 ```

## Debug logging

```
   /opt/proxy/bin/signalfxproxy --configfile /tmp/sfdbproxy.conf -v=3
 ```

## Debugging

```
  cd /var/log/sfproxy
  tail -F *
```

## Configuration

Use the file exampleSfdbproxy.conf as an example configuration.  Importantly,
replace DefaultAuthToken with your auth token and remove any listeners or
forwarders you don't use.

## Code layout

You only need to read this if you want to develop the proxy or understand
the proxy's code.

The proxy is divided into two main components: [forwarder](forwarder)
and [listener](listener).  The forwarder and listener are glued together
by the [demultiplexer](forwarder/demultiplexer.go).

When a listener receives a datapoint, it converts the datapoint into a
basic [datapoint type](core/datapoint.go).  This core datapoint type is
then sent to the multiplexer that will send a pointer to that datapoint
to each forwarder.

Sometimes there is a loss of fidelity during transmission if a listener
and forwarder don't support the same options.  While it's impossible
to make something understand an option it does not, we don't want to
forget support for this option when we translate a datapoint through
the multiplexer.  We work around this by sometimes encoding the raw
representation of the datapoint into the Datapoint object we forward.
For example, points from carbon are not only translated into our core
datapoint format, but also support [ToCarbonLine](protocoltypes/carbon.go)
which allows us to directly convert the abstract datapoint into what it
looked like for carbon, which allows us to forward the point to another
carbon database exactly as we received it.

All message passing between forwarders, multiplexer, and listeners
happen on golang's built in channel abstraction.

## Development

If you want to submit patches for the proxy, make sure your code passes
[travis_check.sh](travis_check.sh) with exit code 0.  For help setting
up your development enviroment, it should be enough to mirror the install
steps of [.travis.yml](.travis.yml).  You may need to make sure your GOPATH
env variable is set correctly.

## Docker

The proxy comes with a [docker image](Dockerfile) that is built and deployed
to [quay.io](https://quay.io/repository/signalfuse/signalfxproxy).  It assumes
you will have a sfdbconfig.json file cross mounted to
/var/config/sfproxy/sfdbconfig.json for the docker container.

## Build status

### Travis

[![Build Status](https://travis-ci.org/signalfuse/signalfxproxy.svg?branch=master)](https://travis-ci.org/signalfuse/signalfxproxy)

### Quay

[![Docker Repository on Quay.io](https://quay.io/repository/signalfuse/signalfxproxy/status
"Docker Repository on Quay.io")](https://quay.io/repository/signalfuse/signalfxproxy)

