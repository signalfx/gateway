# signalfxproxy [![Build Status](https://travis-ci.org/signalfuse/signalfxproxy.svg?branch=master)](https://travis-ci.org/signalfuse/signalfxproxy) [![Docker Repository on Quay.io](https://quay.io/repository/signalfuse/signalfxproxy/status "Docker Repository on Quay.io")](https://quay.io/repository/signalfuse/signalfxproxy)

The proxy is a multilingual datapoint demultiplexer that can accept time
series data from the statsd, carbon, or signalfuse protocols and emit
those datapoints to a series of servers on the statsd, carbon, or
signalfuse protocol.  The proxy is ideally placed on the same server as
either another aggregator, such as statsd, or on a central server that
is already receiving datapoints, such as graphite's carbon database.

## Install and upgrade

```
  curl -s https://raw.githubusercontent.com/signalfuse/signalfxproxy/master/install.sh | sudo sh
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

## Debugging

```
  cd /var/log/sfproxy
  tail -F *
```

## Profiling

If you start the proxy with -pprofaddr="0.0.0.0:6060", it will enable pprof
for the proxy and you can profile it at
[http://localhost:6060/debug/pprof/](http://localhost:6060/debug/pprof/).  You
can find more information about how to use pprof at
[the pprof help page](http://golang.org/pkg/net/http/pprof/).

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

## Config file format

See the [example config](exampleSfdbproxy.conf) file for an example of how
configuration looks.  Configuration is a JSON file with two important fields:
ListenFrom and ForwardTo.

### ListenFrom

ListenFrom is where you define what services the proxy will pretend to be and
what ports to listen for those services on.

#### signalfx

You can pretend to be a signalfx endpoint with the signalfx type.  For this,
you will need to specify which port to bind to.  An example config:

```
        {
            "ListenAddr": "0.0.0.0:18080",
            "Type": "signalfx"
        },
```

#### carbon (for read)

You can pretend to be carbon (the graphite database) with this type.  For
this, you will need to specify the port to bind to.  An example config:

```
        {
            "ListenAddr": "0.0.0.0:12003",
            "Type": "carbon"
        }
```

### ForwardTo

ForwardTo is where you define where the proxy should send datapoints.  Each datapoint
that comes from a ListenFrom definition will be send to each of these.

#### csv

You can write datapoints to a CSV file for debugging with this config.  You
will need to specify the filename.

```
        {
            "Filename": "/tmp/filewrite.csv",
            "Name": "filelocal",
            "type": "csv"
        }
```

#### carbon (for write)

You can write datapoints to a carbon server.  If the point came from a carbon
listener, it will write the same way the proxy saw it.  Host/Port define where
the carbon server is.

```
        {
            "Name": "ourcarbon",
            "Host": "example.com",
            "Port": 2003,
            "type": "carbon"
        },
```

#### signalfx-json

You can write datapoints to SiganlFuse with this endpoint.  You will need to
configure your auth token inside DefaultAuthToken.

```
        {
            "type": "signalfx-json",
            "DefaultAuthToken": "___AUTH_TOKEN___",
            "Name": "testproxy",
        },
```

## Example configs

### Basic

This config will listen for graphite metrics on port 2003 and forward them
to signalfuse with the token ABCD.  It will also report local stats
to signalfuse at 1s intervals

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr" : "0.0.0.0:2003",
    },
  ],

  "ForwardTo": [
    {
      "type": "signalfx-json",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder",
    }
  ]
}
```

### Graphite Options

This config will listen using CollectD's HTTP protocol and forward
all those metrics to a single graphite listener.  It will collect
stats at 1s intervals.  It also signals to graphite that when it creates
a graphite name for a metric, it should put the 'source' (which is usually
proxy) and 'forwarder' (in this case 'graphite-west') first in the graphite
dot delimited name.

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "collectd",
      "ListenAddr" : "0.0.0.0:8081",
    },
  ],

  "ForwardTo": [
    {
      "type": "carbon",
      "DefaultAuthToken": "ABCD",
      "Host": "graphite.database.dc1.com",
      "DimensionsOrder": ["source", "forwarder"],
      "Name": "graphite-west"
    }
  ]
}
```

### SignalFX perf options

This config listens for carbon data on port 2003 and forwards it to signalfuse
using an internal datapoint buffer size of 1,000,000 and sending with 50 threads
simultaniously with each thread sending no more than 5,000 points in a single
call.

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr" : "0.0.0.0:2003",
    },
  ],

  "ForwardTo": [
    {
      "type": "signalfx-json",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder",
      "BufferSize": 1000000,
      "DrainingThreads": 50,
      "MaxDrainSize": 5000
    }
  ]
}
```
