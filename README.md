# metricproxy [![Build Status](https://travis-ci.org/signalfx/metricproxy.svg?branch=master)](https://travis-ci.org/signalfx/metricproxy) [![Docker Repository on Quay.io](https://quay.io/repository/signalfx/metricproxy/status "Docker Repository on Quay.io")](https://quay.io/repository/signalfx/metricproxy)

The proxy is a multilingual datapoint demultiplexer that can accept time
series data from the statsd, carbon, or signalfx protocols and emit
those datapoints to a series of servers on the statsd, carbon, or
signalfx protocol.  The proxy is ideally placed on the same server as
either another aggregator, such as statsd, or on a central server that
is already receiving datapoints, such as graphite's carbon database.

## Table of contents
  * [Install and upgrade](#install)
  * [Running](#running)
  * [Stopping the daemon](#stopping)
  * [Debugging](#debugging)
  * [Code layout](#layout)
  * [Development](#development)
  * [Docker](#docker)
  * [Config file format](#config)
  * [Example formats](#example)
  * [Frequently asked questions](#faq)

## Install and upgrade <a id="install"></a>

```
  curl -s https://raw.githubusercontent.com/signalfx/metricproxy/master/install.sh | sudo sh
  # Config at    /etc/sfdbconfig.conf
  # Binary at    /opt/sfproxy/bin/metricproxy
  # Logs at      /var/log/sfproxy
  # PID file at  /var/run/metricproxy.pid
 ```

## Running <a id="running"></a>

```
   /etc/init.d/metricproxy start
 ```

## Stopping the daemon  <a id="stopping"></a>

```
   /etc/init.d/metricproxy stop
 ```

## Debugging <a id="debugging"></a>

```
  cd /var/log/sfproxy
  tail -F *
```

## Code layout <a id="layout"></a>

You only need to read this if you want to develop the proxy or understand
the proxy's code.

The proxy is divided into two main components: [forwarder](protocol/carbon/carbonforwarder.go)
and [listener](protocol/carbon/carbonlistener.go).  The forwarder and listener
are glued together by the [demultiplexer](protocol/demultiplexer/demultiplexer.go).

When a listener receives a datapoint, it converts the datapoint into a
basic [datapoint type](datapoint/datapoint.go).  This core datapoint type is
then sent to the multiplexer that will send a pointer to that datapoint
to each forwarder.

Sometimes there is a loss of fidelity during transmission if a listener
and forwarder don't support the same options.  While it's impossible
to make something understand an option it does not, we don't want to
forget support for this option when we translate a datapoint through
the multiplexer.  We work around this by sometimes encoding the raw
representation of the datapoint into the Datapoint object we forward.
For example, points from carbon are not only translated into our core
datapoint format, but also support [ToCarbonLine](protocol/carbon/carbon.go)
which allows us to directly convert the abstract datapoint into what it
looked like for carbon, which allows us to forward the point to another
carbon database exactly as we received it.

All message passing between forwarders, multiplexer, and listeners
happen on golang's built in channel abstraction.

## Development <a id="development"></a>

If you want to submit patches for the proxy, make sure your code passes
[travis_check.sh](travis_check.sh) with exit code 0.  For help setting
up your development enviroment, it should be enough to mirror the install
steps of [.travis.yml](.travis.yml).  You may need to make sure your GOPATH
env variable is set correctly.

## Docker <a id="docker"></a>

The proxy comes with a [docker image](Dockerfile) that is built and deployed
to [quay.io](https://quay.io/repository/signalfx/metricproxy).  It assumes
you will have a sfdbconfig.json file cross mounted to
/var/config/sfproxy/sfdbconfig.json for the docker container.

## Config file format <a id="config"></a>

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

You can write datapoints to SignalFx with this endpoint.  You will need to
configure your auth token inside DefaultAuthToken.

```
        {
            "type": "signalfx-json",
            "DefaultAuthToken": "___AUTH_TOKEN___",
            "Name": "testproxy",
        },
```

## Example configs <a id="example"></a>

### Basic <a id="basic"></a>

This config will listen for graphite metrics on port 2003 and forward them
to signalfx with the token ABCD.  It will also report local stats
to signalfx at 1s intervals

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr" : "0.0.0.0:2003",
      "Timeout" : "2m"
    },
  ],

  "ForwardTo": [
    {
      "type": "signalfx-json",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
  ]
}
```

### Graphite Options <a id="graphiteopt"></a>

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

### Graphite Dimensions <a id="graphitedim"></a>

This config will pull dimensions out of graphite metrics if they fit the commakeys
format.  That format is "\_METRIC_NAME\_\[KEY:VALUE,KEY:VALUE]".  For example,
"user.hit_rate\[host:server1,type:production]".  It also has the extra option
of adding a metric type to the datapoints.  For example, if one of the
dimensions is "metrictype" in this config and the dimension's value is "count",
then the value is sent upstream as a datapoint.Count.

It also sets the timeout on idle connections to 1 minute, from the default of 30
seconds.

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr" : "0.0.0.0:2003",
      "Timeout": "1m",
      "MetricDeconstructor": "commakeys",
      "MetricDeconstructorOptions": "mtypedim:metrictype"
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

### Graphite Dimensions using Delimiters  <a id="delimiters"></a>

You can use MetricRules to extract dimensions from the dot-separated names of
graphite metrics.

A metric will be matched to only one matching rule. When multiple rules are
provided, they are evaluated for a match to a metric in the following order:

1. The rule must contain the same number of terms as the name of the metric to
  be matched.
1. If there is more than one rule with the same number of terms as the metric
  name, then matches will be evaluated in the order in which they are defined in
  the config.
1. If there are no rules that match the metric name, the FallbackDeconstructor
  is applied. By default this is "identity": all metrics are emitted as gauges
  with unmodified names.

The simplest rule contains only a DimensionsMap with the same number of terms
and separated by the same delimiter as the incoming metrics. In the following
example, the configuration contains two rules: one that matches all metrics
with four terms, and one that matches all metrics with six terms.

If the following example config were used to process a graphite metric called
`cassandra.cassandra23.production.thread_count`, it would output the following:

```
metricName = thread_count
metricType = Gauge
dimensions = {service=cassandra, instance=cassandra23, tier=production}
```

```
{
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr": "0.0.0.0:2003",
      "MetricDeconstructor": "delimiter",
      "MetricDeconstructorOptionsJSON": {
        "MetricRules": [
          {
            "DimensionsMap": "service.instance.tier.%"
          },
          {
            "DimensionsMap": "service.instance.tier.module.submodule.%"
          }
        ]
      }
    }
  ],
  "ForwardTo": [
    {
      "type": "signalfx-json",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
  ]
}
```

You can define more complex rules for determining the name, type and dimensions
of metrics to be emitted. In this next more complex example, we first define
Dimensions that will be added to every datapoint ('customer: Acme'). We then
explicitly define the metrics that will be sent in as counters rather than
gauges (anything that ends with 'counter.count' or starts with 'counter').

Define a MetricPath separately from the DimensionsMap to match only certain
metrics.

In the example below, the MetricPath `kafka|cassandra.*.*.*.!database`
matches metrics under the following conditions:

1. If the first term of the metric name separated by '.' matches either
  'kafka' or 'cassandra'
1. And the metric contains exactly 10 terms
1. And the fifth term des not match the string 'database'

The MetricPath is followed by a DimensionsMap:
`component.identifier.instance.-.type.tier.item.item.%.%`

1. The first three terms in the metric will be mapped to dimensions as
  indicated in the DimensionsMap: 'component', 'identifier', and 'instance',
  respectively.
1. The fourth term in the metric will be ignored, since it's specified in the
  DimensionsMap as the default ignore character '-'.
1. The fifth and sixth terms will be mapped to dimensions 'type' and 'tier',
  respectively.
1. The seventh and eighth terms will be concatenated together delimited by
  the default separator character '.', because they are both mapped to the
  dimension called 'item'.
1. The ninth and tenth terms are '%', the default metric character, which
  indicates that they should be used for the metric name.

This config also contains MetricName, the value of which will be prefixed onto
the name of every metric emitted.

Finally, note that the MetricPath contains five terms, but the DimensionsMap
contains ten. This means that the MetricPath implicitly contains five
additional metric terms that are '*' (match anything).

If this config were used to process a metric named
`cassandra.bbac.23.foo.primary.prod.nodefactory.node.counter.count`, it would
output the following:

```
metricName = tiered.counter.count
metricType = counter
dimensions = {customer=Acme, component=cassandra, identifier=bbac,
              instance=23, type=primary, tier=prod, item=nodefactory.node,
              business_unit=Coyote}
```

```
{
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr": "0.0.0.0:2003",
      "MetricDeconstructor": "delimiter",
      "MetricDeconstructorOptionsJSON": {
        "Dimensions": {
          "customer": "Acme"
        },
        "TypeRules": [
          {
            "MetricType": "count",
            "EndsWith": "counter.count"
          },
          {
            "MetricType": "cumulative_counter",
            "StartsWith": "counter"
          }
        ],
        "FallbackDeconstructor": "nil",
        "MetricRules": [
          {
            "MetricPath": "kafka|cassandra.*.*.*.!database",
            "DimensionsMap": "component.identifier.instance.-.type.tier.item.item.%.%",
            "Dimensions": {
              "business_unit": "Coyote"
            },
            "MetricName": "tiered"
          }
        ]
      }
    }
  ],
  "ForwardTo": [
    {
      "type": "signalfx-json",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
  ]
}
```

The following is a full list of overrideable options and their defaults:

```
// For the top level
{
  "Delimiter":".",
  "Globbing":"*",
  "OrDelimiter":"|",
  "NotDelimiter":"!",
  "IgnoreDimension":"-",
  "MetricIdentifer":"%",
  "DefaultMetricType":"Gauge",
  "FallbackDeconstructor":"identity",
  "FallbackDeconstructorConfig":"",
  "TypeRules":[],
  "MetricRules":[],
  "Dimensions":{}
}
// A MetricRule
{
  "MetricType":"Gauge", // overrides the DefaultMetricType or TypeRules
  "MetricPath":"",
  "DimensionsMap":"",
  "MetricName":"",
  "Dimensions":{}
}
// A TypeRule. If StartsWith and EndsWith are both specified, they must both match.
{
  "MetricType":"Gauge",
  "StartsWith":"",
  "EndsWith":""
}
```

### SignalFx perf options <a id="perf"></a>

This config listens for carbon data on port 2003 and forwards it to signalfx
using an internal datapoint buffer size of 1,000,000 and sending with 50 threads
simultaniously with each thread sending no more than 5,000 points in a single
call.  It also turns on debug logging, which will spew a large number of log
messages.  Only use debug logging temporarily.

```
{
  "StatsDelay": "1s",
  "LogLevel": "debug",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr" : "0.0.0.0:2003"
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

### CollectD listener dimensions <a id="collectd"></a>

The CollectD listener supports setting dimensions on all recieved metrics with
the Dimensions attribute which expects a map of string => string.

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "collectd",
      "ListenAddr" : "0.0.0.0:8081",
      "Dimensions" : {"hello": "world"}
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

### SignalFx to SignalFx <a id="sfx2sfx"></a>

This config listens using the signalfx protocol, buffers, then forwards
points to signalfx.

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "signalfx",
      "ListenAddr" : "0.0.0.0:8080",
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

### Status Page and profiling <a id="statuspage"></a>

This config only loads a status page.  You can see status information at
`http://localhost:6009/status`, a health check page (useful for load balances) at
`http://localhost:6009/health`, and pprof information at
`http://localhost:6009/debug/pprof/`.  You can learn more about pprof for golang
on [the pprof help page](http://golang.org/pkg/net/http/pprof/).

```
{
  "LocalDebugServer": "0.0.0.0:6009"
}
```

## Frequently asked questions <a id="faq"></a>

### When should I use the proxy?

The SignalFx metric proxy is designed to be used in the following scenarios:
* You are using another metrics monitoring system (e.g. Graphite) in production
and want to evaluate SignalFx in a non-disruptive way
* You have decided to use SignalFx, but you have a mechanism for collecting 
metrics (typically an agent or other client-side code) that does not yet have
a native SignalFx backend or handler
* You limit the number of egress points from your network due to security or
other concerns, and you want to consolidate metrics-related traffic as it leaves
your network.

### How do SignalFx customers typically use the proxy?

For customers using another metrics monitoring system, they insert the proxy
in between their data collectors and that system. For example, instead of writing
to Graphite’s carbon-cache directly, their collectd, StatsD and other agents or
client libraries send their metrics to the proxy, which then forwards them on to
both Graphite and SignalFx. The proxy is configured in this case with a Graphite
listener, and both a Graphite and SignalFx forwarder.

Some customers have metrics collection methods that do not have a native SignalFx
backend or handler, but find that their method has a Graphite backend. In that
situation, customers configure their preferred collector(s) to use the Graphite
backend, then send the Graphite-formatted traffic to the SignalFx proxy, which is
configured with a Graphite listener and a SignalFx forwarder.

### How much traffic can the proxy handle?

We have benchmarked the proxy against a mid-sized, compute-optimized instance on
Amazon Web Services, the c3.2xlarge. As the only service on such an instance, it
comfortably processes in excess of 6 million datapoints per minute.

### How do I monitor the proxy?

The proxy emits a number of metrics to SignalFx that you can monitor for its
health and performance. For the cumulative counters, we recommend that you use
the default (rate/sec) rollup. A partial list of the more commonly watched metrics
follows.

|Metric|Description|
|------|-----------|
|Alloc|Gauge for proxy memory allocated. To use, filter on `source:proxy` and change Settings (under the ‘gear’ icon) to use IEC units.|
|active_connections|Gauge for number of active connections that a proxy has (for traffic it is listening to) or is making (to the SignalFx service). To see what is inbound to the proxy, for example, filter on `source:proxy` and `location:listener`. Apply an analytics function of `Sum:aggregation` and group by `type` and `name` to see more details on the type of traffic.|
|datapoints_backup_size|Gauge for size of queue backup within proxy. Contains dimensions for type of traffic and location. If this metric grows rapidly, it can be an indicator of an outbound networking issue.|
|dropped_points|Cumulative counter for number of datapoints dropped per second. To use with proxy, make sure to filter on `source:proxy` and on the `location` (listener, middle or forwarder).|
|num_goroutine|Gauge for number of Go routines per proxy. To use, filter on `source:proxy`.|
|total_connections|Cumulative counter for total connections that a proxy has (for traffic it is listening to) or is making (to the SignalFx service). To see what is inbound to the proxy, for example, filter on `source:proxy` and `location:listener`. Apply an analytics function of `Sum:aggregation` and group by `type` and `name` to see more details on the type of traffic.|
|total_datapoints|Cumulative counter for number of datapoints received by the proxy per second. To use with proxy, filter on `source:proxy` and `location:middle`. If you have more than one proxy, applying an analytics function of `Sum:aggregation` and grouping by the dimension host will let you distinguish between the various proxies.|

### The proxy has stopped sending datapoints. Help me troubleshoot!

The most common areas to look into are as follows: 

**1. Is the proxy hardware or instance sized properly?** Depending on the volume
of traffic that you are sending, you may need a larger box. (See above for the
benchmarking we’ve done.)

**2. Are the buffers full?** Look in the proxy logs for messages that indicate the
buffer is full for one or more forwarders in the proxy:

`level=warning msg="Unable to process datapoints" err="unable to send more datapoints.  Buffer full”`

To address this, adjust the buffer size in the proxy’s performance options, as
documented [above](#perf).

**3. Is your outbound network experiencing issues?** Network latency can obviously
affect datapoint submission; it can also cause the proxy to get backed up. One way
to debug this is to look at the datapoints_backup_size metric, as documented above.
If this metric has shot up suddenly, there’s a good chance that datapoints are not
exiting the proxy at a fast enough rate.

### What is your high availability strategy for the proxy?

The proxy is stateless, and can be put behind a load balancer for HA purposes.
Using a load balancer is also a good way to submit a volume of metrics that exceeds
what a single proxy server can process.
