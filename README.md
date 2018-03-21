# metricproxy [![Circle CI](https://circleci.com/gh/signalfx/metricproxy.svg?style=svg)](https://circleci.com/gh/signalfx/metricproxy)

The SignalFx Metricproxy lets you aggregate metrics and send them to
SignalFx. It is a multilingual datapoint demultiplexer that can accept
time series data from the carbon (Graphite), collectd or SignalFx protocols
and emit those datapoints to a series of servers using the carbon, collectd
or SignalFx protocols. We recommend placing the proxy either on the same
server as another existing metrics aggregator or on a central server that
is already receiving datapoints, such as Graphite's carbon database.

## Install and upgrade

```
$ curl -s \
https://raw.githubusercontent.com/signalfx/metricproxy/master/install.sh\
| sudo sh

# Config at    /etc/sfdbconfig.conf
# Binary at    /opt/sfproxy/bin/metricproxy
# Logs at      /var/log/sfproxy
# PID file at  /var/run/metricproxy.pid
```

### Go Dependency

Additional Steps are required to install the metricproxy on Ubuntu machines
because the golang package in Ubuntu is not up to date. The following steps
to [update](https://github.com/golang/go/wiki/Ubuntu) the golang package on
Ubuntu must be executed before running the install script.

```
sudo add-apt-repository ppa:gophers/archive
sudo apt-get update
sudo apt-get install golang-1.10
```

(If /usr/bin/go does NOT exist then create a symbolic link)

```
sudo ln /usr/lib/go-1.10/bin/go /usr/bin/go
```

OR
(If /usr/bin/go does already exist, then overwrite the older binary)

```
sudo cp /usr/lib/go-1.10/bin/go /usr/bin/go
```

## Running

```
/etc/init.d/metricproxy start
```

## Stopping the daemon

```
/etc/init.d/metricproxy stop
```

## Debugging

```
cd /var/log/sfproxy
tail -F *
```

## Sizing

We are currently running 118,358 DPS (7,080,000 DPM) on a c3.2xlarge with 70%
idle CPU (the metric to watch).  I would recommend a server comparable to a
c3.2xlarge for safety if running around 7 million DPM and a c3.xlarge for
around 2-3M DPM.  One can start with a server comparable to a c3.xlarge
and monitor idle CPU to see if this should be increased to a c3.2xlarge comparable
server.

## Http Proxy Support

The go http code supports the use of an http proxy through the environment
variable `HTTP_PROXY="http://proxyhost:proxyport"`. With this the metricproxy
will proxy all http connections through that host and port. This can be put
into the start script, or as part of the environment sent into the container
if using a container solution like maestro.

## Code layout

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

## Development

If you want to submit patches for the proxy, make sure your code passes
[travis_check.sh](travis_check.sh) with exit code 0.  For help setting
up your development enviroment, it should be enough to mirror the install
steps of [.travis.yml](.travis.yml).  You may need to make sure your GOPATH
env variable is set correctly.

## Docker

The proxy comes with a [docker image](Dockerfile) that is built and deployed
to [quay.io](https://quay.io/repository/signalfx/metricproxy).  It assumes
you will have a sfdbproxy.conf file cross mounted to
/var/config/sfproxy/sfdbproxy.conf for the docker container.

## Config file format

See the [example config](exampleSfdbproxy.conf) file for an example of how
configuration looks.  Configuration is a JSON file with two important fields:
ListenFrom and ForwardTo.

### ListenFrom

ListenFrom is where you define what services the proxy will pretend to be and
what ports to listen for those services on.

#### collectd

You can receive data sent by CollectD by setting up a `collectd` endpoint.
For this, you will need to specify which port to bind to.  An example config:

```
        {
            "ListenAddr": "0.0.0.0:18000",
            "Type": "collectd"
        }
```

When configuring CollectD, the target URL path will be `/post-collectd`.

#### signalfx (for read)

You can expose a SignalFx endpoint with the `signalfx` type. A SignalFx
endpoint can listen on all the signalfx protocols in both "Protocol Buffers"
and "JSON" for events and datapoints (e.g. /v2/datapoint, /v2/event). It can
also listen on the collectd protocol (/v1/collectd).

For this, you will need to specify which port to bind to.  An example config:

```
        {
            "ListenAddr": "0.0.0.0:18080",
            "Type": "signalfx"
        }
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

You can optionally choose to listen to carbon over UDP as well (as opposed
to the default of TCP).

```
        {
            "ListenAddr": "0.0.0.0:12003",
            "Type": "carbon",
            "Protocol": "udp"
        }
```

#### prometheus

You can use the metricproxy as prometheus remote storage. To do this, you will
need to specify the port to bind to.  An example config:

```
        {
            "ListenAddr": "0.0.0.0:12003",
            "Type": "prometheus",
            "ListenPath": "/write"
        }
```

...and then add the following to your prometheus.yml:

```

remote_write:
  - url: "http://hostname:12003/write"
```

If you want someting different than the default endpoint of "/write" you can
specify it with "ListenPath". An alternative example config:

```
        {
            "ListenAddr": "0.0.0.0:12003",
            "Type": "prometheus",
            "ListenPath": "/receive"
        }
```

...and then add the following to your prometheus.yml:

```

remote_write:
  - url: "http://hostname:12003/receive"
```

#### wavefront

You can send wavefront metrics to signalfx through our proxy in the same
fashion as you would have sent them to the wavefront proxy. You will need
to specify the port to bind to.  An example config:

```
        {
            "ListenAddr": "0.0.0.0:12878",
            "Type": "wavefront",
            "ExtractCollectdDimensions": "false"
        }
```

You can optionally choose to decode dimensions like we do from collectd style
metrics (although it's applied to all metrics coming in) by changing
`ExtractCollectdDimensions` to true. Default is true. If you were encoding
dimensions using the `[foo=bar]` sytnax inside instance and host fields, this
will continue to give you the dimensions you expect.

```
        {
            "ListenAddr": "0.0.0.0:12878",
            "Type": "wavefront",
            "ExtractCollectdDimensions": "true"
        }
```

#### common properties

All listeners support a "Dimensions" property which is expected to be a
map(string => string) and adds the dimensions to all points sent.  For example:

        {
            "ListenAddr": "0.0.0.0:18080",
            "Dimensions": { "env": "prod" },
            "Type": "signalfx"
        }

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
        }
```

#### signalfx (for write)

You can write datapoints to SignalFx with this endpoint.  You will need to
configure your auth token inside DefaultAuthToken.

```
        {
            "type": "signalfx",
            "DefaultAuthToken": "___AUTH_TOKEN___",
            "Name": "testproxy"
        }
```

By default gzip compression talking to SignalFx is turned on, if for some
reason you want to turn it off you can disable it in the SigalFx forward config
like this:

```
        {
            "type": "signalfx",
            "DefaultAuthToken": "___AUTH_TOKEN___",
            "Name": "testproxy",
            "DisableCompression" :true
        }
```

## Example configs

### Basic

This config will listen for graphite metrics on port 2003 and forward them
to signalfx with the token ABCD.  It will also report local stats
to signalfx at 1s intervals

```
{
  "StatsDelay": "1s",
  "LogDir": "/var/log/sfproxy",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr" : "0.0.0.0:2003",
      "Timeout" : "2m"
    }
  ],

  "ForwardTo": [
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
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
      "ListenAddr" : "0.0.0.0:8081"
    }
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

### Graphite Dimensions

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
    }
  ],

  "ForwardTo": [
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
  ]
}
```

### Graphite Dimensions using Regular Expressions

You can use MetricRules to extract dimensions and metric names from the dot-
separated names of graphite metrics using regular expressions.

A metric will be matched to the first rule that matches the regular expression.
If no groups are specified the entire metric will be used as the metric name
and no dimensions will be parsed out. All groups should be named. If the named
group starts with sf_metric it will be appended together to form the metric
name, otherwise it will become a dimension with the name of the group name,
and the value of what it matches.

For each rule, you can define the following:

1. Regex - REQUIRED - regular expression with optionally named matching groups
1. AdditionalDimensions - used to add static dimensions to every metric that
   matches this rule
1. MetricType - to set the specific type of metric this is; default is gauge
1. MetricName - if present this will be the first part of the metricName.  If
   no named groups starting with sf_metric are specified, this will be the
   entire metric name.

e.g.

```
{
  "Type": "carbon",
  "ListenAddr": "0.0.0.0:2003",
  "MetricDeconstructor": "regex",
  "MetricDeconstructorOptionsJSON": {
    "FallbackDeconstructor": "nil",
    "MetricRules": [
      {
        "Regex": "(?P<sf_metric_0>foo.*)\\.(?P<middle>.*)(?P<sf_metric_1>\\.baz)",
        "AdditionalDimensions": {
          "key": "value"
        }
      },
      {
        "Regex": "(?P<sf_metric>counter.*)",
        "MetricType": "cumulative_counter"
      },
      {
        "Regex": "madeup.*",
        "MetricName": "synthetic.metric"
      },
      {
        "Regex": "common.*"
      }
    ]
  }
}
```

In the above example, if you sent in the metric foo.bar.baz it would match the
first rule and the metric name would become foo.baz with a dimensions of
"middle":"bar", and then an additional metric with "key":"value" added and the
type would be the default of gauge.

If you sent in the metric "counter.page_views" the resulting metric name would
continue to be "counter.page_views" (because you named it sf_metric)_but have
the type of cumulative counter.  No dimensions are being extracted or added in
this example.

If you sent in the metric "madeup.page_faults" the resulting metric name would
be "synthetic.metric" with type gauge.

If you sent in the metric "common.page_load_max", the resulting metric name
would continue to be "common.page_load_max" (because no groups were specified)
of type gauge.

If you sent in the metric "albatros.cpu.idle", this would fall through and go
to the FallbackDeconstructor and in this case since we're using the nil
deconstructor, be rejected and won't be passed on to SignalFx.

### Graphite Dimensions using Delimiters

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
      "type": "signalfx",
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
            "MetricType": "counter",
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
      "type": "signalfx",
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

### SignalFx perf options

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
    }
  ],

  "ForwardTo": [
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder",
      "BufferSize": 1000000,
      "DrainingThreads": 50,
      "MaxDrainSize": 5000
    }
  ]
}
```

### CollectD listener dimensions

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
    }
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

### SignalFx to SignalFx

This config listens using the signalfx protocol, buffers, then forwards
points to signalfx.

```
{
  "StatsDelay": "1s",
  "ListenFrom": [
    {
      "Type": "signalfx",
      "ListenAddr" : "0.0.0.0:8080"
    }
  ],

  "ForwardTo": [
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
  ]
}
```

### Teeing a subset of metrics on a forwarder

The config listens on signalfx and graphite, and forwards everything to
graphite, and a smaller subset (excludes anything starting with cpu) to SignalFx.
Below any metric starting with cpu will be denied, except for cpu.idle.
If only allow was specificed, those that matched would be allowed and those
that failed would be denied.  If only deny was provided those that matched
would be denied and those that were not would be allowed.

```
{
  "StatsDelay": "1s",
  "LogDir": "/tmp",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr": "0.0.0.0:2003"
    },
    {
      "Type": "signalfx",
      "ListenAddr": "0.0.0.0:8080"
    }
  ],
  "ForwardTo": [
    {
      "type": "carbon",
      "Name": "ourcarbon",
      "Host": "example.com",
      "Port": 2003
    },
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder",
      "Filters": {
        "Deny": [
          "^cpu"
        ],
        "Allow": [
          "^cpu.idle$"
        ]
      }
    }
  ]
}
```

### Status Page and profiling

This config only loads a status page.  You can see configuration information at
`http://localhost:6009/debug/vars`, explore the objects in memory at
`http://localhost:6009/debug/explorer/`, and pprof information at
`http://localhost:6009/debug/pprof/`.  You can learn more about pprof for golang
on [the pprof help page](http://golang.org/pkg/net/http/pprof/).

```
{
  "LocalDebugServer": "0.0.0.0:6009"
}
```

### Health checks

Health checks are available on the listening port of any collectd or
signalfx listener. E.g.  If you had a signalfx listener at 8080, the
healthcheck would be located at `http://localhost:8080/healthz`.
Healthchecks are useful when putting the proxy behind a loadbalancer.

### Graceful Shutdown

The below config specifies what are the default values for the graceful
shutdown parameters.  Upon receiving a SIGTERM, the graceful shutdown
procedure will close all health checks to prevent a loadbalancer from
initiating any new connections. Now it will check every
GracefulCheckInterval to see if the number of in flight datapoints and
events is 0.  If it then stays 0 for SilentGracefulTime, or the entire
graceful shutdown takes longer than MaxGracefulWaitTime the listeners
and forwarders will be closed and the process will exit.

If yo're in front of a load balancer we recommend a MaxGracefulWaitTime
of "30s".  This gives the lb time to hit the health check and divert
traffic.

If you're not in front of a load balancer you will always hit the
MaxGracefulWaitTime so you probably want to set that to something
low like 1s.  The config below is this use case.

```
{
  "MaxGracefulWaitTime": "1s",
  "GracefulCheckInterval": "1s",
  "SilentGracefulTime": "2s",
  "StatsDelay": "1s",
  "LogDir": "/tmp",
  "ListenFrom": [
    {
      "Type": "carbon",
      "ListenAddr": "0.0.0.0:2003"
    },
    {
      "Type": "signalfx",
      "ListenAddr": "0.0.0.0:8080"
    }
  ],
  "ForwardTo": [
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
  ]
}
```

### Debugging connections via headers

Setup a debug config

```
{
  "DebugFlag": "secretdebug",
  "ForwardTo": [
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
}
```

Then, send a request with the debug header set to secretdebug.

```
curl -H "X-Debug-Id:secretdebug" -H "Content-Type: application/json" -XPOST \
   -d '{"gauge": [{"metric":"bob", "dimensions": {"org":"dev"}, "value": 3}]}' localhost:8080/v2/datapoint
```

The config will tell the HTTP request to debug each datapoint sent with X-Debug-Id
set to secretdebug and log statements will show when each item is through the
proxy pipeline.

### Debugging connections via debug dimensions

Setup a local debug server, then you can configure which dimensions are
logged out.

```
{
  "LocalDebugServer": "0.0.0.0:6060",
  "ForwardTo": [
    {
      "type": "signalfx",
      "DefaultAuthToken": "ABCD",
      "Name": "signalfxforwarder"
    }
}
```

Then set which dimensions to debug via a POST.

```
curl -XPOST -d '{"org":"dev"}' localhost:6060/debug/dims
```

Then, any datapoints with the "org" dimension of "dev" will be logged.
