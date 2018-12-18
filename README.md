# Gateway [![Circle CI](https://circleci.com/gh/signalfx/gateway.svg?style=svg)](https://circleci.com/gh/signalfx/gateway)

The SignalFx Gateway lets you aggregate metrics and send them to
SignalFx. It is a multilingual datapoint demultiplexer that can accept
time series data from the carbon (Graphite), collectd or SignalFx protocols
and emit those datapoints to a series of servers using the carbon, collectd
or SignalFx protocols. We recommend placing the gateway either on the same
server as another existing metrics aggregator or on a central server that
is already receiving datapoints, such as Graphite's carbon database.

## Usage and Configuration

Please refer to the usage and configuration documentation located [here](https://github.com/signalfx/integrations/tree/master/gateway) 
for more information on operating the SignalFx Gateway.

## Development

If you want to submit patches for the gateway, make sure your code passes
[travis_check.sh](travis_check.sh) with exit code 0.  For help setting
up your development environment, it should be enough to mirror the install
steps of [.travis.yml](.travis.yml).  You may need to make sure your GOPATH
env variable is set correctly.

## Code layout

You only need to read this if you want to develop the gateway or understand
the gateway's code.

The gateway is divided into two main components: [forwarder](protocol/carbon/carbonforwarder.go)
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


## Config file format

See the [example config](exampleGateway.conf) file for an example of how
configuration looks.  Configuration is a JSON file with two important fields:
ListenFrom and ForwardTo.

## License

The SignalFx Gateway is released under the Apache 2.0 license. See [LICENSE](./LICENSE) for more details.
