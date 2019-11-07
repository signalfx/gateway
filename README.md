# Gateway [![Circle CI](https://circleci.com/gh/signalfx/gateway.svg?style=svg)](https://circleci.com/gh/signalfx/gateway)

The SignalFx Gateway lets you aggregate metrics and send them to
SignalFx. It is a multilingual datapoint demultiplexer that can accept
time series data from the carbon (Graphite), collectd or SignalFx protocols
and emit those datapoints to a series of servers using the carbon, collectd
or SignalFx protocols. We recommend placing the gateway either on the same
server as another existing metrics aggregator or on a central server that
is already receiving datapoints, such as Graphite's carbon database.

The SignalFx Gateway also acts as a simple proxy and forwarder for SignalFx's
Events Ingest and Trace Ingest APIs.

## Usage and Configuration

Please refer to the usage and configuration documentation located
[here](https://github.com/signalfx/integrations/tree/release/gateway) for more
information on operating the SignalFx Gateway.

## Development

If you want to submit patches for the gateway, make sure your code passes
[travis_check.sh](travis_check.sh) with exit code 0. For help setting up your
development environment, it should be enough to mirror the install steps of
[.travis.yml](.travis.yml). You may need to make sure your GOPATH env variable
is set correctly.

## Code layout

You only need to read this if you want to develop the gateway or understand the
gateway's code.

The gateway is divided into two main components:
[forwarder](protocol/carbon/carbonforwarder.go) and
[listener](protocol/carbon/carbonlistener.go). The forwarder and listener are
glued together by the [demultiplexer](protocol/demultiplexer/demultiplexer.go).

When a listener receives a datapoint, it converts the datapoint into a basic
[datapoint type](datapoint/datapoint.go). This core datapoint type is then sent
to the multiplexer that will send a pointer to that datapoint to each forwarder.

Sometimes there is a loss of fidelity during transmission if a listener and
forwarder don't support the same options. While it's impossible to make
something understand an option it does not, we don't want to forget support for
this option when we translate a datapoint through the multiplexer. We work
around this by sometimes encoding the raw representation of the datapoint into
the Datapoint object we forward. For example, points from carbon are not only
translated into our core datapoint format, but also support
[ToCarbonLine](protocol/carbon/carbon.go) which allows us to directly convert
the abstract datapoint into what it looked like for carbon, which allows us to
forward the point to another carbon database exactly as we received it.

All message passing between forwarders, multiplexer, and listeners happen on
golang's built in channel abstraction.

## Config file format

See the [example config](exampleGateway.conf) file for an example of how
configuration looks. Configuration is a JSON file with two important fields:
`ListenFrom` and `ForwardTo`.

## License

The SignalFx Gateway is released under the Apache 2.0 license. See
[LICENSE](./LICENSE) for more details.

## Dependencies

_Note: this list of dependencies applies to both the SignalFx Gateway and its
Smart Gateway variant._

Name | License
---- | -------
`github.com/apache/thrift/lib/go/thrift`                     | [Apache Software License v2.0](https://github.com/apache/thrift/tree/master/LICENSE)
`github.com/beorn7/perks/quantile`                           | [MIT](https://github.com/beorn7/perks/tree/master/LICENSE)
`github.com/coreos/bbolt`                                    | [MIT](https://github.com/etcd-io/bbolt/tree/master/LICENSE)
`github.com/coreos/etcd`                                     | [Apache Software License v2.0](https://github.com/etcd-io/etcd/tree/master/LICENSE)
`github.com/coreos/go-semver/semver`                         | [Apache Software License v2.0](https://github.com/coreos/go-semver/tree/master/LICENSE)
`github.com/coreos/go-systemd/journal`                       | [Apache Software License v2.0](https://github.com/coreos/go-systemd/tree/master/LICENSE)
`github.com/coreos/pkg/capnslog`                             | [Apache Software License v2.0](https://github.com/coreos/pkg/tree/master/LICENSE)
`github.com/davecgh/go-spew/spew`                            | [ISC](https://github.com/davecgh/go-spew/tree/master/LICENSE)
`github.com/dgrijalva/jwt-go`                                | [MIT](https://github.com/dgrijalva/jwt-go/tree/master/LICENSE)
`github.com/ghodss/yaml`                                     | [MIT](https://github.com/ghodss/yaml/tree/master/LICENSE)
`github.com/go-logfmt/logfmt`                                | [MIT](https://github.com/go-logfmt/logfmt/tree/master/LICENSE)
`github.com/go-stack/stack`                                  | [MIT](https://github.com/go-stack/stack/tree/master/LICENSE.md)
`github.com/gobwas/glob`                                     | [MIT](https://github.com/gobwas/glob/tree/master/LICENSE)
`github.com/gogo/protobuf`                                   | [BSD 3-Clause](https://github.com/gogo/protobuf/tree/master/LICENSE)
`github.com/golang/groupcache/lru`                           | [Apache Software License v2.0](https://github.com/golang/groupcache/tree/master/LICENSE)
`github.com/golang/protobuf`                                 | [BSD 3-Clause](https://github.com/golang/protobuf/tree/master/LICENSE)
`github.com/golang/snappy`                                   | [BSD 3-Clause](https://github.com/golang/snappy/tree/master/LICENSE)
`github.com/google/btree`                                    | [Apache Software License v2.0](https://github.com/google/btree/tree/master/LICENSE)
`github.com/google/go-cmp`                                   | [BSD 3-Clause](https://github.com/google/go-cmp/tree/master/LICENSE)
`github.com/gopherjs/gopherjs/js`                            | [BSD 2-Clause](https://github.com/gopherjs/gopherjs/tree/master/LICENSE)
`github.com/gorilla/context`                                 | [BSD 3-Clause](https://github.com/gorilla/context/tree/master/LICENSE)
`github.com/gorilla/mux`                                     | [BSD 3-Clause](https://github.com/gorilla/mux/tree/master/LICENSE)
`github.com/gorilla/websocket`                               | [BSD 2-Clause](https://github.com/gorilla/websocket/tree/master/LICENSE)
`github.com/grpc-ecosystem/go-grpc-middleware`               | [Apache Software License v2.0](https://github.com/grpc-ecosystem/go-grpc-middleware/tree/master/LICENSE)
`github.com/grpc-ecosystem/go-grpc-prometheus`               | [Apache Software License v2.0](https://github.com/grpc-ecosystem/go-grpc-prometheus/tree/master/LICENSE)
`github.com/grpc-ecosystem/grpc-gateway`                     | [BSD 3-Clause](https://github.com/grpc-ecosystem/grpc-gateway/tree/master/LICENSE.txt)
`github.com/inconshreveable/mousetrap`                       | [Apache Software License v2.0](https://github.com/inconshreveable/mousetrap/tree/master/LICENSE)
`github.com/jaegertracing/jaeger/thrift-gen/jaeger`          | [Apache Software License v2.0](https://github.com/jaegertracing/jaeger/tree/master/LICENSE)
`github.com/jonboulle/clockwork`                             | [Apache Software License v2.0](https://github.com/jonboulle/clockwork/tree/master/LICENSE)
`github.com/jtolds/gls`                                      | [MIT](https://github.com/jtolds/gls/tree/master/LICENSE)
`github.com/konsorten/go-windows-terminal-sequences`         | [MIT](https://github.com/konsorten/go-windows-terminal-sequences/tree/master/LICENSE)
`github.com/kr/logfmt`                                       | [MIT](https://github.com/kr/logfmt/Readme)
`github.com/mailru/easyjson`                                 | [MIT](https://github.com/mailru/easyjson/tree/master/LICENSE)
`github.com/matttproud/golang_protobuf_extensions/pbutil`    | [Apache Software License v2.0](https://github.com/matttproud/golang_protobuf_extensions/tree/master/LICENSE)
`github.com/mdubbyap/timespan`                               | [MIT](https://github.com/mdubbyap/timespan/tree/master/LICENSE)
`github.com/opentracing/opentracing-go`                      | [Apache Software License v2.0](https://github.com/opentracing/opentracing-go/tree/master/LICENSE)
`github.com/pkg/errors`                                      | [BSD 2-Clause](https://github.com/pkg/errors/tree/master/LICENSE)
`github.com/pmezard/go-difflib/difflib`                      | [BSD 3-Clause](https://github.com/pmezard/go-difflib/tree/master/LICENSE)
`github.com/prometheus/client_golang/prometheus`             | [Apache Software License v2.0](https://github.com/prometheus/client_golang/tree/master/LICENSE)
`github.com/prometheus/client_model/go`                      | [Apache Software License v2.0](https://github.com/prometheus/client_model/tree/master/LICENSE)
`github.com/prometheus/common`                               | [Apache Software License v2.0](https://github.com/prometheus/common/tree/master/LICENSE)
`github.com/prometheus/procfs`                               | [Apache Software License v2.0](https://github.com/prometheus/procfs/tree/master/LICENSE)
`github.com/prometheus/prometheus/prompb`                    | [Apache Software License v2.0](https://github.com/prometheus/prometheus/tree/master/LICENSE)
`github.com/signalfx/com_signalfx_metrics_protobuf`          | [Apache Software License v2.0](https://github.com/signalfx/com_signalfx_metrics_protobuf/tree/master/LICENSE)
`github.com/signalfx/embetcd/embetcd`                        | [Apache Software License v2.0](https://github.com/signalfx/embetcd/tree/master/LICENSE)
`github.com/signalfx/go-distribute`                          | [Apache Software License v2.0](https://github.com/signalfx/go-distribute/tree/master/LICENSE)
`github.com/signalfx/go-metrics`                             | [BSD 3-Clause](https://github.com/signalfx/go-metrics/tree/master/LICENSE)
`github.com/signalfx/gohistogram`                            | [MIT](https://github.com/signalfx/gohistogram/tree/master/LICENSE)
`github.com/signalfx/golib`                                  | [Apache Software License v2.0](https://github.com/signalfx/golib/tree/master/LICENSE)
`github.com/signalfx/ondiskencoding`                         | [Apache Software License v2.0](https://github.com/signalfx/ondiskencoding/tree/master/LICENSE)
`github.com/mdubbyap/tdigest`                                | [Apache Software License v2.0](https://github.com/mdubbyap/tdigest/tree/master/LICENSE)
`github.com/signalfx/xdgbasedir`                             | [Apache Software License v2.0](https://github.com/signalfx/xdgbasedir/tree/master/LICENSE)
`github.com/sirupsen/logrus`                                 | [MIT](https://github.com/sirupsen/logrus/tree/master/LICENSE)
`github.com/smartystreets/assertions`                        | [MIT](https://github.com/smartystreets/assertions/tree/master/LICENSE.md)
`github.com/smartystreets/goconvey`                          | [MIT](https://github.com/smartystreets/goconvey/tree/master/LICENSE.md)
`github.com/soheilhy/cmux`                                   | [Apache Software License v2.0](https://github.com/soheilhy/cmux/tree/master/LICENSE)
`github.com/spaolacci/murmur3`                               | [BSD 3-Clause](https://github.com/spaolacci/murmur3/tree/master/LICENSE)
`github.com/spf13/cobra`                                     | [Apache Software License v2.0](https://github.com/spf13/cobra/tree/master/LICENSE.txt)
`github.com/spf13/pflag`                                     | [BSD 3-Clause](https://github.com/spf13/pflag/tree/master/LICENSE)
`github.com/stretchr/testify/assert`                         | [MIT](https://github.com/stretchr/testify/tree/master/LICENSE)
`github.com/tmc/grpc-websocket-proxy/wsproxy`                | [MIT](https://github.com/tmc/grpc-websocket-proxy/tree/master/LICENSE)
`github.com/uber-go/atomic`                                  | [MIT](https://github.com/uber-go/atomic/tree/master/LICENSE.txt)
`github.com/uber/tchannel-go`                                | [MIT](https://github.com/uber/tchannel-go/blob/dev/LICENSE.md)
`github.com/ugorji/go`                                       | [MIT](https://github.com/ugorji/go/tree/master/LICENSE)
`github.com/xiang90/probing`                                 | [MIT](https://github.com/xiang90/probing/tree/master/LICENSE)
`go.uber.org/atomic`                                         | [MIT](https://github.com/uber-go/atomic/tree/master/LICENSE.txt)
`go.uber.org/multierr`                                       | [MIT](https://github.com/uber-go/multierr/tree/master/LICENSE.txt)
`go.uber.org/zap`                                            | [MIT](https://github.com/uber-go/zap/tree/master/LICENSE.txt)
`golang.org/x/crypto`                                        | [BSD 3-Clause](https://github.com/golang/crypto/tree/master/LICENSE)
`golang.org/x/net`                                           | [BSD 3-Clause](https://github.com/golang/net/tree/master/LICENSE)
`golang.org/x/sys`                                           | [BSD 3-Clause](https://github.com/golang/sys/tree/master/LICENSE)
`golang.org/x/text`                                          | [BSD 3-Clause](https://github.com/golang/text/tree/master/LICENSE)
`golang.org/x/time`                                          | [BSD 3-Clause](https://github.com/golang/time/tree/master/LICENSE)
`google.golang.org/genproto`                                 | [Apache Software License v2.0](https://github.com/google/go-genproto/tree/master/LICENSE)
`google.golang.org/grpc`                                     | [Apache Software License v2.0](https://github.com/grpc/grpc-go/tree/master/LICENSE)
`gopkg.in/logfmt.v0`                                         | [MIT](https://github.com/go-logfmt/logfmt/blob/v0.4.0/LICENSE)
`gopkg.in/natefinch/lumberjack.v2`                           | [MIT](https://github.com/natefinch/lumberjack/blob/v2.1/LICENSE)
`gopkg.in/stack.v1`                                          | [MIT](https://github.com/go-stack/stack/blob/v1.8.0/LICENSE.md)
`gopkg.in/yaml.v2`                                           | [Apache Software License v2.0](https://github.com/go-yaml/yaml/blob/v2.2.2/LICENSE)
`gotest.tools/assert`                                        | [Apache Software License v2.0](https://github.com/gotestyourself/gotest.tools/tree/master/LICENSE)
`gotest.tools`                                               | [Apache Software License v2.0](https://github.com/gotestyourself/gotest.tools/tree/master/LICENSE)
`stathat.com/c/consistent`                                   | [MIT](https://github.com/stathat/consistent/tree/master/LICENSE)
