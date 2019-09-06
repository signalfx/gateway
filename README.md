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
`github.com/apache/thrift/lib/go/thrift`                     | [Apache Software License v2.0](./vendor/github.com/apache/thrift/LICENSE)
`github.com/beorn7/perks/quantile`                           | [MIT](./vendor/github.com/beorn7/perks/LICENSE)
`github.com/coreos/bbolt`                                    | [MIT](./vendor/github.com/etcd-io/bbolt/LICENSE)
`github.com/coreos/etcd`                                     | [Apache Software License v2.0](./vendor/github.com/etcd-io/etcd/LICENSE)
`github.com/coreos/go-semver/semver`                         | [Apache Software License v2.0](./vendor/github.com/coreos/go-semver/LICENSE)
`github.com/coreos/go-systemd/journal`                       | [Apache Software License v2.0](./vendor/github.com/coreos/go-systemd/LICENSE)
`github.com/coreos/pkg/capnslog`                             | [Apache Software License v2.0](./vendor/github.com/coreos/pkg/LICENSE)
`github.com/davecgh/go-spew/spew`                            | [ISC](./vendor/github.com/davecgh/go-spew/LICENSE)
`github.com/dgrijalva/jwt-go`                                | [MIT](./vendor/github.com/dgrijalva/jwt-go/LICENSE)
`github.com/ghodss/yaml`                                     | [MIT](./vendor/github.com/ghodss/yaml/LICENSE)
`github.com/go-logfmt/logfmt`                                | [MIT](./vendor/github.com/go-logfmt/logfmt/LICENSE)
`github.com/go-stack/stack`                                  | [MIT](./vendor/github.com/go-stack/stack/LICENSE.md)
`github.com/gobwas/glob`                                     | [MIT](./vendor/github.com/gobwas/glob/LICENSE)
`github.com/gogo/protobuf`                                   | [BSD 3-Clause](./vendor/github.com/gogo/protobuf/LICENSE)
`github.com/golang/groupcache/lru`                           | [Apache Software License v2.0](./vendor/github.com/golang/groupcache/LICENSE)
`github.com/golang/protobuf`                                 | [BSD 3-Clause](./vendor/github.com/golang/protobuf/LICENSE)
`github.com/golang/snappy`                                   | [BSD 3-Clause](./vendor/github.com/golang/snappy/LICENSE)
`github.com/google/btree`                                    | [Apache Software License v2.0](./vendor/github.com/google/btree/LICENSE)
`github.com/google/go-cmp`                                   | [BSD 3-Clause](./vendor/github.com/google/go-cmp/LICENSE)
`github.com/gopherjs/gopherjs/js`                            | [BSD 2-Clause](./vendor/github.com/gopherjs/gopherjs/LICENSE)
`github.com/gorilla/context`                                 | [BSD 3-Clause](./vendor/github.com/gorilla/context/LICENSE)
`github.com/gorilla/mux`                                     | [BSD 3-Clause](./vendor/github.com/gorilla/mux/LICENSE)
`github.com/gorilla/websocket`                               | [BSD 2-Clause](./vendor/github.com/gorilla/websocket/LICENSE)
`github.com/grpc-ecosystem/go-grpc-middleware`               | [Apache Software License v2.0](./vendor/github.com/grpc-ecosystem/go-grpc-middleware/LICENSE)
`github.com/grpc-ecosystem/go-grpc-prometheus`               | [Apache Software License v2.0](./vendor/github.com/grpc-ecosystem/go-grpc-prometheus/LICENSE)
`github.com/grpc-ecosystem/grpc-gateway`                     | [BSD 3-Clause](./vendor/github.com/grpc-ecosystem/grpc-gateway/LICENSE.txt)
`github.com/inconshreveable/mousetrap`                       | [Apache Software License v2.0](./vendor/github.com/inconshreveable/mousetrap/LICENSE)
`github.com/jaegertracing/jaeger/thrift-gen/jaeger`          | [Apache Software License v2.0](./vendor/github.com/jaegertracing/jaeger/LICENSE)
`github.com/jonboulle/clockwork`                             | [Apache Software License v2.0](./vendor/github.com/jonboulle/clockwork/LICENSE)
`github.com/jtolds/gls`                                      | [MIT](./vendor/github.com/jtolds/gls/LICENSE)
`github.com/konsorten/go-windows-terminal-sequences`         | [MIT](./vendor/github.com/konsorten/go-windows-terminal-sequences/LICENSE)
`github.com/kr/logfmt`                                       | [MIT](./vendor/github.com/kr/logfmt/Readme)
`github.com/mailru/easyjson`                                 | [MIT](./vendor/github.com/mailru/easyjson/LICENSE)
`github.com/matttproud/golang_protobuf_extensions/pbutil`    | [Apache Software License v2.0](./vendor/github.com/matttproud/golang_protobuf_extensions/LICENSE)
`github.com/mdubbyap/timespan`                               | [MIT](./vendor/github.com/mdubbyap/timespan/LICENSE)
`github.com/opentracing/opentracing-go`                      | [Apache Software License v2.0](./vendor/github.com/opentracing/opentracing-go/LICENSE)
`github.com/pkg/errors`                                      | [BSD 2-Clause](./vendor/github.com/pkg/errors/LICENSE)
`github.com/pmezard/go-difflib/difflib`                      | [BSD 3-Clause](./vendor/github.com/pmezard/go-difflib/LICENSE)
`github.com/prometheus/client_golang/prometheus`             | [Apache Software License v2.0](./vendor/github.com/prometheus/client_golang/LICENSE)
`github.com/prometheus/client_model/go`                      | [Apache Software License v2.0](./vendor/github.com/prometheus/client_model/LICENSE)
`github.com/prometheus/common`                               | [Apache Software License v2.0](./vendor/github.com/prometheus/common/LICENSE)
`github.com/prometheus/procfs`                               | [Apache Software License v2.0](./vendor/github.com/prometheus/procfs/LICENSE)
`github.com/prometheus/prometheus/prompb`                    | [Apache Software License v2.0](./vendor/github.com/prometheus/prometheus/LICENSE)
`github.com/signalfx/com_signalfx_metrics_protobuf`          | [Apache Software License v2.0](./vendor/github.com/signalfx/com_signalfx_metrics_protobuf/LICENSE)
`github.com/signalfx/embetcd/embetcd`                        | [Apache Software License v2.0](./vendor/github.com/signalfx/embetcd/LICENSE)
`github.com/signalfx/go-distribute`                          | [Apache Software License v2.0](./vendor/github.com/signalfx/go-distribute/LICENSE)
`github.com/signalfx/go-metrics`                             | [BSD 3-Clause](./vendor/github.com/signalfx/go-metrics/LICENSE)
`github.com/signalfx/gohistogram`                            | [MIT](./vendor/github.com/signalfx/gohistogram/LICENSE)
`github.com/signalfx/golib`                                  | [Apache Software License v2.0](./vendor/github.com/signalfx/golib/LICENSE)
`github.com/signalfx/ondiskencoding`                         | [Apache Software License v2.0](./vendor/github.com/signalfx/ondiskencoding/LICENSE)
`github.com/mdubbyap/tdigest`                                | [Apache Software License v2.0](./vendor/github.com/mdubbyap/tdigest/LICENSE)
`github.com/signalfx/xdgbasedir`                             | [Apache Software License v2.0](./vendor/github.com/signalfx/xdgbasedir/LICENSE)
`github.com/sirupsen/logrus`                                 | [MIT](./vendor/github.com/sirupsen/logrus/LICENSE)
`github.com/smartystreets/assertions`                        | [MIT](./vendor/github.com/smartystreets/assertions/LICENSE.md)
`github.com/smartystreets/goconvey`                          | [MIT](./vendor/github.com/smartystreets/goconvey/LICENSE.md)
`github.com/soheilhy/cmux`                                   | [Apache Software License v2.0](./vendor/github.com/soheilhy/cmux/LICENSE)
`github.com/spaolacci/murmur3`                               | [BSD 3-Clause](./vendor/github.com/spaolacci/murmur3/LICENSE)
`github.com/spf13/cobra`                                     | [Apache Software License v2.0](./vendor/github.com/spf13/cobra/LICENSE.txt)
`github.com/spf13/pflag`                                     | [BSD 3-Clause](./vendor/github.com/spf13/pflag/LICENSE)
`github.com/stretchr/testify/assert`                         | [MIT](./vendor/github.com/stretchr/testify/LICENSE)
`github.com/tmc/grpc-websocket-proxy/wsproxy`                | [MIT](./vendor/github.com/tmc/grpc-websocket-proxy/LICENSE)
`github.com/uber-go/atomic`                                  | [MIT](./vendor/github.com/uber-go/atomic/LICENSE.txt)
`github.com/uber/tchannel-go`                                | [MIT](./vendor/github.com/uber/tchannel-go/blob/dev/LICENSE.md)
`github.com/ugorji/go`                                       | [MIT](./vendor/github.com/ugorji/go/LICENSE)
`github.com/xiang90/probing`                                 | [MIT](./vendor/github.com/xiang90/probing/LICENSE)
`go.uber.org/atomic`                                         | [MIT](./vendor/github.com/uber-go/atomic/LICENSE.txt)
`go.uber.org/multierr`                                       | [MIT](./vendor/github.com/uber-go/multierr/LICENSE.txt)
`go.uber.org/zap`                                            | [MIT](./vendor/github.com/uber-go/zap/LICENSE.txt)
`golang.org/x/crypto`                                        | [BSD 3-Clause](./vendor/github.com/golang/crypto/LICENSE)
`golang.org/x/net`                                           | [BSD 3-Clause](./vendor/github.com/golang/net/LICENSE)
`golang.org/x/sys`                                           | [BSD 3-Clause](./vendor/github.com/golang/sys/LICENSE)
`golang.org/x/text`                                          | [BSD 3-Clause](./vendor/github.com/golang/text/LICENSE)
`golang.org/x/time`                                          | [BSD 3-Clause](./vendor/github.com/golang/time/LICENSE)
`google.golang.org/genproto`                                 | [Apache Software License v2.0](./vendor/github.com/google/go-genproto/LICENSE)
`google.golang.org/grpc`                                     | [Apache Software License v2.0](./vendor/github.com/grpc/grpc-go/LICENSE)
`gopkg.in/logfmt.v0`                                         | [MIT](./vendor/github.com/go-logfmt/logfmt/blob/v0.4.0/LICENSE)
`gopkg.in/natefinch/lumberjack.v2`                           | [MIT](./vendor/github.com/natefinch/lumberjack/blob/v2.1/LICENSE)
`gopkg.in/stack.v1`                                          | [MIT](./vendor/github.com/go-stack/stack/blob/v1.8.0/LICENSE.md)
`gopkg.in/yaml.v2`                                           | [Apache Software License v2.0](./vendor/github.com/go-yaml/yaml/blob/v2.2.2/LICENSE)
`gotest.tools/assert`                                        | [Apache Software License v2.0](./vendor/github.com/gotestyourself/gotest.tools/LICENSE)
`gotest.tools`                                               | [Apache Software License v2.0](./vendor/github.com/gotestyourself/gotest.tools/LICENSE)
`stathat.com/c/consistent`                                   | [MIT](./vendor/github.com/stathat/consistent/LICENSE)
