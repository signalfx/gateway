package config

import (
	"context"
	"github.com/signalfx/gateway/protocol"
	"github.com/signalfx/gateway/protocol/carbon"
	"github.com/signalfx/gateway/protocol/carbon/metricdeconstructor"
	"github.com/signalfx/gateway/protocol/collectd"
	"github.com/signalfx/gateway/protocol/csv"
	"github.com/signalfx/gateway/protocol/prometheus"
	"github.com/signalfx/gateway/protocol/signalfx"
	"github.com/signalfx/gateway/protocol/wavefront"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/web"
)

type forwarderLoader interface {
	Forwarder(conf *ForwardTo) (protocol.Forwarder, error)
}

type listenerLoader interface {
	Listener(sink signalfx.Sink, conf *ListenFrom) (protocol.Listener, error)
}

type listenSinkWrapper interface {
	WrapSink(sink signalfx.Sink, conf *ListenFrom) signalfx.Sink
}

// NewLoader creates the default loader for gateway protocols
func NewLoader(ctx context.Context, logger log.Logger, version string, debugContext *web.HeaderCtxFlag, itemFlagger *dpsink.ItemFlagger, ctxdims *log.CtxDimensions, next web.NextConstructor) *Loader {
	sfxL := &signalFxLoader{
		logger:        logger,
		rootContext:   ctx,
		versionString: version,
		itemFlagger:   itemFlagger,
		ctxdims:       ctxdims,
		httpChain:     next,
		debugContext:  debugContext,
	}
	return &Loader{
		forwarders: map[string]forwarderLoader{
			"signalfx-json": sfxL,
			"signalfx":      sfxL,
			"carbon": &carbonLoader{
				logger: logger,
			},
			"csv": &csvLoader{},
		},
		listeners: map[string]listenerLoader{
			"signalfx": sfxL,
			"carbon": &carbonLoader{
				logger: logger,
			},
			"collectd": &collectdLoader{
				rootContext:  ctx,
				debugContext: debugContext,
				logger:       logger,
				httpChain:    next,
			},
			"prometheus": &prometheusLoader{
				rootContext:  ctx,
				debugContext: debugContext,
				logger:       logger,
				httpChain:    next,
			},
			"wavefront": &wavefrontLoader{
				logger: logger,
			},
		},
		listenWrappers: []listenSinkWrapper{
			&dimensionListenerSink{},
		},
	}
}

// Loader is able to load forwarders and listeners from config type strings
type Loader struct {
	forwarders     map[string]forwarderLoader
	listeners      map[string]listenerLoader
	listenWrappers []listenSinkWrapper
}

// Forwarder loads a forwarder based upon config, finding the right forwarder first
func (l *Loader) Forwarder(conf *ForwardTo) (protocol.Forwarder, error) {
	if conf.Type == "" {
		return nil, errors.New("type required to load config")
	}
	if l, exists := l.forwarders[conf.Type]; exists {
		return l.Forwarder(conf)
	}
	return nil, errors.Errorf("cannot find config %s", conf.Type)
}

// Listener loads a listener based upon config, finding the right listener first
func (l *Loader) Listener(sink signalfx.Sink, conf *ListenFrom) (protocol.Listener, error) {
	if conf.Type == "" {
		return nil, errors.New("type required to load config")
	}
	wrappedSink := sink
	for _, w := range l.listenWrappers {
		wrappedSink = w.WrapSink(wrappedSink, conf)
	}
	if l, exists := l.listeners[conf.Type]; exists {
		return l.Listener(wrappedSink, conf)
	}
	return nil, errors.Errorf("cannot find config %s", conf.Type)
}

type dimensionListenerSink struct {
}

func (d *dimensionListenerSink) WrapSink(sink signalfx.Sink, conf *ListenFrom) signalfx.Sink {
	if len(conf.Dimensions) == 0 {
		return sink
	}
	return signalfx.IncludingDimensions(conf.Dimensions, sink)
}

type csvLoader struct {
}

func (s *csvLoader) Forwarder(conf *ForwardTo) (protocol.Forwarder, error) {
	sfConf := csv.Config{
		Filename: conf.Filename,
	}
	return csv.NewForwarder(&sfConf)
}

type collectdLoader struct {
	rootContext  context.Context
	debugContext *web.HeaderCtxFlag
	httpChain    web.NextConstructor
	logger       log.Logger
}

type prometheusLoader struct {
	rootContext  context.Context
	debugContext *web.HeaderCtxFlag
	httpChain    web.NextConstructor
	logger       log.Logger
}

func (p *prometheusLoader) Listener(sink signalfx.Sink, conf *ListenFrom) (protocol.Listener, error) {
	sfConf := prometheus.Config{
		ListenAddr:      conf.ListenAddr,
		ListenPath:      conf.ListenPath,
		Timeout:         conf.TimeoutDuration,
		StartingContext: p.rootContext,
		HTTPChain:       p.httpChain,
		Logger:          p.logger,
	}
	return prometheus.NewListener(sink, &sfConf)
}

func (s *collectdLoader) Listener(sink signalfx.Sink, conf *ListenFrom) (protocol.Listener, error) {
	sfConf := collectd.ListenerConfig{
		ListenAddr:      conf.ListenAddr,
		ListenPath:      conf.ListenPath,
		Timeout:         conf.TimeoutDuration,
		StartingContext: s.rootContext,
		DebugContext:    s.debugContext,
		HTTPChain:       s.httpChain,
		Logger:          s.logger,
	}
	return collectd.NewListener(sink, &sfConf)
}

type signalFxLoader struct {
	logger        log.Logger
	rootContext   context.Context
	debugContext  *web.HeaderCtxFlag
	versionString string
	itemFlagger   *dpsink.ItemFlagger
	ctxdims       *log.CtxDimensions
	httpChain     web.NextConstructor
}

func (s *signalFxLoader) Listener(sink signalfx.Sink, conf *ListenFrom) (protocol.Listener, error) {
	sfConf := signalfx.ListenerConfig{
		ListenAddr:                         conf.ListenAddr,
		Timeout:                            conf.TimeoutDuration,
		Logger:                             s.logger,
		RootContext:                        s.rootContext,
		DebugContext:                       s.debugContext,
		HTTPChain:                          s.httpChain,
		SpanNameReplacementRules:           conf.SpanNameReplacementRules,
		SpanNameReplacementBreakAfterMatch: conf.SpanNameReplacementBreakAfterMatch,
		AdditionalSpanTags:                 conf.AdditionalSpanTags,
	}
	return signalfx.NewListener(sink, &sfConf)
}

func (s *signalFxLoader) Forwarder(conf *ForwardTo) (protocol.Forwarder, error) {
	sfConf := signalfx.ForwarderConfig{
		DatapointURL:       conf.URL,
		EventURL:           conf.EventURL,
		TraceURL:           conf.TraceURL,
		Timeout:            conf.TimeoutDuration,
		SourceDimensions:   conf.SourceDimensions,
		GatewayVersion:     &s.versionString,
		MaxIdleConns:       conf.DrainingThreads,
		AuthToken:          conf.DefaultAuthToken,
		DisableCompression: conf.DisableCompression,
		Logger:             s.logger,
		Filters:            conf.Filters,
		TraceSample:        conf.TraceSample,
	}
	if sfConf.TraceSample != nil {
		sfConf.TraceSample.EtcdServer = conf.Server
		sfConf.TraceSample.EtcdClient = conf.Client
		sfConf.TraceSample.AdditionalDimensions = datapoint.AddMaps(conf.AdditionalDimensions, conf.TraceSample.AdditionalDimensions)
		sfConf.TraceSample.ClusterName = conf.ClusterName
	}
	return signalfx.NewForwarder(&sfConf)
}

type carbonLoader struct {
	logger log.Logger
}

func (s *carbonLoader) loadMetricDeconstructor(conf *ListenFrom) (metricdeconstructor.MetricDeconstructor, error) {
	if conf.MetricDeconstructor == nil {
		return nil, nil
	}
	opts := ""
	if conf.MetricDeconstructorOptions != nil {
		opts = *conf.MetricDeconstructorOptions
	}
	md, err := metricdeconstructor.Load(*conf.MetricDeconstructor, opts)
	if err == nil {
		return md, nil
	}

	return metricdeconstructor.LoadJSON(*conf.MetricDeconstructor, conf.MetricDeconstructorOptionsJSON)
}

func (s *carbonLoader) Listener(sink signalfx.Sink, conf *ListenFrom) (protocol.Listener, error) {
	md, err := s.loadMetricDeconstructor(conf)
	if err != nil {
		return nil, errors.Annotate(err, "cannot load metric deconstructor")
	}
	sfConf := carbon.ListenerConfig{
		ServerAcceptDeadline: conf.ServerAcceptDeadline,
		ConnectionTimeout:    conf.TimeoutDuration,
		ListenAddr:           conf.ListenAddr,
		Logger:               s.logger,
		MetricDeconstructor:  md,
		Protocol:             conf.Protocol,
	}

	return carbon.NewListener(sink, &sfConf)
}

func (s *carbonLoader) Forwarder(conf *ForwardTo) (protocol.Forwarder, error) {
	if conf.Host == nil {
		return nil, errors.New("carbon loader requires config 'host' set")
	}
	sfConf := carbon.ForwarderConfig{
		Port:                   conf.Port,
		Timeout:                conf.TimeoutDuration,
		DimensionOrder:         conf.DimensionsOrder,
		IdleConnectionPoolSize: conf.DrainingThreads,
		Filters:                conf.Filters,
	}
	f, err := carbon.NewForwarder(*conf.Host, &sfConf)
	return &protocol.UneventfulForwarder{
		DatapointForwarder: f,
	}, err
}

type wavefrontLoader struct {
	logger log.Logger
}

func (s *wavefrontLoader) Listener(sink signalfx.Sink, conf *ListenFrom) (protocol.Listener, error) {
	sfConf := wavefront.ListenerConfig{
		ServerAcceptDeadline: conf.ServerAcceptDeadline,
		ConnectionTimeout:    conf.TimeoutDuration,
		ListenAddr:           conf.ListenAddr,
		Logger:               s.logger,
	}

	return wavefront.NewListener(sink, &sfConf)
}
