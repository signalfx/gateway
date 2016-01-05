package config

import (
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/carbon"
	"github.com/signalfx/metricproxy/protocol/collectd"
	"github.com/signalfx/metricproxy/protocol/csv"
	"github.com/signalfx/metricproxy/protocol/signalfx"
	"golang.org/x/net/context"
)

type forwarderLoader interface {
	Forwarder(conf *ForwardTo) (protocol.Forwarder, error)
}

type listenerLoader interface {
	Listener(sink dpsink.Sink, conf *ListenFrom) (protocol.Listener, error)
}

func NewLoader(ctx context.Context, logger log.Logger, version string) *Loader {
	sfxL := &signalFxLoader{
		logger:        logger,
		rootContext:   ctx,
		versionString: version,
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
			"signalfx": &signalFxLoader{
				logger:        logger,
				rootContext:   ctx,
				versionString: version,
			},
			"carbon": &carbonLoader{
				logger: logger,
			},
			"collectd": &collectdLoader{
				rootContext: ctx,
			},
		},
	}
}

type Loader struct {
	forwarders map[string]forwarderLoader
	listeners  map[string]listenerLoader
}

func (l *Loader) Forwarder(conf *ForwardTo) (protocol.Forwarder, error) {
	if conf.Type == "" {
		return nil, errors.New("type required to load config")
	}
	if l, exists := l.forwarders[conf.Type]; exists {
		return l.Forwarder(conf)
	}
	return nil, errors.Errorf("cannot find config %s", conf.Type)
}

func (l *Loader) Listener(sink dpsink.Sink, conf *ListenFrom) (protocol.Listener, error) {
	if conf.Type == "" {
		return nil, errors.New("type required to load config")
	}
	if l, exists := l.listeners[conf.Type]; exists {
		return l.Listener(sink, conf)
	}
	return nil, errors.Errorf("cannot find config %s", conf.Type)
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
	rootContext context.Context
}

func (s *collectdLoader) Listener(sink dpsink.Sink, conf *ListenFrom) (protocol.Listener, error) {
	sfConf := collectd.ListenerConfig{
		ListenAddr:        conf.ListenAddr,
		ListenPath:        conf.ListenPath,
		Timeout:           conf.TimeoutDuration,
		DefaultDimensions: conf.Dimensions,
		StartingContext:   s.rootContext,
	}
	return collectd.NewListener(sink, &sfConf)
}

type signalFxLoader struct {
	logger        log.Logger
	rootContext   context.Context
	versionString string
}

func (s *signalFxLoader) Listener(sink dpsink.Sink, conf *ListenFrom) (protocol.Listener, error) {
	sfConf := signalfx.ListenerConfig{
		ListenAddr:  conf.ListenAddr,
		Timeout:     conf.TimeoutDuration,
		Logger:      s.logger,
		RootContext: s.rootContext,
	}
	return signalfx.NewListener(sink, &sfConf)
}

func (s *signalFxLoader) Forwarder(conf *ForwardTo) (protocol.Forwarder, error) {
	sfConf := signalfx.ForwarderConfig{
		DatapointURL:     conf.URL,
		EventURL:         conf.EventURL,
		Timeout:          conf.TimeoutDuration,
		SourceDimensions: conf.SourceDimensions,
		Logger:           s.logger,
		ProxyVersion:     &s.versionString,
		MaxIdleConns:     conf.DrainingThreads,
		AuthToken:        conf.DefaultAuthToken,
	}
	return signalfx.NewForwarder(&sfConf), nil
}

type carbonLoader struct {
	logger log.Logger
}

func (s *carbonLoader) Listener(sink dpsink.Sink, conf *ListenFrom) (protocol.Listener, error) {
	sfConf := carbon.ListenerConfig{
		ServerAcceptDeadline: conf.ServerAcceptDeadline,
		ConnectionTimeout:    conf.TimeoutDuration,
		ListenAddr:           conf.ListenAddr,
		Logger:               s.logger,
	}
	return carbon.NewListener(sink, &sfConf)
}

func (s *carbonLoader) Forwarder(conf *ForwardTo) (protocol.Forwarder, error) {
	if conf.Host == nil {
		return nil, errors.New("Carbon loader requires config 'host' set")
	}
	sfConf := carbon.ForwarderConfig{
		Port:                   conf.Port,
		Timeout:                conf.TimeoutDuration,
		DimensionOrder:         conf.DimensionsOrder,
		IdleConnectionPoolSize: conf.DrainingThreads,
	}
	f, err := carbon.NewForwarder(*conf.Host, &sfConf)
	return &protocol.UneventfulForwarder{f}, err
}