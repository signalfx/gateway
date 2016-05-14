package config

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"expvar"
	"github.com/signalfx/gohelpers/stringhelper"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/protocol/common"
	"github.com/signalfx/xdgbasedir"
	"os"
)

// ForwardTo configures where we forward datapoints to
type ForwardTo struct {
	URL               *string `json:",omitempty"`
	EventURL          *string `json:",omitempty"`
	Host              *string `json:",omitempty"`
	Port              *uint16 `json:",omitempty"`
	Type              string
	TimeoutDuration   *time.Duration       `json:"-"`
	Timeout           *string              `json:",omitempty"`
	DefaultSource     *string              `json:",omitempty"`
	DefaultAuthToken  *string              `json:",omitempty"`
	Pipeline          *int64               `json:",omitempty"`
	Name              *string              `json:",omitempty"`
	DrainingThreads   *int64               `json:",omitempty"`
	MetricCreationURL *string              `json:",omitempty"`
	MaxDrainSize      *int64               `json:",omitempty"`
	Filename          *string              `json:",omitempty"`
	SourceDimensions  *string              `json:",omitempty"`
	FormatVersion     *uint32              `json:",omitempty"`
	DimensionsOrder   []string             `json:",omitempty"`
	Filters           *filtering.FilterObj `json:",omitempty"`
}

// ListenFrom configures how we listen for datapoints to forward
type ListenFrom struct {
	Type                           string
	ListenAddr                     *string                `json:",omitempty"`
	Dimensions                     map[string]string      `json:",omitempty"`
	MetricDeconstructor            *string                `json:",omitempty"`
	MetricDeconstructorOptions     *string                `json:",omitempty"`
	MetricDeconstructorOptionsJSON map[string]interface{} `json:",omitempty"`
	Timeout                        *string                `json:",omitempty"`
	Name                           *string                `json:",omitempty"`
	ListenPath                     *string                `json:",omitempty"`
	JSONEngine                     *string                `json:",omitempty"`
	Encrypted                      *bool                  `json:",omitempty"`
	TimeoutDuration                *time.Duration         `json:"-"`
	ServerAcceptDeadline           *time.Duration         `json:"-"`
}

func (listenFrom *ListenFrom) String() string {
	return stringhelper.GenericFromString(listenFrom)
}

func (forwardTo *ForwardTo) String() string {
	return stringhelper.GenericFromString(forwardTo)
}

// ProxyConfig is the full config as presented inside the proxy config file
type ProxyConfig struct {
	ForwardTo                       []*ForwardTo   `json:",omitempty"`
	ListenFrom                      []*ListenFrom  `json:",omitempty"`
	StatsDelay                      *string        `json:",omitempty"`
	StatsDelayDuration              *time.Duration `json:"-"`
	NumProcs                        *int           `json:",omitempty"`
	LocalDebugServer                *string        `json:",omitempty"`
	PidFilename                     *string        `json:",omitempty"`
	LogDir                          *string        `json:",omitempty"`
	LogMaxSize                      *int           `json:",omitempty"`
	LogMaxBackups                   *int           `json:",omitempty"`
	LogFormat                       *string        `json:",omitempty"`
	PprofAddr                       *string        `json:",omitempty"`
	DebugFlag                       *string        `json:",omitempty"`
	ServerName                      *string        `json:",omitempty"`
	MaxGracefulWaitTime             *string        `json:",omitempty"`
	GracefulCheckInterval           *string        `json:",omitempty"`
	MinimalGracefulWaitTime         *string        `json:",omitempty"`
	SilentGracefulTime              *string        `json:",omitempty"`
	MaxGracefulWaitTimeDuration     *time.Duration `json:"-"`
	GracefulCheckIntervalDuration   *time.Duration `json:"-"`
	MinimalGracefulWaitTimeDuration *time.Duration `json:"-"`
	SilentGracefulTimeDuration      *time.Duration `json:"-"`
}

// DefaultProxyConfig is default values for the proxy config
var DefaultProxyConfig = &ProxyConfig{
	PidFilename:                     pointer.String("metricproxy.pid"),
	LogDir:                          pointer.String(os.TempDir()),
	LogMaxSize:                      pointer.Int(100),
	LogMaxBackups:                   pointer.Int(10),
	LogFormat:                       pointer.String(""),
	ServerName:                      pointer.String(getDefaultName(os.Hostname)),
	MaxGracefulWaitTimeDuration:     pointer.Duration(30 * time.Second),
	GracefulCheckIntervalDuration:   pointer.Duration(time.Second),
	MinimalGracefulWaitTimeDuration: pointer.Duration(3 * time.Second),
	SilentGracefulTimeDuration:      pointer.Duration(2 * time.Second),
}

func getDefaultName(osHostname func() (string, error)) string {
	hostname, err := osHostname()
	if err == nil {
		return hostname
	}
	return "unknown"
}

func decodeConfig(configBytes []byte) (*ProxyConfig, error) {
	var config ProxyConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, errors.Annotate(err, "cannot unmarshal config JSON")
	}
	if config.StatsDelay != nil {
		duration, err := time.ParseDuration(*config.StatsDelay)
		config.StatsDelayDuration = &duration
		if err != nil {
			return nil, errors.Annotatef(err, "cannot parse stats delay %s", *config.StatsDelay)
		}
	}
	err := config.decodeTimeouts()
	if err != nil {
		return nil, err
	}
	err = config.decodeGracefulDurations()
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (p *ProxyConfig) decodeTimeouts() error {
	for _, f := range p.ForwardTo {
		if f.Timeout != nil {
			duration, err := time.ParseDuration(*f.Timeout)
			f.TimeoutDuration = &duration
			if err != nil {
				return errors.Annotatef(err, "cannot parse timeout %s", *f.Timeout)
			}
		}
	}
	for _, f := range p.ListenFrom {
		if f.Timeout != nil {
			duration, err := time.ParseDuration(*f.Timeout)
			f.TimeoutDuration = &duration
			if err != nil {
				return errors.Annotatef(err, "cannot parse timeout %s", *f.Timeout)
			}
		}
	}
	return nil
}

func (p *ProxyConfig) decodeGracefulDurations() error {
	if p.MinimalGracefulWaitTime != nil {
		duration, err := time.ParseDuration(*p.MinimalGracefulWaitTime)
		p.MinimalGracefulWaitTimeDuration = &duration
		if err != nil {
			return errors.Annotatef(err, "cannot parse minimal graceful wait time %s", *p.MinimalGracefulWaitTime)
		}
	}
	if p.GracefulCheckInterval != nil {
		duration, err := time.ParseDuration(*p.GracefulCheckInterval)
		p.GracefulCheckIntervalDuration = &duration
		if err != nil {
			return errors.Annotatef(err, "cannot parse graceful check interval %s", *p.GracefulCheckInterval)
		}
	}
	if p.MaxGracefulWaitTime != nil {
		duration, err := time.ParseDuration(*p.MaxGracefulWaitTime)
		p.MaxGracefulWaitTimeDuration = &duration
		if err != nil {
			return errors.Annotatef(err, "cannot parse max graceful wait time %s", *p.MaxGracefulWaitTime)
		}
	}
	if p.SilentGracefulTime != nil {
		duration, err := time.ParseDuration(*p.SilentGracefulTime)
		p.SilentGracefulTimeDuration = &duration
		if err != nil {
			return errors.Annotatef(err, "cannot parse silent graceful wait time %s", *p.SilentGracefulTime)
		}
	}
	return nil
}

func loadConfig(configFile string) (*ProxyConfig, error) {
	configBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read from config file %s", configFile)
	}
	return decodeConfig(configBytes)
}

var xdgbasedirGetConfigFileLocation = xdgbasedir.GetConfigFileLocation

func (p *ProxyConfig) String() string {
	// TODO: Format this
	return "<config object>"
}

// Var returns the proxy config itself as an expvar
func (p *ProxyConfig) Var() expvar.Var {
	return expvar.Func(func() interface{} {
		return p
	})
}

// Load loads proxy configuration from a filename that is in an xdg configuration location
func Load(configFile string, logger log.Logger) (*ProxyConfig, error) {
	p, err := loadNoDefault(configFile, logger)
	if err != nil {
		return nil, err
	}
	return pointer.FillDefaultFrom(p, DefaultProxyConfig).(*ProxyConfig), nil
}

func loadNoDefault(configFile string, logger log.Logger) (*ProxyConfig, error) {
	logCtx := log.NewContext(logger).With(logkey.ConfigFile, configFile)
	filename, err := xdgbasedirGetConfigFileLocation(configFile)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot get config file for location %s", configFile)
	}
	logCtx = logCtx.With(logkey.Filename, filename)
	config, errFilename := loadConfig(filename)
	if errFilename == nil {
		return config, nil
	}
	logCtx.Log(log.Err, errFilename, "unable to load original config filename")
	var errConfigfile error
	config, errConfigfile = loadConfig(configFile)
	if errConfigfile != nil {
		logCtx.Log(log.Err, errConfigfile, "unable to load config again")
		return nil, errors.Annotatef(errConfigfile, "cannot load config file %s", errConfigfile)
	}
	return config, nil
}
