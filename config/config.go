package config

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"expvar"
	"os"

	"github.com/signalfx/gateway/etcdIntf"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/gateway/protocol/filtering"
	"github.com/signalfx/gateway/sampling"
	"github.com/signalfx/gohelpers/stringhelper"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/xdgbasedir"
)

// ForwardTo configures where we forward datapoints to
type ForwardTo struct {
	URL                  *string `json:",omitempty"`
	EventURL             *string `json:",omitempty"`
	TraceURL             *string `json:",omitempty"`
	Host                 *string `json:",omitempty"`
	Port                 *uint16 `json:",omitempty"`
	Type                 string
	TimeoutDuration      *time.Duration              `json:"-"`
	Timeout              *string                     `json:",omitempty"`
	DefaultSource        *string                     `json:",omitempty"`
	DefaultAuthToken     *string                     `json:",omitempty"`
	BufferSize           *int64                      `json:",omitempty"`
	Name                 *string                     `json:",omitempty"`
	DrainingThreads      *int64                      `json:",omitempty"`
	MetricCreationURL    *string                     `json:",omitempty"`
	MaxDrainSize         *int64                      `json:",omitempty"`
	Filename             *string                     `json:",omitempty"`
	SourceDimensions     *string                     `json:",omitempty"`
	FormatVersion        *uint32                     `json:",omitempty"`
	DimensionsOrder      []string                    `json:",omitempty"`
	Filters              *filtering.FilterObj        `json:",omitempty"`
	TraceSample          *sampling.SmartSampleConfig `json:",omitempty"`
	AdditionalDimensions map[string]string           `json:",omitempty"`
	DisableCompression   *bool                       `json:",omitempty"`
	Server               etcdIntf.Server             `json:"-"`
	Client               etcdIntf.Client             `json:"-"`
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
	Protocol                       *string                `json:",omitempty"`
	TimeoutDuration                *time.Duration         `json:"-"`
	ServerAcceptDeadline           *time.Duration         `json:"-"`
	SpanNameReplacementRules       []string               `json:",omitempty"`
}

func (listenFrom *ListenFrom) String() string {
	return stringhelper.GenericFromString(listenFrom)
}

func (forwardTo *ForwardTo) String() string {
	return stringhelper.GenericFromString(forwardTo)
}

// GatewayConfig is the full config as presented inside the gateway config file
type GatewayConfig struct {
	ForwardTo                      []*ForwardTo      `json:",omitempty"`
	ListenFrom                     []*ListenFrom     `json:",omitempty"`
	StatsDelay                     *string           `json:",omitempty"`
	StatsDelayDuration             *time.Duration    `json:"-"`
	NumProcs                       *int              `json:",omitempty"`
	LocalDebugServer               *string           `json:",omitempty"`
	PidFilename                    *string           `json:",omitempty"`
	LogDir                         *string           `json:",omitempty"`
	LogMaxSize                     *int              `json:",omitempty"`
	LogMaxBackups                  *int              `json:",omitempty"`
	LogFormat                      *string           `json:",omitempty"`
	PprofAddr                      *string           `json:",omitempty"`
	DebugFlag                      *string           `json:",omitempty"`
	ServerName                     *string           `json:",omitempty"`
	MaxGracefulWaitTime            *string           `json:",omitempty"`
	GracefulCheckInterval          *string           `json:",omitempty"`
	SilentGracefulTime             *string           `json:",omitempty"`
	MaxGracefulWaitTimeDuration    *time.Duration    `json:"-"`
	GracefulCheckIntervalDuration  *time.Duration    `json:"-"`
	SilentGracefulTimeDuration     *time.Duration    `json:"-"`
	LateThreshold                  *string           `json:",omitempty"`
	FutureThreshold                *string           `json:",omitempty"`
	LateThresholdDuration          *time.Duration    `json:"-"`
	FutureThresholdDuration        *time.Duration    `json:"-"`
	ClusterOperation               *string           `json:",omitempty"`
	ClusterDataDir                 *string           `json:",omitempty"`
	TargetClusterAddresses         []string          `json:",omitempty"`
	AdvertisePeerAddress           *string           `json:",omitempty"`
	ListenOnPeerAddress            *string           `json:",omitempty"`
	AdvertiseClientAddress         *string           `json:",omitempty"`
	ListenOnClientAddress          *string           `json:",omitempty"`
	ETCDMetricsAddress             *string           `json:",omitempty"`
	UnhealthyMemberTTL             *time.Duration    `json:"-"`
	RemoveMemberTimeout            *time.Duration    `json:"-"`
	AdditionalDimensions           map[string]string `json:",omitempty"`
	InternalMetricsListenerAddress *string           `json:",omitempty"`
}

// DefaultGatewayConfig is default values for the gateway config
var DefaultGatewayConfig = &GatewayConfig{
	PidFilename:                   pointer.String("gateway.pid"),
	LogDir:                        pointer.String(os.TempDir()),
	LogMaxSize:                    pointer.Int(100),
	LogMaxBackups:                 pointer.Int(10),
	LogFormat:                     pointer.String(""),
	ServerName:                    pointer.String(getDefaultName(os.Hostname)),
	MaxGracefulWaitTimeDuration:   pointer.Duration(time.Second),
	GracefulCheckIntervalDuration: pointer.Duration(time.Second),
	SilentGracefulTimeDuration:    pointer.Duration(2 * time.Second),
	ListenOnPeerAddress:           pointer.String("127.0.0.1:2380"),
	AdvertisePeerAddress:          pointer.String("127.0.0.1:2380"),
	ListenOnClientAddress:         pointer.String("127.0.0.1:2379"),
	AdvertiseClientAddress:        pointer.String("127.0.0.1:2379"),
	ETCDMetricsAddress:            pointer.String("127.0.0.1:2381"),
	UnhealthyMemberTTL:            pointer.Duration(time.Second * 5),
	RemoveMemberTimeout:           pointer.Duration(time.Second),
	ClusterDataDir:                pointer.String("./etcd-data"),
	ClusterOperation:              pointer.String(""),
	TargetClusterAddresses:        []string{},
	AdditionalDimensions:          map[string]string{},
}

func getDefaultName(osHostname func() (string, error)) string {
	hostname, err := osHostname()
	if err == nil {
		return hostname
	}
	return "unknown"
}

func decodeConfig(configBytes []byte) (*GatewayConfig, error) {
	var config GatewayConfig
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
	err = config.decodeDurations()
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func (p *GatewayConfig) decodeTimeouts() error {
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

func decodeDuration(str *string, dur **time.Duration) error {
	if str != nil {
		duration, err := time.ParseDuration(*str)
		if err != nil {
			return err
		}
		*dur = &duration
	}
	return nil
}

func (p *GatewayConfig) decodeDurations() error {
	if err := decodeDuration(p.GracefulCheckInterval, &p.GracefulCheckIntervalDuration); err != nil {
		return errors.Annotatef(err, "cannot parse graceful check interval %s", *p.GracefulCheckInterval)
	}
	if err := decodeDuration(p.MaxGracefulWaitTime, &p.MaxGracefulWaitTimeDuration); err != nil {
		return errors.Annotatef(err, "cannot parse max graceful wait time %s", *p.MaxGracefulWaitTime)
	}
	if err := decodeDuration(p.SilentGracefulTime, &p.SilentGracefulTimeDuration); err != nil {
		return errors.Annotatef(err, "cannot parse silent graceful wait time %s", *p.SilentGracefulTime)
	}
	if err := decodeDuration(p.FutureThreshold, &p.FutureThresholdDuration); err != nil {
		return errors.Annotatef(err, "cannot parse future threshold %s", *p.FutureThreshold)
	}
	if err := decodeDuration(p.LateThreshold, &p.LateThresholdDuration); err != nil {
		return errors.Annotatef(err, "cannot parse lag threshold %s", *p.LateThreshold)
	}
	return nil
}

func loadConfig(configFile string) (*GatewayConfig, error) {
	configBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read from config file %s", configFile)
	}
	return decodeConfig(configBytes)
}

var xdgbasedirGetConfigFileLocation = xdgbasedir.GetConfigFileLocation

func (p *GatewayConfig) String() string {
	// TODO: Format this
	return "<config object>"
}

// Var returns the gateway config itself as an expvar
func (p *GatewayConfig) Var() expvar.Var {
	return expvar.Func(func() interface{} {
		return p
	})
}

// Load loads gateway configuration from a filename that is in an xdg configuration location
func Load(configFile string, logger log.Logger) (*GatewayConfig, error) {
	p, err := loadNoDefault(configFile, logger)
	if err == nil {
		c := pointer.FillDefaultFrom(p, DefaultGatewayConfig).(*GatewayConfig)
		if err = os.Setenv("gatewayServerName", *c.ServerName); err == nil {
			return c, nil
		}
	}
	return nil, err
}

func loadNoDefault(configFile string, logger log.Logger) (*GatewayConfig, error) {
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
	var errConfigfile error
	config, errConfigfile = loadConfig(configFile)
	if errConfigfile != nil {
		logCtx.Log(log.Err, errConfigfile, "unable to load config again")
		return nil, errors.Annotatef(errConfigfile, "cannot load config file %s", errConfigfile)
	}
	return config, nil
}
