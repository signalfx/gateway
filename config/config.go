package config

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/cep21/gohelpers/stringhelper"
	"github.com/cep21/xdgbasedir"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/golib/errors"
)

// ForwardTo configures where we forward datapoints to
type ForwardTo struct {
	URL               *string
	EventURL          *string
	Host              *string
	Port              *uint16
	Type              string
	TimeoutDuration   *time.Duration `json:"-"`
	Timeout           *string
	DefaultSource     *string
	DefaultAuthToken  *string
	BufferSize        *uint32
	Name              *string
	DrainingThreads   *uint32
	MetricCreationURL *string
	MaxDrainSize      *uint32
	Filename          *string
	SourceDimensions  *string
	FormatVersion     *uint32
	DimensionsOrder   []string
}

// ListenFrom configures how we listen for datapoints to forward
type ListenFrom struct {
	Type                           string
	ListenAddr                     *string
	Dimensions                     map[string]string
	MetricDeconstructor            *string
	MetricDeconstructorOptions     *string
	MetricDeconstructorOptionsJSON map[string]interface{}
	Timeout                        *string
	Name                           *string
	ListenPath                     *string
	JSONEngine                     *string
	Encrypted                      *bool
	TimeoutDuration                *time.Duration `json:"-"`
	ServerAcceptDeadline           *time.Duration `json:"-"`
}

func (listenFrom *ListenFrom) String() string {
	return stringhelper.GenericFromString(listenFrom)
}

func (forwardTo *ForwardTo) String() string {
	return stringhelper.GenericFromString(forwardTo)
}

// ProxyConfig is the full config as presented inside the proxy config file
type ProxyConfig struct {
	ForwardTo          []*ForwardTo
	ListenFrom         []*ListenFrom
	StatsDelay         *string
	StatsDelayDuration *time.Duration `json:"-"`
	NumProcs           *int
	EnableStatusPage   *bool
	LocalDebugServer   *string
	PidFilename        *string
	LogDir             *string
	LogMaxSize         *int
	LogMaxBackups      *int
	LogFormat          *string
	LogLevel           *string
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
	for _, f := range config.ForwardTo {
		if f.Timeout != nil {
			duration, err := time.ParseDuration(*f.Timeout)
			f.TimeoutDuration = &duration
			if err != nil {
				return nil, errors.Annotatef(err, "cannot parse timeout %s", *f.Timeout)
			}
		}
	}
	for _, f := range config.ListenFrom {
		if f.Timeout != nil {
			duration, err := time.ParseDuration(*f.Timeout)
			f.TimeoutDuration = &duration
			if err != nil {
				return nil, errors.Annotatef(err, "cannot parse timeout %s", *f.Timeout)
			}
		}
	}
	return &config, nil
}

func loadConfig(configFile string) (*ProxyConfig, error) {
	configBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot read from config file %s", configFile)
	}
	return decodeConfig(configBytes)
}

var xdgbasedirGetConfigFileLocation = xdgbasedir.GetConfigFileLocation

// Load loads proxy configuration from a filename that is in an xdg configuration location
func Load(configFile string, logger log.Logger) (*ProxyConfig, error) {
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
