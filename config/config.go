package config

import (
	"encoding/json"
	"github.com/cep21/gohelpers/stringhelper"
	"github.com/cep21/xdgbasedir"
	"github.com/golang/glog"
	"io/ioutil"
	"time"
)

// ForwardTo configures where we forward datapoints to
type ForwardTo struct {
	URL               *string
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
}

// ListenFrom configures where we forward datapoints to
type ListenFrom struct {
	Type                       string
	ListenAddr                 *string
	MetricDeconstructor        *string
	MetricDeconstructorOptions *string
	Timeout                    *string
	Name                       *string
	ListenPath                 *string
	Encrypted                  *bool
	TimeoutDuration            *time.Duration `json:"-"`
}

func (listenFrom *ListenFrom) String() string {
	return stringhelper.GenericFromString(listenFrom)
}

func (forwardTo *ForwardTo) String() string {
	return stringhelper.GenericFromString(forwardTo)
}

// LoadedConfig is the full config as presented inside the proxy config file
type LoadedConfig struct {
	ForwardTo          []*ForwardTo
	ListenFrom         []*ListenFrom
	StatsDelay         *string
	StatsDelayDuration *time.Duration `json:"-"`
}

func decodeConfig(configBytes []byte) (*LoadedConfig, error) {
	var config LoadedConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, err
	}
	if config.StatsDelay != nil {
		duration, err := time.ParseDuration(*config.StatsDelay)
		config.StatsDelayDuration = &duration
		if err != nil {
			return nil, err
		}
	}
	for _, f := range config.ForwardTo {
		if f.Timeout != nil {
			duration, err := time.ParseDuration(*f.Timeout)
			f.TimeoutDuration = &duration
			if err != nil {
				return nil, err
			}
		}
	}
	for _, f := range config.ListenFrom {
		if f.Timeout != nil {
			duration, err := time.ParseDuration(*f.Timeout)
			f.TimeoutDuration = &duration
			if err != nil {
				return nil, err
			}
		}
	}
	return &config, nil
}

func loadConfig(configFile string) (*LoadedConfig, error) {
	configBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	return decodeConfig(configBytes)
}

var xdgbasedirGetConfigFileLocation = xdgbasedir.GetConfigFileLocation

// LoadConfig loads proxy configuration from a filename that is in an xdg configuration location
func LoadConfig(configFile string) (*LoadedConfig, error) {
	filename, err := xdgbasedirGetConfigFileLocation(configFile)
	if err != nil {
		return nil, err
	}
	config, errFilename := loadConfig(filename)
	if errFilename == nil {
		return config, nil
	}
	glog.V(1).Infof("Unable to load config from %s with error %s\n", filename, errFilename)
	var errConfigfile error
	config, errConfigfile = loadConfig(configFile)
	if errConfigfile != nil {
		glog.Errorf("Unable to load config from %s or %s with error %s | %s\n", filename, configFile, errFilename, errConfigfile)
		return nil, errConfigfile
	}
	return config, nil
}
