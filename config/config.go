package config

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/signalfx/embetcd/embetcd"
	"github.com/signalfx/gateway/etcdIntf"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/gateway/protocol/filtering"
	"github.com/signalfx/gateway/protocol/signalfx/spanobfuscation"
	"github.com/signalfx/gateway/sampling"
	"github.com/signalfx/gohelpers/stringhelper"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/env"
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
	AuthTokenEnvVar      *string                     `json:",omitempty"`
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
	TraceDistributor     *sampling.SmartSampleConfig `json:",omitempty"`
	AdditionalDimensions map[string]string           `json:",omitempty"`
	DisableCompression   *bool                       `json:",omitempty"`
	Client               etcdIntf.Client             `json:"-"`
	ClusterName          *string                     `json:"-"`
}

// ListenFrom configures how we listen for datapoints to forward
type ListenFrom struct {
	Type                               string
	ListenAddr                         *string                               `json:",omitempty"`
	Dimensions                         map[string]string                     `json:",omitempty"`
	MetricDeconstructor                *string                               `json:",omitempty"`
	MetricDeconstructorOptions         *string                               `json:",omitempty"`
	MetricDeconstructorOptionsJSON     map[string]interface{}                `json:",omitempty"`
	Timeout                            *string                               `json:",omitempty"`
	Name                               *string                               `json:",omitempty"`
	ListenPath                         *string                               `json:",omitempty"`
	JSONEngine                         *string                               `json:",omitempty"`
	Encrypted                          *bool                                 `json:",omitempty"`
	Protocol                           *string                               `json:",omitempty"`
	TimeoutDuration                    *time.Duration                        `json:"-"`
	ServerAcceptDeadline               *time.Duration                        `json:"-"`
	SpanNameReplacementRules           []string                              `json:",omitempty"`
	SpanNameReplacementBreakAfterMatch *bool                                 `json:",omitempty"`
	AdditionalSpanTags                 map[string]string                     `json:",omitempty"`
	RemoveSpanTags                     []*spanobfuscation.TagMatchRuleConfig `json:",omitempty"`
	ObfuscateSpanTags                  []*spanobfuscation.TagMatchRuleConfig `json:",omitempty"`
	Counter                            *dpsink.Counter
}

func (listenFrom *ListenFrom) String() string {
	return stringhelper.GenericFromString(listenFrom)
}

func (forwardTo *ForwardTo) String() string {
	return stringhelper.GenericFromString(forwardTo)
}

// GatewayConfig is the full config as presented inside the gateway config file
type GatewayConfig struct {
	// General Gateway Configurations
	NumProcs             *int              `json:",omitempty"`
	PidFilename          *string           `json:",omitempty"`
	AdditionalDimensions map[string]string `json:",omitempty"`

	// forwarder Configuration
	ForwardTo []*ForwardTo `json:",omitempty"`

	// Listener Configurations
	ListenFrom              []*ListenFrom  `json:",omitempty"`
	LateThreshold           *string        `json:",omitempty"`
	FutureThreshold         *string        `json:",omitempty"`
	LateThresholdDuration   *time.Duration `json:"-"`
	FutureThresholdDuration *time.Duration `json:"-"`

	// Log configurations
	LogDir        *string `json:",omitempty"`
	LogMaxSize    *int    `json:",omitempty"`
	LogMaxBackups *int    `json:",omitempty"`
	LogFormat     *string `json:",omitempty"`

	// Internal Metric Configurations
	StatsDelay                     *string        `json:",omitempty"`
	StatsDelayDuration             *time.Duration `json:"-"`
	InternalMetricsListenerAddress *string        `json:",omitempty"`

	// Debug Configurations
	LocalDebugServer *string `json:",omitempty"`
	PprofAddr        *string `json:",omitempty"`
	DebugFlag        *string `json:",omitempty"`

	// Shutdown Configurations
	MaxGracefulWaitTime           *string        `json:",omitempty"`
	GracefulCheckInterval         *string        `json:",omitempty"`
	SilentGracefulTime            *string        `json:",omitempty"`
	MaxGracefulWaitTimeDuration   *time.Duration `json:"-"`
	GracefulCheckIntervalDuration *time.Duration `json:"-"`
	SilentGracefulTimeDuration    *time.Duration `json:"-"`

	// Clustering

	// General Cluster configurations
	ServerName       *string `json:",omitempty"`
	ClusterName      *string `json:",omitempty"`
	ClusterOperation *string `json:",omitempty"`
	ClusterDataDir   *string `json:",omitempty"`

	// Target Cluster Addresses
	TargetClusterAddresses []string `json:",omitempty"`

	// Peer Addresses
	AdvertisedPeerAddresses []string `json:",omitempty"`
	AdvertisePeerAddress    *string  `json:",omitempty"`
	ListenOnPeerAddresses   []string `json:",omitempty"`
	ListenOnPeerAddress     *string  `json:",omitempty"`

	// Client Addresses
	AdvertisedClientAddresses []string `json:",omitempty"`
	AdvertiseClientAddress    *string  `json:",omitempty"`
	ListenOnClientAddresses   []string `json:",omitempty"`
	ListenOnClientAddress     *string  `json:",omitempty"`

	// Metric Addresses for Etcd
	EtcdListenOnMetricsAddresses []string `json:",omitempty"`
	ETCDMetricsAddress           *string  `json:",omitempty"`

	// Durations
	UnhealthyMemberTTL     *time.Duration `json:"-"`
	RemoveMemberTimeout    *time.Duration `json:"-"`
	EtcdServerStartTimeout *time.Duration `json:"-"`

	// Etcd Configurable Durations
	EtcdDialTimeout            *time.Duration `json:"-"`
	EtcdAutoSyncInterval       *time.Duration `json:"-"`
	EtcdStartupGracePeriod     *time.Duration `json:"-"`
	EtcdClusterCleanUpInterval *time.Duration `json:"-"`
	EtcdHeartBeatInterval      *time.Duration `json:"-"` // maps to TickMs
	EtcdElectionTimeout        *time.Duration `json:"-"` // maps to ElectionMs this should be 10x TickMS https://github.com/etcd-io/etcd/blob/release-3.3/Documentation/tuning.md

	// Etcd configurable file limits
	EtcdSnapCount    *uint64 `json:",omitempty"`
	EtcdMaxSnapFiles *uint   `json:",omitempty"`
	EtcdMaxWalFiles  *uint   `json:",omitempty"`

	// Default reporting delay for gateway internal metrics
	InternalMetricsReportingDelay         *string
	InternalMetricsReportingDelayDuration *time.Duration
}

func stringToURL(s string) (u *url.URL, err error) {
	if s != "" && !strings.HasPrefix(s, "http") {
		u, err = url.Parse(fmt.Sprintf("http://%s", s))
	} else {
		u, err = url.Parse(s)
	}

	return u, err
}

func etcdURLHelper(single *string, multiple []string) []url.URL {
	urls := make([]url.URL, 0, len(multiple)+1)
	if single != nil {
		u, err := stringToURL(*single)
		if err == nil {
			urls = append(urls, *u)
		}
	}
	if len(multiple) > 0 {
		for _, stringURL := range multiple {
			u, err := stringToURL(stringURL)
			if err == nil {
				urls = append(urls, *u)
			}
		}
	}
	return urls
}

var etcdClusterStateMapping = map[string]string{
	"seed":                         embed.ClusterStateFlagNew,
	"join":                         embed.ClusterStateFlagExisting,
	embed.ClusterStateFlagNew:      embed.ClusterStateFlagNew,
	embed.ClusterStateFlagExisting: embed.ClusterStateFlagExisting,
}

// copyEtcdDurations is a helper function for copying *GatewayConfig to *embed.Config
func copyEtcdDurations(p *GatewayConfig, etcdCfg *embed.Config) {
	if p.EtcdHeartBeatInterval != nil {
		etcdCfg.TickMs = uint(*p.EtcdHeartBeatInterval / time.Millisecond)
	}
	if p.EtcdElectionTimeout != nil {
		etcdCfg.ElectionMs = uint(*p.EtcdElectionTimeout / time.Millisecond)
	}
}

// copyEtcdURLs is a helper function for copying *GatewayConfig to *embed.Config
func copyEtcdURLs(p *GatewayConfig, etcdCfg *embed.Config) {
	// process urls
	etcdCfg.ListenMetricsUrls = etcdURLHelper(p.ETCDMetricsAddress, p.EtcdListenOnMetricsAddresses)
	etcdCfg.LPUrls = etcdURLHelper(p.ListenOnPeerAddress, p.ListenOnPeerAddresses)
	etcdCfg.APUrls = etcdURLHelper(p.AdvertisePeerAddress, p.AdvertisedPeerAddresses)
	etcdCfg.LCUrls = etcdURLHelper(p.ListenOnClientAddress, p.ListenOnClientAddresses)
	etcdCfg.ACUrls = etcdURLHelper(p.AdvertiseClientAddress, p.AdvertisedClientAddresses)
}

// copyEtcdFileConfigs is a helper function for copying *GatewayConfig to *embed.Config
func copyEtcdFileConfigs(p *GatewayConfig, etcdCfg *embed.Config) {
	if p.EtcdSnapCount != nil {
		etcdCfg.SnapCount = *p.EtcdSnapCount
	}

	if p.EtcdMaxSnapFiles != nil {
		etcdCfg.MaxSnapFiles = *p.EtcdMaxSnapFiles
	}

	if p.EtcdMaxWalFiles != nil {
		etcdCfg.MaxWalFiles = *p.EtcdMaxWalFiles
	}
}

// ToEtcdConfig returns a config struct for github.com/signalfx/embetcd/embetcd
func (p *GatewayConfig) ToEtcdConfig() *embetcd.Config {
	// etcd/embed config struct
	etcdCfg := embed.NewConfig()

	// general configurations
	if p.ServerName != nil {
		etcdCfg.Name = *p.ServerName
	}
	if p.ClusterDataDir != nil {
		etcdCfg.Dir = path.Join(*p.ClusterDataDir, "etcd")
	}

	// cast "seed" or "join" to their corresponding etcd/embed cluster state
	// or just return the exact value that was passed into the config
	if p.ClusterOperation != nil {
		if op, ok := etcdClusterStateMapping[*p.ClusterOperation]; ok {
			etcdCfg.ClusterState = op
		} else {
			etcdCfg.ClusterState = *p.ClusterOperation
		}
	}

	// set the cluster name as the initial cluster token
	if p.ClusterName != nil {
		etcdCfg.InitialClusterToken = *p.ClusterName
	}

	copyEtcdDurations(p, etcdCfg)
	copyEtcdURLs(p, etcdCfg)
	copyEtcdFileConfigs(p, etcdCfg)

	// signalfx/embetcd config struct
	cfg := &embetcd.Config{
		// creates an embedded etcd server config with default values that we'll override
		Config:              etcdCfg,
		CleanUpInterval:     p.EtcdClusterCleanUpInterval,
		DialTimeout:         p.EtcdDialTimeout,
		AutoSyncInterval:    p.EtcdAutoSyncInterval,
		StartupGracePeriod:  p.EtcdStartupGracePeriod,
		UnhealthyTTL:        p.UnhealthyMemberTTL,
		RemoveMemberTimeout: p.RemoveMemberTimeout,
	}

	// validate urls before we use them
	initialCluster := make([]string, 0, len(cfg.InitialCluster))
	for _, s := range p.TargetClusterAddresses {
		if s, err := stringToURL(s); err == nil {
			initialCluster = append(initialCluster, s.String())
		}
	}
	cfg.InitialCluster = initialCluster

	if p.ClusterName != nil {
		cfg.ClusterName = *p.ClusterName
	}

	return cfg
}

// DefaultGatewayConfig returns default gateway config
func DefaultGatewayConfig() *GatewayConfig {
	return &GatewayConfig{
		PidFilename:                   pointer.String("/var/run/gateway.pid"),
		LogDir:                        pointer.String(os.TempDir()),
		LogMaxSize:                    pointer.Int(100),
		LogMaxBackups:                 pointer.Int(10),
		LogFormat:                     pointer.String(""),
		ServerName:                    pointer.String(getDefaultName(os.Hostname)),
		MaxGracefulWaitTimeDuration:   pointer.Duration(time.Second),
		GracefulCheckIntervalDuration: pointer.Duration(time.Second),
		SilentGracefulTimeDuration:    pointer.Duration(2 * time.Second),
		ListenOnPeerAddress:           pointer.String("0.0.0.0:2380"),
		AdvertisePeerAddress:          pointer.String("127.0.0.1:2380"),
		ListenOnClientAddress:         pointer.String("0.0.0.0:2379"),
		AdvertiseClientAddress:        pointer.String("127.0.0.1:2379"),
		ETCDMetricsAddress:            pointer.String("127.0.0.1:2381"),
		EtcdServerStartTimeout:        pointer.Duration(time.Second * 90),
		UnhealthyMemberTTL:            pointer.Duration(embetcd.DefaultUnhealthyTTL),
		RemoveMemberTimeout:           pointer.Duration(time.Second),
		EtcdStartupGracePeriod:        pointer.Duration(time.Second * 120),
		EtcdDialTimeout:               pointer.Duration(embetcd.DefaultDialTimeout),
		EtcdClusterCleanUpInterval:    pointer.Duration(embetcd.DefaultCleanUpInterval),
		EtcdAutoSyncInterval:          pointer.Duration(embetcd.DefaultAutoSyncInterval),
		ClusterDataDir:                pointer.String("./etcd-data"),
		ClusterOperation:              pointer.String(""),
		TargetClusterAddresses:        []string{},
		AdditionalDimensions:          map[string]string{},
		ClusterName:                   pointer.String("gateway"),
		NumProcs:                      pointer.Int(runtime.NumCPU()),
		EtcdHeartBeatInterval:         pointer.Duration(time.Millisecond * 500),
		EtcdElectionTimeout:           pointer.Duration(time.Millisecond * 5000), // etcd recommends 10x heartbeat interval https://github.com/etcd-io/etcd/blob/release-3.3/Documentation/tuning.md
		EtcdSnapCount:                 pointer.Uint64(100000),
		EtcdMaxSnapFiles:              pointer.Uint(embed.DefaultMaxSnapshots),
		EtcdMaxWalFiles:               pointer.Uint(embed.DefaultMaxWALs),
	}
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
	if config.InternalMetricsReportingDelay != nil {
		duration, err := time.ParseDuration(*config.InternalMetricsReportingDelay)
		config.InternalMetricsReportingDelayDuration = &duration
		if err != nil {
			return nil, errors.Annotatef(err, "cannot parse internal metrics delay %s", *config.InternalMetricsReportingDelay)
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
	config.decodeEnvAuthToken()
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

func (p *GatewayConfig) decodeEnvAuthToken() {
	for _, f := range p.ForwardTo {
		if f.AuthTokenEnvVar != nil {
			if token, exists := os.LookupEnv(*f.AuthTokenEnvVar); exists {
				f.DefaultAuthToken = &token
			}
		}
		if f.DefaultAuthToken == nil {
			if token, exists := os.LookupEnv("SIGNALFX_ACCESS_TOKEN"); exists {
				f.DefaultAuthToken = &token
			}
		}
	}
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
		c := pointer.FillDefaultFrom(p, DefaultGatewayConfig()).(*GatewayConfig)
		if err = os.Setenv("gatewayServerName", *c.ServerName); err == nil {
			if c.ClusterName == nil || *c.ClusterName == "" {
				return nil, errors.New("gateway now requires configuring ClusterName at top level of config")
			}
			// fill in environment variable values to override config file
			loadFromEnv(c)
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

// loadFromEnv loads environment variables to override values in the gateway config file
func loadFromEnv(conf *GatewayConfig) {
	// general configs
	conf.ServerName = env.GetStringEnvVar("SFX_SERVER_NAME", conf.ServerName)
	conf.ClusterName = env.GetStringEnvVar("SFX_GATEWAY_CLUSTER_NAME", conf.ClusterName)
	conf.ClusterOperation = env.GetStringEnvVar("SFX_CLUSTER_OPERATION", conf.ClusterOperation)
	conf.ClusterDataDir = env.GetStringEnvVar("SFX_CLUSTER_DATA_DIR", conf.ClusterDataDir)

	// file limits
	conf.EtcdSnapCount = env.GetUint64EnvVar("SFX_ETCD_SNAP_COUNT", conf.EtcdSnapCount)
	conf.EtcdMaxSnapFiles = env.GetUintEnvVar("SFX_ETCD_MAX_SNAP_FILES", conf.EtcdMaxSnapFiles)
	conf.EtcdMaxWalFiles = env.GetUintEnvVar("SFX_ETD_MAX_WAL_FILES", conf.EtcdMaxWalFiles)

	// Target Cluster Addresses
	conf.TargetClusterAddresses = env.GetCommaSeparatedStringEnvVar("SFX_TARGET_CLUSTER_ADDRESSES", conf.TargetClusterAddresses)

	// Peer Addresses
	conf.ListenOnPeerAddress = env.GetStringEnvVar("SFX_LISTEN_ON_PEER_ADDRESS", conf.ListenOnPeerAddress)
	conf.ListenOnPeerAddresses = env.GetCommaSeparatedStringEnvVar("SFX_LISTEN_ON_PEER_ADDRESSES", conf.ListenOnPeerAddresses)
	conf.AdvertisePeerAddress = env.GetStringEnvVar("SFX_ADVERTISE_PEER_ADDRESS", conf.AdvertisePeerAddress)
	conf.AdvertisedPeerAddresses = env.GetCommaSeparatedStringEnvVar("SFX_ADVERTISED_PEER_ADDRESSES", conf.AdvertisedPeerAddresses)

	// Client Addresses
	conf.ListenOnClientAddress = env.GetStringEnvVar("SFX_LISTEN_ON_CLIENT_ADDRESS", conf.ListenOnClientAddress)
	conf.ListenOnClientAddresses = env.GetCommaSeparatedStringEnvVar("SFX_LISTEN_ON_CLIENT_ADDRESSES", conf.ListenOnClientAddresses)
	conf.AdvertiseClientAddress = env.GetStringEnvVar("SFX_ADVERTISE_CLIENT_ADDRESS", conf.AdvertiseClientAddress)
	conf.AdvertisedClientAddresses = env.GetCommaSeparatedStringEnvVar("SFX_ADVERTISED_CLIENT_ADDRESSES", conf.AdvertisedClientAddresses)

	// Metric Addresses
	conf.ETCDMetricsAddress = env.GetStringEnvVar("SFX_ETCD_METRICS_ADDRESS", conf.ETCDMetricsAddress)
	conf.EtcdListenOnMetricsAddresses = env.GetCommaSeparatedStringEnvVar("SFX_ETCD_LISTEN_ON_METRICS_ADDRESSES", conf.EtcdListenOnMetricsAddresses)

	// Durations
	conf.UnhealthyMemberTTL = env.GetDurationEnvVar("SFX_UNHEALTHY_MEMBER_TTL", conf.UnhealthyMemberTTL)
	conf.RemoveMemberTimeout = env.GetDurationEnvVar("SFX_REMOVE_MEMBER_TIMEOUT", conf.RemoveMemberTimeout)
	conf.EtcdDialTimeout = env.GetDurationEnvVar("SFX_ETCD_DIAL_TIMEOUT", conf.EtcdDialTimeout)
	conf.EtcdClusterCleanUpInterval = env.GetDurationEnvVar("SFX_ETCD_CLUSTER_CLEANUP_INTERVAL", conf.EtcdClusterCleanUpInterval)
	conf.EtcdAutoSyncInterval = env.GetDurationEnvVar("SFX_ETCD_AUTOSYNC_INTERVAL", conf.EtcdAutoSyncInterval)
	conf.EtcdStartupGracePeriod = env.GetDurationEnvVar("SFX_ETCD_STARTUP_GRACE_PERIOD", conf.EtcdStartupGracePeriod)
	conf.EtcdHeartBeatInterval = env.GetDurationEnvVar("SFX_ETCD_HEARTBEAT_INTERVAL", conf.EtcdHeartBeatInterval)
	conf.EtcdElectionTimeout = env.GetDurationEnvVar("SFX_ETCD_ELECTION_TIMEOUT", conf.EtcdElectionTimeout)
}
