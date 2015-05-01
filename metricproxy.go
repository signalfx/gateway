package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"

	"github.com/Sirupsen/logrus"
	log "github.com/Sirupsen/logrus"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dpsink"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/carbon"
	"github.com/signalfx/metricproxy/protocol/collectd"
	"github.com/signalfx/metricproxy/protocol/csv"
	"github.com/signalfx/metricproxy/protocol/demultiplexer"
	"github.com/signalfx/metricproxy/protocol/signalfx"
	"github.com/signalfx/metricproxy/stats"
	"github.com/signalfx/metricproxy/statuspage"
	"golang.org/x/net/context"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// ForwardingLoader is the function definition of a function that can load a config
// for a proxy and return the streamer
type ForwardingLoader func(context.Context, *config.ForwardTo) (protocol.Forwarder, error)

// AllForwarderLoaders is a map of config names to loaders for that config
var allForwarderLoaders = map[string]ForwardingLoader{
	"signalfx-json": signalfx.ForwarderLoader,
	"carbon":        carbon.ForwarderLoader,
	"csv": func(ctx context.Context, conf *config.ForwardTo) (protocol.Forwarder, error) {
		return csv.ForwarderLoader(conf)
	},
}

// A ListenerLoader loads a DatapointListener from a configuration definition
type ListenerLoader func(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (protocol.Listener, error)

// AllListenerLoaders is a map of all loaders from config, for each listener we support
var allListenerLoaders = map[string]ListenerLoader{
	"signalfx": func(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (protocol.Listener, error) {
		return signalfx.ListenerLoader(ctx, sink, listenFrom)
	},
	"carbon": func(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (protocol.Listener, error) {
		return carbon.ListenerLoader(ctx, sink, listenFrom)
	},
	"collectd": func(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (protocol.Listener, error) {
		return collectd.ListenerLoader(ctx, sink, listenFrom)
	},
}

func writePidFile(pidFileName string) error {
	pid := os.Getpid()
	err := ioutil.WriteFile(pidFileName, []byte(strconv.FormatInt(int64(pid), 10)), os.FileMode(0644))
	if err != nil {
		return err
	}
	return nil
}

type proxyCommandLineConfigurationT struct {
	configFileName                string
	pidFileName                   string
	pprofaddr                     string
	logDir                        string
	logMaxSize                    int
	logMaxBackups                 int
	stopChannel                   chan bool
	closeWhenWaitingToStopChannel chan struct{}
	logJSON                       bool
	ctx                           context.Context
	allForwarders                 []protocol.Forwarder
	allListeners                  []protocol.Listener
	statDrainThread               *stats.StatDrainingThread
}

var proxyCommandLineConfigurationDefault proxyCommandLineConfigurationT
var logSetupSync sync.Once

func init() {
	flag.StringVar(&proxyCommandLineConfigurationDefault.configFileName, "configfile", "sf/sfdbproxy.conf", "Name of the db proxy configuration file")

	// All of these are deprecated.  We want to use the config file instead
	flag.StringVar(&proxyCommandLineConfigurationDefault.pidFileName, "metricproxypid", "metricproxy.pid", "deprecated: Use config file instead...  Name of the file to store the PID in")
	flag.StringVar(&proxyCommandLineConfigurationDefault.pprofaddr, "pprofaddr", "", "deprecated: Use config file instead...  Address to open pprof info on")

	flag.StringVar(&proxyCommandLineConfigurationDefault.logDir, "logdir", os.TempDir(), "deprecated: Use config file instead...  Directory to store log files.  If -, will log to stdout")
	flag.IntVar(&proxyCommandLineConfigurationDefault.logMaxSize, "log_max_size", 100, "deprecated: Use config file instead...  Maximum size of log files (in Megabytes)")
	flag.IntVar(&proxyCommandLineConfigurationDefault.logMaxBackups, "log_max_backups", 10, "deprecated: Use config file instead...  Maximum number of rotated log files to keep")
	flag.BoolVar(&proxyCommandLineConfigurationDefault.logJSON, "logjson", false, "deprecated: Use config file instead...  Log in JSON format (usable with logstash)")

	proxyCommandLineConfigurationDefault.stopChannel = make(chan bool)
	proxyCommandLineConfigurationDefault.ctx = context.Background()
	proxyCommandLineConfigurationDefault.closeWhenWaitingToStopChannel = make(chan struct{})
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) blockTillSetupReady() {
	// A closed channel will never block
	_ = <-proxyCommandLineConfiguration.closeWhenWaitingToStopChannel
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) getLogrusOutput(loadedConfig *config.ProxyConfig) io.Writer {
	logDir := proxyCommandLineConfiguration.logDir
	if loadedConfig.LogDir != nil {
		logDir = *loadedConfig.LogDir
	}
	if logDir == "-" {
		fmt.Println("Sending logging to stdout")
		return os.Stdout
	}
	logMaxSize := proxyCommandLineConfiguration.logMaxSize
	if loadedConfig.LogMaxSize != nil {
		logMaxSize = *loadedConfig.LogMaxSize
	}
	logMaxBackups := proxyCommandLineConfiguration.logMaxBackups
	if loadedConfig.LogMaxBackups != nil {
		logMaxBackups = *loadedConfig.LogMaxBackups
	}
	lumberjackLogger := &lumberjack.Logger{
		Filename:   path.Join(logDir, "metricproxy.log"),
		MaxSize:    logMaxSize, // megabytes
		MaxBackups: logMaxBackups,
	}
	fmt.Printf("Sending logging to %s temp is %s\n", lumberjackLogger.Filename, os.TempDir())
	return lumberjackLogger
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) getLogrusFormatter(loadedConfig *config.ProxyConfig) logrus.Formatter {
	useJSON := proxyCommandLineConfiguration.logJSON
	if loadedConfig.LogFormat != nil {
		useJSON = *loadedConfig.LogFormat == "json"
	}
	if useJSON {
		return &log.JSONFormatter{}
	}
	return &log.TextFormatter{DisableColors: true}
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) setupLogrus(loadedConfig *config.ProxyConfig) {
	out := proxyCommandLineConfiguration.getLogrusOutput(loadedConfig)
	formatter := proxyCommandLineConfiguration.getLogrusFormatter(loadedConfig)

	// -race detection in unit tests w/o this
	logSetupSync.Do(func() {
		log.SetOutput(out)
		log.SetFormatter(formatter)
	})
}

func setupForwarders(ctx context.Context, loadedConfig *config.ProxyConfig) ([]protocol.Forwarder, error) {
	allForwarders := make([]protocol.Forwarder, 0, len(loadedConfig.ForwardTo))
	allKeepers := []stats.Keeper{stats.NewGolangKeeper()}
	for _, forwardConfig := range loadedConfig.ForwardTo {
		loader, ok := allForwarderLoaders[forwardConfig.Type]
		if !ok {
			log.WithField("type", forwardConfig.Type).Error("Unknown loader type")
			return nil, fmt.Errorf("unknown loader type %s", forwardConfig.Type)
		}
		forwarder, err := loader(ctx, forwardConfig)
		if err != nil {
			log.WithField("err", err).Error("unable to load config")
			return nil, err
		}
		allForwarders = append(allForwarders, forwarder)
		allKeepers = append(allKeepers, forwarder)
	}
	return allForwarders, nil
}

func setupListeners(ctx context.Context, loadedConfig *config.ProxyConfig, multiplexer dpsink.Sink) ([]protocol.Listener, error) {
	allListeners := make([]protocol.Listener, 0, len(loadedConfig.ListenFrom))
	for _, listenConfig := range loadedConfig.ListenFrom {
		loader, ok := allListenerLoaders[listenConfig.Type]
		if !ok {
			log.WithField("type", listenConfig.Type).Error("Unknown loader type")
			return nil, fmt.Errorf("unknown loader type %s", listenConfig.Type)
		}
		listener, err := loader(ctx, multiplexer, listenConfig)
		if err != nil {
			log.WithField("err", err).Error("Unable to load config")
			return nil, err
		}
		allListeners = append(allListeners, listener)
	}
	return allListeners, nil
}

func setupDebugServer(allKeepers []stats.Keeper, loadedConfig *config.ProxyConfig, debugServerAddrFromCmd string) {
	debugServerAddr := loadedConfig.LocalDebugServer
	if debugServerAddr == nil {
		debugServerAddr = &debugServerAddrFromCmd
	}
	if debugServerAddr != nil && *debugServerAddr != "" {
		go func() {
			statusPage := statuspage.New(loadedConfig, allKeepers)
			log.WithField("debugAddr", debugServerAddr).Info("Opening debug server")
			http.Handle("/status", statusPage.StatusPage())
			http.Handle("/health", statusPage.HealthPage())
			err := http.ListenAndServe(*debugServerAddr, nil)
			log.WithField("err", err).Info("Finished listening")
		}()
	}
}

func recastToSinks(in []protocol.Forwarder) []dpsink.Sink {
	r := make([]dpsink.Sink, len(in))
	for i, n := range in {
		r[i] = n
	}
	return r
}

func recastListenToKeeper(in []protocol.Listener) []stats.Keeper {
	r := make([]stats.Keeper, len(in))
	for i, n := range in {
		r[i] = n
	}
	return r
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) main() error {
	log.WithField("configFile", proxyCommandLineConfiguration.configFileName).Info("Looking for config file")

	loadedConfig, err := config.Load(proxyCommandLineConfiguration.configFileName)
	if err != nil {
		log.WithField("err", err).Error("Unable to load config")
		return err
	}
	proxyCommandLineConfiguration.setupLogrus(loadedConfig)

	{
		pidFilename := proxyCommandLineConfiguration.pidFileName
		if loadedConfig.PidFilename != nil {
			pidFilename = *loadedConfig.PidFilename
		}
		writePidFile(pidFilename)
	}

	log.WithField("config", loadedConfig).Info("Config loaded")
	if loadedConfig.NumProcs != nil {
		runtime.GOMAXPROCS(*loadedConfig.NumProcs)
	} else {
		numProcs := runtime.NumCPU()
		runtime.GOMAXPROCS(numProcs)
	}

	proxyCommandLineConfiguration.allForwarders, err = setupForwarders(proxyCommandLineConfiguration.ctx, loadedConfig)
	if err != nil {
		return err
	}
	allKeepers := []stats.Keeper{stats.NewGolangKeeper()}

	multiplexerCounter := dpsink.Counter{}
	multiplexer := dpsink.FromChain(demultiplexer.New(recastToSinks(proxyCommandLineConfiguration.allForwarders)), dpsink.NextWrap(multiplexerCounter))

	allKeepers = append(allKeepers, stats.ToKeeper(&multiplexerCounter, map[string]string{"location": "middle", "name": "proxy", "type": "demultiplexer"}))

	proxyCommandLineConfiguration.allListeners, err = setupListeners(proxyCommandLineConfiguration.ctx, loadedConfig, multiplexer)
	if err != nil {
		return err
	}
	// I wish I could do this, but golang doesn't allow append() of super/sub types
	// allKeepers = append(allKeepers, proxyCommandLineConfiguration.allListeners...)
	allKeepers = append(allKeepers, recastListenToKeeper(proxyCommandLineConfiguration.allListeners)...)

	for _, forwarder := range proxyCommandLineConfiguration.allForwarders {
		allKeepers = append(allKeepers, forwarder)
	}

	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		proxyCommandLineConfiguration.statDrainThread = stats.NewDrainingThread(*loadedConfig.StatsDelayDuration, recastToSinks(proxyCommandLineConfiguration.allForwarders), allKeepers, proxyCommandLineConfiguration.ctx)
	} else {
		log.Info("Skipping stat keeping")
	}

	setupDebugServer(allKeepers, loadedConfig, proxyCommandLineConfiguration.pprofaddr)

	log.Infof("Setup done.  Blocking!")
	if proxyCommandLineConfiguration.closeWhenWaitingToStopChannel != nil {
		close(proxyCommandLineConfiguration.closeWhenWaitingToStopChannel)
	}
	_ = <-proxyCommandLineConfiguration.stopChannel
	return nil
}

var flagParse = flag.Parse

func main() {
	flagParse()
	proxyCommandLineConfigurationDefault.main()
}
