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
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/datapoint"
	"github.com/signalfuse/signalfxproxy/protocol/carbon"
	"github.com/signalfuse/signalfxproxy/protocol/collectd"
	"github.com/signalfuse/signalfxproxy/protocol/csv"
	"github.com/signalfuse/signalfxproxy/protocol/demultiplexer"
	"github.com/signalfuse/signalfxproxy/protocol/signalfx"
	"github.com/signalfuse/signalfxproxy/stats"
	"github.com/signalfuse/signalfxproxy/statuspage"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// ForwardingLoader is the function definition of a function that can load a config
// for a proxy and return the streamer
type ForwardingLoader func(*config.ForwardTo) (stats.StatKeepingStreamer, error)

// AllForwarderLoaders is a map of config names to loaders for that config
var allForwarderLoaders = map[string]ForwardingLoader{
	"signalfx-json": signalfx.ForwarderLoader,
	"carbon":        carbon.ForwarderLoader,
	"csv":           csv.ForwarderLoader,
}

// A ListenerLoader loads a DatapointListener from a configuration definition
type ListenerLoader func(datapoint.Streamer, *config.ListenFrom) (stats.ClosableKeeper, error)

// AllListenerLoaders is a map of all loaders from config, for each listener we support
var allListenerLoaders = map[string]ListenerLoader{
	"signalfx": signalfx.ListenerLoader,
	"carbon":   carbon.ListenerLoader,
	"collectd": collectd.ListenerLoader,
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
	allForwarders                 []stats.StatKeepingStreamer
	allListeners                  []stats.ClosableKeeper
	statDrainThread               stats.DrainingThread
}

var proxyCommandLineConfigurationDefault proxyCommandLineConfigurationT
var logSetupSync sync.Once

func init() {
	flag.StringVar(&proxyCommandLineConfigurationDefault.configFileName, "configfile", "sf/sfdbproxy.conf", "Name of the db proxy configuration file")

	// All of these are deprecated.  We want to use the config file instead
	flag.StringVar(&proxyCommandLineConfigurationDefault.pidFileName, "signalfxproxypid", "signalfxproxy.pid", "deprecated: Use config file instead...  Name of the file to store the PID in")
	flag.StringVar(&proxyCommandLineConfigurationDefault.pprofaddr, "pprofaddr", "", "deprecated: Use config file instead...  Address to open pprof info on")

	flag.StringVar(&proxyCommandLineConfigurationDefault.logDir, "logdir", os.TempDir(), "deprecated: Use config file instead...  Directory to store log files.  If -, will log to stdout")
	flag.IntVar(&proxyCommandLineConfigurationDefault.logMaxSize, "log_max_size", 100, "deprecated: Use config file instead...  Maximum size of log files (in Megabytes)")
	flag.IntVar(&proxyCommandLineConfigurationDefault.logMaxBackups, "log_max_backups", 10, "deprecated: Use config file instead...  Maximum number of rotated log files to keep")
	flag.BoolVar(&proxyCommandLineConfigurationDefault.logJSON, "logjson", false, "deprecated: Use config file instead...  Log in JSON format (usable with logstash)")

	proxyCommandLineConfigurationDefault.stopChannel = make(chan bool)
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
		fmt.Printf("Sending logging to stdout")
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
		Filename:   path.Join(logDir, "signalfxproxy.log"),
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

func setupForwarders(loadedConfig *config.ProxyConfig) ([]stats.StatKeepingStreamer, error) {
	allForwarders := make([]stats.StatKeepingStreamer, 0, len(loadedConfig.ForwardTo))
	allKeepers := []stats.Keeper{stats.NewGolangKeeper()}
	for _, forwardConfig := range loadedConfig.ForwardTo {
		loader, ok := allForwarderLoaders[forwardConfig.Type]
		if !ok {
			log.WithField("type", forwardConfig.Type).Error("Unknown loader type")
			return nil, fmt.Errorf("Unknown loader type %s", forwardConfig.Type)
		}
		forwarder, err := loader(forwardConfig)
		if err != nil {
			log.WithField("err", err).Error("Unable to load config")
			return nil, err
		}
		allForwarders = append(allForwarders, forwarder)
		allKeepers = append(allKeepers, forwarder)
	}
	return allForwarders, nil
}

func setupListeners(loadedConfig *config.ProxyConfig, multiplexer stats.StatKeepingStreamer) ([]stats.ClosableKeeper, error) {
	allListeners := make([]stats.ClosableKeeper, 0, len(loadedConfig.ListenFrom))
	for _, listenConfig := range loadedConfig.ListenFrom {
		loader, ok := allListenerLoaders[listenConfig.Type]
		if !ok {
			log.WithField("type", listenConfig.Type).Error("Unknown loader type")
			return nil, fmt.Errorf("Unknown loader type %s", listenConfig.Type)
		}
		listener, err := loader(multiplexer, listenConfig)
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

func recast(input []stats.StatKeepingStreamer) []datapoint.NamedStreamer {
	var ret []datapoint.NamedStreamer
	for _, i := range input {
		ret = append(ret, i)
	}
	return ret
}

func recast2(input []stats.StatKeepingStreamer) []datapoint.Streamer {
	var ret []datapoint.Streamer
	for _, i := range input {
		ret = append(ret, i)
	}
	return ret
}

func recastSK(input []stats.ClosableKeeper) []stats.Keeper {
	var ret []stats.Keeper
	for _, i := range input {
		ret = append(ret, i)
	}
	return ret
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

	proxyCommandLineConfiguration.allForwarders, err = setupForwarders(loadedConfig)
	if err != nil {
		return err
	}
	allKeepers := []stats.Keeper{stats.NewGolangKeeper()}
	multiplexer := demultiplexer.New(recast(proxyCommandLineConfiguration.allForwarders))
	allKeepers = append(allKeepers, multiplexer)

	proxyCommandLineConfiguration.allListeners, err = setupListeners(loadedConfig, multiplexer)
	if err != nil {
		return err
	}
	// I wish I could do this, but golang doesn't allow append() of super/sub types
	// allKeepers = append(allKeepers, proxyCommandLineConfiguration.allListeners...)
	allKeepers = append(allKeepers, recastSK(proxyCommandLineConfiguration.allListeners)...)

	for _, forwarder := range proxyCommandLineConfiguration.allForwarders {
		allKeepers = append(allKeepers, forwarder)
	}

	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		proxyCommandLineConfiguration.statDrainThread = stats.NewDrainingThread(*loadedConfig.StatsDelayDuration, recast2(proxyCommandLineConfiguration.allForwarders), allKeepers, proxyCommandLineConfiguration.stopChannel)
		go proxyCommandLineConfiguration.statDrainThread.Start()
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
