package main

import (
	"flag"
	"fmt"
	"github.com/Sirupsen/logrus"
	log "github.com/Sirupsen/logrus"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/forwarder"
	"github.com/signalfuse/signalfxproxy/listener"
	"github.com/signalfuse/signalfxproxy/stats"
	"github.com/signalfuse/signalfxproxy/statuspage"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
)

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
	allForwarders                 []core.DatapointStreamingAPI
	allListeners                  []listener.DatapointListener
	statDrainThread               core.StatDrainingThread
}

var proxyCommandLineConfiguration proxyCommandLineConfigurationT
var logSetupSync sync.Once

func init() {
	flag.StringVar(&proxyCommandLineConfiguration.configFileName, "configfile", "sf/sfdbproxy.conf", "Name of the db proxy configuration file")

	// All of these are deprecated.  We want to use the config file instead
	flag.StringVar(&proxyCommandLineConfiguration.pidFileName, "signalfxproxypid", "signalfxproxy.pid", "deprecated: Use config file instead...  Name of the file to store the PID in")
	flag.StringVar(&proxyCommandLineConfiguration.pprofaddr, "pprofaddr", "", "deprecated: Use config file instead...  Address to open pprof info on")

	flag.StringVar(&proxyCommandLineConfiguration.logDir, "logdir", os.TempDir(), "deprecated: Use config file instead...  Directory to store log files.  If -, will log to stdout")
	flag.IntVar(&proxyCommandLineConfiguration.logMaxSize, "log_max_size", 100, "deprecated: Use config file instead...  Maximum size of log files (in Megabytes)")
	flag.IntVar(&proxyCommandLineConfiguration.logMaxBackups, "log_max_backups", 10, "deprecated: Use config file instead...  Maximum number of rotated log files to keep")
	flag.BoolVar(&proxyCommandLineConfiguration.logJSON, "logjson", false, "deprecated: Use config file instead...  Log in JSON format (usable with logstash)")

	proxyCommandLineConfiguration.stopChannel = make(chan bool)
	proxyCommandLineConfiguration.closeWhenWaitingToStopChannel = make(chan struct{})
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) blockTillSetupReady() {
	// A closed channel will never block
	_ = <-proxyCommandLineConfiguration.closeWhenWaitingToStopChannel
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) getLogrusOutput(loadedConfig *config.LoadedConfig) io.Writer {
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

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) getLogrusFormatter(loadedConfig *config.LoadedConfig) logrus.Formatter {
	useJSON := proxyCommandLineConfiguration.logJSON
	if loadedConfig.LogFormat != nil {
		useJSON = *loadedConfig.LogFormat == "json"
	}
	if useJSON {
		return &log.JSONFormatter{}
	}
	return &log.TextFormatter{DisableColors: true}
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) setupLogrus(loadedConfig *config.LoadedConfig) {
	out := proxyCommandLineConfiguration.getLogrusOutput(loadedConfig)
	formatter := proxyCommandLineConfiguration.getLogrusFormatter(loadedConfig)

	// -race detection in unit tests w/o this
	logSetupSync.Do(func() {
		log.SetOutput(out)
		log.SetFormatter(formatter)
	})
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) main() error {
	log.WithField("configFile", proxyCommandLineConfiguration.configFileName).Info("Looking for config file")

	loadedConfig, err := config.LoadConfig(proxyCommandLineConfiguration.configFileName)
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

	proxyCommandLineConfiguration.allForwarders = make([]core.DatapointStreamingAPI, 0, len(loadedConfig.ForwardTo))
	allStatKeepers := []core.StatKeeper{stats.NewGolangStatKeeper()}
	for _, forwardConfig := range loadedConfig.ForwardTo {
		loader, ok := forwarder.AllForwarderLoaders[forwardConfig.Type]
		if !ok {
			log.WithField("type", forwardConfig.Type).Error("Unknown loader type")
			return fmt.Errorf("Unknown loader type %s", forwardConfig.Type)
		}
		forwarder, err := loader(forwardConfig)
		if err != nil {
			log.WithField("err", err).Error("Unable to load config")
			return err
		}
		proxyCommandLineConfiguration.allForwarders = append(proxyCommandLineConfiguration.allForwarders, forwarder)
		allStatKeepers = append(allStatKeepers, forwarder)
	}
	multiplexer := forwarder.NewStreamingDatapointDemultiplexer(proxyCommandLineConfiguration.allForwarders)
	allStatKeepers = append(allStatKeepers, multiplexer)

	proxyCommandLineConfiguration.allListeners = make([]listener.DatapointListener, 0, len(loadedConfig.ListenFrom))
	for _, forwardConfig := range loadedConfig.ListenFrom {
		loader, ok := listener.AllListenerLoaders[forwardConfig.Type]
		if !ok {
			log.WithField("type", forwardConfig.Type).Error("Unknown loader type")
			return fmt.Errorf("Unknown loader type %s", forwardConfig.Type)
		}
		listener, err := loader(multiplexer, forwardConfig)
		if err != nil {
			log.WithField("err", err).Error("Unable to load config")
			return err
		}
		proxyCommandLineConfiguration.allListeners = append(proxyCommandLineConfiguration.allListeners, listener)
		allStatKeepers = append(allStatKeepers, listener)
	}

	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		proxyCommandLineConfiguration.statDrainThread = core.NewStatDrainingThread(*loadedConfig.StatsDelayDuration, proxyCommandLineConfiguration.allForwarders, allStatKeepers, proxyCommandLineConfiguration.stopChannel)
		go proxyCommandLineConfiguration.statDrainThread.Start()
	} else {
		log.Info("Skipping stat keeping")
	}

	debugServerAddr := loadedConfig.LocalDebugServer
	if debugServerAddr == nil {
		debugServerAddr = &proxyCommandLineConfiguration.pprofaddr
	}
	if debugServerAddr != nil && *debugServerAddr != "" {
		go func() {
			statusPage := statuspage.NewProxyStatusPage(loadedConfig, allStatKeepers)
			log.WithField("debugAddr", debugServerAddr).Info("Opening debug server")
			http.Handle("/status", statusPage.StatusPage())
			http.Handle("/health", statusPage.HealthPage())
			err := http.ListenAndServe(*debugServerAddr, nil)
			log.WithField("err", err).Info("Finished listening")
		}()
	}

	log.Infof("Setup done.  Blocking!")
	if proxyCommandLineConfiguration.closeWhenWaitingToStopChannel != nil {
		close(proxyCommandLineConfiguration.closeWhenWaitingToStopChannel)
	}
	_ = <-proxyCommandLineConfiguration.stopChannel
	return nil
}

func main() {
	flag.Parse()
	proxyCommandLineConfiguration.main()
}
