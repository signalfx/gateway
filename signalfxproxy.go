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
	configFileName string
	pidFileName    string
	pprofaddr      string
	logDir         string
	logMaxSize     int
	logMaxBackups  int
	stopChannel    chan bool
	logJSON        bool
}

var proxyCommandLineConfiguration proxyCommandLineConfigurationT
var logSetupSync sync.Once

func init() {
	flag.StringVar(&proxyCommandLineConfiguration.configFileName, "configfile", "sf/sfdbproxy.conf", "Name of the db proxy configuration file")
	flag.StringVar(&proxyCommandLineConfiguration.pidFileName, "signalfxproxypid", "signalfxproxy.pid", "Name of the file to store the PID in")
	flag.StringVar(&proxyCommandLineConfiguration.pprofaddr, "pprofaddr", "", "Address to open pprof info on")
	flag.StringVar(&proxyCommandLineConfiguration.logDir, "logdir", os.TempDir(), "Directory to store log files.  If -, will log to stdout")
	flag.IntVar(&proxyCommandLineConfiguration.logMaxSize, "log_max_size", 100, "Maximum size of log files (in Megabytes)")
	flag.IntVar(&proxyCommandLineConfiguration.logMaxBackups, "log_max_backups", 10, "Maximum number of rotated log files to keep")
	flag.BoolVar(&proxyCommandLineConfiguration.logJSON, "logjson", false, "Log in JSON format (usable with logstash)")
	proxyCommandLineConfiguration.stopChannel = make(chan bool)
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) getLogrusOutput() io.Writer {
	if proxyCommandLineConfiguration.logDir == "-" {
		fmt.Printf("Sending logging to stdout")
		return os.Stdout
	}
	lumberjackLogger := &lumberjack.Logger{
		Filename:   path.Join(proxyCommandLineConfiguration.logDir, "signalfxproxy.log"),
		MaxSize:    proxyCommandLineConfiguration.logMaxSize, // megabytes
		MaxBackups: proxyCommandLineConfiguration.logMaxBackups,
	}
	fmt.Printf("Sending logging to %s temp is %s\n", lumberjackLogger.Filename, os.TempDir())
	return lumberjackLogger
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) getLogrusFormatter() logrus.Formatter {
	if proxyCommandLineConfiguration.logJSON {
		return &log.JSONFormatter{}
	}
	return &log.TextFormatter{DisableColors: true}
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) setupLogrus() {
	out := proxyCommandLineConfiguration.getLogrusOutput()
	formatter := proxyCommandLineConfiguration.getLogrusFormatter()

	// -race detection in unit tests w/o this
	logSetupSync.Do(func() {
		log.SetOutput(out)
		log.SetFormatter(formatter)
	})
}

func (proxyCommandLineConfiguration *proxyCommandLineConfigurationT) main() {
	proxyCommandLineConfiguration.setupLogrus()
	// TODO: Configure this on command line.  Needed because JSON decoding can get kinda slow.
	numProcs := runtime.NumCPU()
	runtime.GOMAXPROCS(numProcs)

	if proxyCommandLineConfiguration.pprofaddr != "" {
		go func() {
			log.WithField("pprofaddr", proxyCommandLineConfiguration.pprofaddr).Info("Opening pprof debug information")
			err := http.ListenAndServe(proxyCommandLineConfiguration.pprofaddr, nil)
			log.WithField("err", err).Info("Finished listening")
		}()
	}
	writePidFile(proxyCommandLineConfiguration.pidFileName)
	log.WithField("configFile", proxyCommandLineConfiguration.configFileName).Info("Looking for config file")

	loadedConfig, err := config.LoadConfig(proxyCommandLineConfiguration.configFileName)
	if err != nil {
		log.WithField("err", err).Error("Unable to load config")
		return
	}
	log.WithField("config", loadedConfig).Info("Config loaded")
	allForwarders := []core.DatapointStreamingAPI{}
	allStatKeepers := []core.StatKeeper{}
	for _, forwardConfig := range loadedConfig.ForwardTo {
		loader, ok := forwarder.AllForwarderLoaders[forwardConfig.Type]
		if !ok {
			log.WithField("type", forwardConfig.Type).Error("Unknown loader type")
			return
		}
		forwarder, err := loader(forwardConfig)
		if err != nil {
			log.WithField("err", err).Error("Unable to load config")
			return
		}
		allForwarders = append(allForwarders, forwarder)
		allStatKeepers = append(allStatKeepers, forwarder)
	}
	multiplexer := forwarder.NewStreamingDatapointDemultiplexer(allForwarders)
	allStatKeepers = append(allStatKeepers, multiplexer)

	allListeners := make([]listener.DatapointListener, len(loadedConfig.ListenFrom))
	for _, forwardConfig := range loadedConfig.ListenFrom {
		loader, ok := listener.AllListenerLoaders[forwardConfig.Type]
		if !ok {
			log.WithField("type", forwardConfig.Type).Error("Unknown loader type")
			return
		}
		listener, err := loader(multiplexer, forwardConfig)
		if err != nil {
			log.WithField("err", err).Error("Unable to load config")
			return
		}
		allListeners = append(allListeners, listener)
		allStatKeepers = append(allStatKeepers, listener)
	}

	allStatKeepers = append(allStatKeepers, stats.NewGolangStatKeeper())

	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		go core.DrainStatsThread(*loadedConfig.StatsDelayDuration, allForwarders, allStatKeepers, proxyCommandLineConfiguration.stopChannel)
	} else {
		log.Info("Skipping stat keeping")
	}

	log.Infof("Setup done.  Blocking!")
	_ = <-proxyCommandLineConfiguration.stopChannel
}

func main() {
	flag.Parse()
	proxyCommandLineConfiguration.main()
}
