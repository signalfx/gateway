package main

import (
	"flag"
	"io"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"path"
	"runtime"
	"strconv"

	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/demultiplexer"
	"golang.org/x/net/context"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	"strings"
	"sync"
	"time"
)

const versionString = "0.9.0"

func writePidFile(pidFileName string) error {
	pid := os.Getpid()
	return ioutil.WriteFile(pidFileName, []byte(strconv.FormatInt(int64(pid), 10)), os.FileMode(0644))
}

type proxyFlags struct {
	configFileName string
}

type proxy struct {
	flags           proxyFlags
	allListeners    []protocol.Listener
	allDPForwarders []protocol.Forwarder
	logger          *log.Hierarchy
	setupDoneSignal chan struct{}
	tk              timekeeper.TimeKeeper
}

var mainInstance = proxy{
	tk:     timekeeper.RealTime{},
	logger: log.DefaultLogger.CreateChild(),
}

func init() {
	flag.StringVar(&mainInstance.flags.configFileName, "configfile", "sf/sfdbproxy.conf", "Name of the db proxy configuration file")
}

func (p *proxy) getLogOutput(loadedConfig *config.ProxyConfig) io.Writer {
	logDir := *loadedConfig.LogDir
	if logDir == "-" {
		p.logger.Log("Sending logging to stdout")
		return os.Stdout
	}
	logMaxSize := *loadedConfig.LogMaxSize
	logMaxBackups := *loadedConfig.LogMaxBackups
	lumberjackLogger := &lumberjack.Logger{
		Filename:   path.Join(logDir, "metricproxy.log"),
		MaxSize:    logMaxSize, // megabytes
		MaxBackups: logMaxBackups,
	}
	p.logger.Log(logkey.Filename, lumberjackLogger.Filename, logkey.Dir, os.TempDir(), "Logging redirect setup")
	return lumberjackLogger
}

func (p *proxy) getLogger(loadedConfig *config.ProxyConfig) log.Logger {
	out := p.getLogOutput(loadedConfig)
	useJSON := *loadedConfig.LogFormat == "json"
	if useJSON {
		return log.NewJSONLogger(out, log.DefaultErrorHandler)
	}
	return log.NewLogfmtLogger(out, log.DefaultErrorHandler)
}

func setupForwarders(loader *config.Loader, loadedConfig *config.ProxyConfig, logger log.Logger, scheduler *sfxclient.Scheduler) ([]protocol.Forwarder, error) {
	allForwarders := make([]protocol.Forwarder, 0, len(loadedConfig.ForwardTo))
	for _, forwardConfig := range loadedConfig.ForwardTo {
		logCtx := log.NewContext(logger).With(logkey.Protocol, forwardConfig.Type, logkey.Direction, "forwarder")
		forwarder, err := loader.Forwarder(forwardConfig)
		if err != nil {
			logCtx.Log(log.Err, err, "unable to load config")
			return nil, err
		}
		allForwarders = append(allForwarders, forwarder)
		name := func() string {
			if forwardConfig.Name != nil {
				return *forwardConfig.Name
			}
			return forwardConfig.Type
		}()
		// TODO: Add buffering and counting
		scheduler.AddGroupedCallback(name+"_f", forwarder)
		scheduler.GroupedDefaultDimensions(name+"_f", map[string]string{
			"name":      name,
			"direction": "forwarder",
			"type":      forwardConfig.Type,
		})
	}
	return allForwarders, nil
}

func setupListeners(loader *config.Loader, loadedConfig *config.ProxyConfig, multiplexer dpsink.Sink, logger log.Logger, scheduler *sfxclient.Scheduler) ([]protocol.Listener, error) {
	allListeners := make([]protocol.Listener, 0, len(loadedConfig.ListenFrom))
	for _, listenConfig := range loadedConfig.ListenFrom {
		logCtx := log.NewContext(logger).With(logkey.Protocol, listenConfig.Type, logkey.Direction, "listener")
		listener, err := loader.Listener(multiplexer, listenConfig)
		if err != nil {
			logCtx.Log(log.Err, err, "unable to load config")
			return nil, err
		}
		allListeners = append(allListeners, listener)
		name := func() string {
			if listenConfig.Name != nil {
				return *listenConfig.Name
			}
			return listenConfig.Type
		}()
		// TODO: Add counting
		scheduler.AddGroupedCallback(name+"_l", listener)
		scheduler.GroupedDefaultDimensions(name+"_l", map[string]string{
			"name":      name,
			"direction": "listener",
			"type":      listenConfig.Type,
		})
	}
	return allListeners, nil
}

func splitSinks(forwarders []protocol.Forwarder) ([]dpsink.DSink, []dpsink.ESink, error) {
	dsinks := make([]dpsink.DSink, 0, len(forwarders))
	esinks := make([]dpsink.ESink, 0, len(forwarders))
	for _, f := range forwarders {
		dsink, isDsink := f.(dpsink.DSink)
		esink, isEsink := f.(dpsink.ESink)
		if !isDsink && !isEsink {
			return nil, nil, errors.Errorf("forwarder %s is neither datapoint nor event sink", f)
		}
		if isDsink {
			dsinks = append(dsinks, dsink)
		}
		if isEsink {
			esinks = append(esinks, esink)
		}
	}
	return dsinks, esinks, nil
}

//func setupDebugServer(allKeepers []stats.Keeper, loadedConfig *config.ProxyConfig, debugServerAddrFromCmd string, logger log.Logger) {
//	debugServerAddr := loadedConfig.LocalDebugServer
//	if debugServerAddr == nil {
//		debugServerAddr = &debugServerAddrFromCmd
//	}
//	logCtx := log.NewContext(logger).With(logkey.Protocol, "debugserver")
//	if debugServerAddr != nil && *debugServerAddr != "" {
//		go func() {
//			statusPage := statuspage.New(loadedConfig, allKeepers)
//			logCtx.Log(logkey.DebugAddr, debugServerAddr, "opening debug server")
//			http.Handle("/status", statusPage.StatusPage())
//			http.Handle("/health", statusPage.HealthPage())
//			err := http.ListenAndServe(*debugServerAddr, nil)
//			logCtx.Log(log.Err, err, "finished listening")
//		}()
//	}
//}

func (p *proxy) main(ctx context.Context) error {
	p.logger.Log(logkey.ConfigFile, p.flags.configFileName, "Looking for config file")
	p.logger.Log(logkey.Env, os.Environ(), "Looking for config file")

	loadedConfig, err := config.Load(p.flags.configFileName, p.logger)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to load config")
		return err
	}
	logger := log.NewContext(p.getLogger(loadedConfig)).With(logkey.Time, log.DefaultTimestamp, logkey.Caller, log.DefaultCaller)
	p.logger = nil
	// Disable the default logger to make sure nobody else uses it
	log.DefaultLogger.Set(logger)
	defer func() {
		log.DefaultLogger.Set(log.Discard)
	}()
	pidFilename := *loadedConfig.PidFilename
	if err := writePidFile(pidFilename); err != nil {
		logger.Log(log.Err, err, logkey.Filename, pidFilename, "cannot store pid in pid file")
	}
	logger.Log(logkey.Config, loadedConfig, logkey.Env, strings.Join(os.Environ(), "-"), "config loaded")

	if loadedConfig.NumProcs != nil {
		runtime.GOMAXPROCS(*loadedConfig.NumProcs)
	} else {
		numProcs := runtime.NumCPU()
		runtime.GOMAXPROCS(numProcs)
	}

	loader := config.NewLoader(ctx, logger, versionString)
	scheduler := sfxclient.NewScheduler()
	scheduler.AddCallback(sfxclient.GoMetricsSource)

	forwarders, err := setupForwarders(loader, loadedConfig, logger, scheduler)
	if err != nil {
		return errors.Annotate(err, "unable to setup forwarders")
	}
	p.allDPForwarders = forwarders
	defer func() {
		for _, f := range forwarders {
			if err := f.Close(); err != nil {
				logger.Log(log.Err, err, "cannot close forwarder")
			}
		}
	}()
	dpSinks, eSinks, err := splitSinks(forwarders)
	if err != nil {
		return errors.Annotate(err, "cannot split forwarding sinks")
	}

	multiplexer := &demultiplexer.Demultiplexer{
		DatapointSinks: dpSinks,
		EventSinks:     eSinks,
	}

	listeners, err := setupListeners(loader, loadedConfig, multiplexer, logger, scheduler)
	if err != nil {
		return errors.Annotate(err, "cannot setup listeners from configuration")
	}
	p.allListeners = listeners
	defer func() {
		for _, l := range listeners {
			if err := l.Close(); err != nil {
				logger.Log(log.Err, err, "cannot close listener")
			}
		}
	}()

	// We still want to schedule stat collection so people can debug the server if they want
	scheduler.Sink = dpsink.Discard
	scheduler.ReportingDelayNs = (time.Second * 30).Nanoseconds()
	wg := sync.WaitGroup{}
	finisehdContext, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		scheduler.Sink = multiplexer
		scheduler.ReportingDelayNs = loadedConfig.StatsDelayDuration.Nanoseconds()
		wg.Add(1)
		go func() {
			err := scheduler.Schedule(finisehdContext)
			logger.Log(log.Err, err, logkey.Struct, "scheduler", "Schedule finished")
			wg.Done()
		}()
	} else {
		logger.Log("skipping stat keeping")
	}

	//	setupDebugServer(allKeepers, loadedConfig, p.pprofaddr, logger)

	logger.Log("Setup done.  Blocking!")
	if p.setupDoneSignal != nil {
		close(p.setupDoneSignal)
	}
	<-ctx.Done()
	cancelFunc()
	wg.Wait()
	return nil
}

var flagParse = flag.Parse

func main() {
	flagParse()
	mainInstance.main(context.Background())
}
