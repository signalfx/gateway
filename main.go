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

	"expvar"
	"fmt"
	"github.com/signalfx/golib/eventcounter"
	"github.com/signalfx/metricproxy/debug"
	"github.com/signalfx/metricproxy/dp/dpbuffered"
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
	listeners       []protocol.Listener
	forwarders      []protocol.Forwarder
	logger          log.Logger
	setupDoneSignal chan struct{}
	tk              timekeeper.TimeKeeper
	debugServer     *debug.Server
	stdout          io.Writer
	gomaxprocs      func(int) int
}

var mainInstance = proxy{
	tk:         timekeeper.RealTime{},
	logger:     log.DefaultLogger.CreateChild(),
	stdout:     os.Stdout,
	gomaxprocs: runtime.GOMAXPROCS,
}

func init() {
	flag.StringVar(&mainInstance.flags.configFileName, "configfile", "sf/sfdbproxy.conf", "Name of the db proxy configuration file")
}

func (p *proxy) getLogOutput(loadedConfig *config.ProxyConfig) io.Writer {
	logDir := *loadedConfig.LogDir
	if logDir == "-" {
		p.logger.Log("Sending logging to stdout")
		return p.stdout
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

func forwarderName(f *config.ForwardTo) string {
	if f.Name != nil {
		return *f.Name
	}
	return f.Type
}

func setupForwarders(ctx context.Context, tk timekeeper.TimeKeeper, loader *config.Loader, loadedConfig *config.ProxyConfig, logger log.Logger, scheduler *sfxclient.Scheduler) ([]protocol.Forwarder, error) {
	allForwarders := make([]protocol.Forwarder, 0, len(loadedConfig.ForwardTo))
	for idx, forwardConfig := range loadedConfig.ForwardTo {
		logCtx := log.NewContext(logger).With(logkey.Protocol, forwardConfig.Type, logkey.Direction, "forwarder")
		forwarder, err := loader.Forwarder(forwardConfig)
		if err != nil {
			logCtx.Log(log.Err, err, "unable to load config")
			return nil, err
		}
		name := forwarderName(forwardConfig)
		// Buffering -> counting -> (forwarder)
		limitedLogger := &log.RateLimitedLogger{
			EventCounter: eventcounter.New(tk.Now(), time.Second),
			Limit:        2, // Only 1 a second
			Logger:       logCtx,
			Now:          tk.Now,
		}
		count := &dpsink.Counter{
			Logger: limitedLogger,
		}
		endingSink := dpsink.FromChain(forwarder, dpsink.NextWrap(count))
		bconf := &dpbuffered.Config{
			BufferSize:         forwardConfig.BufferSize,
			MaxTotalDatapoints: forwardConfig.BufferSize,
			MaxTotalEvents:     forwardConfig.BufferSize,
			MaxDrainSize:       forwardConfig.MaxDrainSize,
			NumDrainingThreads: forwardConfig.DrainingThreads,
		}
		bf := dpbuffered.NewBufferedForwarder(ctx, bconf, endingSink, limitedLogger)
		allForwarders = append(allForwarders, bf)

		groupName := fmt.Sprintf("%s_f_%d", name, idx)

		scheduler.AddGroupedCallback(groupName, forwarder)
		scheduler.GroupedDefaultDimensions(groupName, map[string]string{
			"name":      name,
			"direction": "forwarder",
			"type":      forwardConfig.Type,
		})
	}
	return allForwarders, nil
}

func setupListeners(tk timekeeper.TimeKeeper, loader *config.Loader, loadedConfig *config.ProxyConfig, multiplexer dpsink.Sink, logger log.Logger, scheduler *sfxclient.Scheduler) ([]protocol.Listener, error) {
	listeners := make([]protocol.Listener, 0, len(loadedConfig.ListenFrom))
	for idx, listenConfig := range loadedConfig.ListenFrom {
		logCtx := log.NewContext(logger).With(logkey.Protocol, listenConfig.Type, logkey.Direction, "listener")
		name := func() string {
			if listenConfig.Name != nil {
				return *listenConfig.Name
			}
			return listenConfig.Type
		}()
		count := &dpsink.Counter{
			Logger: &log.RateLimitedLogger{
				EventCounter: eventcounter.New(tk.Now(), time.Second),
				Limit:        1, // Only 1 a second
				Logger:       logCtx,
				Now:          tk.Now,
			},
		}
		endingSink := dpsink.FromChain(multiplexer, dpsink.NextWrap(count))

		listener, err := loader.Listener(endingSink, listenConfig)
		if err != nil {
			logCtx.Log(log.Err, err, "unable to load config")
			return nil, err
		}
		listeners = append(listeners, listener)
		groupName := fmt.Sprintf("%s_l_%d", name, idx)
		scheduler.AddGroupedCallback(groupName, listener)
		scheduler.AddGroupedCallback(groupName, count)
		scheduler.GroupedDefaultDimensions(groupName, map[string]string{
			"name":      name,
			"direction": "listener",
			"type":      listenConfig.Type,
		})
	}
	return listeners, nil
}

func splitSinks(forwarders []protocol.Forwarder) ([]dpsink.DSink, []dpsink.ESink) {
	dsinks := make([]dpsink.DSink, 0, len(forwarders))
	esinks := make([]dpsink.ESink, 0, len(forwarders))
	for _, f := range forwarders {
		dsinks = append(dsinks, f)
		esinks = append(esinks, f)
	}
	return dsinks, esinks
}

func (p *proxy) setupDebugServer(conf *config.ProxyConfig, logger log.Logger, scheduler *sfxclient.Scheduler) error {
	if conf.LocalDebugServer == nil {
		return nil
	}
	debugServer, err := debug.New(*conf.LocalDebugServer, p, &debug.Config{
		Logger: log.NewContext(logger).With(logkey.Protocol, "debugserver"),
	})
	if err != nil {
		return errors.Annotate(err, "cannot setup debug server")
	}
	p.debugServer = debugServer
	p.debugServer.Exp2.Exported["config"] = conf.Var()
	p.debugServer.Exp2.Exported["datapoints"] = scheduler.Var()
	p.debugServer.Exp2.Exported["goruntime"] = expvar.Func(func() interface{} {
		return runtime.Version()
	})
	p.debugServer.Exp2.Exported["proxy_version"] = expvar.Func(func() interface{} {
		return versionString
	})
	return nil
}

func setupGoMaxProcs(numProcs *int, gomaxprocs func(int) int) {
	if numProcs != nil {
		gomaxprocs(*numProcs)
	} else {
		numProcs := runtime.NumCPU()
		gomaxprocs(numProcs)
	}
}

func (p *proxy) Close() error {
	errs := make([]error, len(p.forwarders)+len(p.listeners)+1)
	for _, f := range p.forwarders {
		errs = append(errs, f.Close())
	}
	for _, l := range p.listeners {
		errs = append(errs, l.Close())
	}
	if p.debugServer != nil {
		errs = append(errs, p.debugServer.Close())
	}
	return errors.NewMultiErr(errs)
}

func (p *proxy) main(ctx context.Context) error {
	// Disable the default logger to make sure nobody else uses it
	err := p.run(ctx)
	return errors.NewMultiErr([]error{err, p.Close()})
}

func (p *proxy) run(ctx context.Context) error {
	p.logger.Log(logkey.ConfigFile, p.flags.configFileName, "Looking for config file")
	p.logger.Log(logkey.Env, os.Environ(), "Looking for config file")

	loadedConfig, err := config.Load(p.flags.configFileName, p.logger)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to load config")
		return err
	}
	p.logger = log.NewContext(p.getLogger(loadedConfig)).With(logkey.Time, log.DefaultTimestamp, logkey.Caller, log.DefaultCaller)
	logger := p.logger
	log.DefaultLogger.Set(p.logger)
	defer func() {
		log.DefaultLogger.Set(log.Discard)
	}()

	pidFilename := *loadedConfig.PidFilename
	if err := writePidFile(pidFilename); err != nil {
		logger.Log(log.Err, err, logkey.Filename, pidFilename, "cannot store pid in pid file")
	}
	defer os.Remove(pidFilename)
	logger.Log(logkey.Config, loadedConfig, logkey.Env, strings.Join(os.Environ(), "-"), "config loaded")

	setupGoMaxProcs(loadedConfig.NumProcs, p.gomaxprocs)

	loader := config.NewLoader(ctx, logger, versionString)
	scheduler := sfxclient.NewScheduler()
	scheduler.AddCallback(sfxclient.GoMetricsSource)

	forwarders, err := setupForwarders(ctx, p.tk, loader, loadedConfig, logger, scheduler)
	if err != nil {
		return errors.Annotate(err, "unable to setup forwarders")
	}
	p.forwarders = forwarders
	dpSinks, eSinks := splitSinks(forwarders)

	multiplexer := &demultiplexer.Demultiplexer{
		DatapointSinks: dpSinks,
		EventSinks:     eSinks,
	}

	listeners, err := setupListeners(p.tk, loader, loadedConfig, multiplexer, logger, scheduler)
	if err != nil {
		return errors.Annotate(err, "cannot setup listeners from configuration")
	}
	p.listeners = listeners

	// We still want to schedule stat collection so people can debug the server if they want
	scheduler.Sink = dpsink.Discard
	scheduler.ReportingDelayNs = (time.Second * 30).Nanoseconds()
	wg := sync.WaitGroup{}
	finisehdContext, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		scheduler.Sink = multiplexer
		scheduler.ReportingDelayNs = loadedConfig.StatsDelayDuration.Nanoseconds()
	} else {
		logger.Log("skipping stat keeping")
	}

	// Schedule datapoint collection to a Discard sink so we can get the stats in Expvar()
	wg.Add(1)
	go func() {
		err := scheduler.Schedule(finisehdContext)
		logger.Log(log.Err, err, logkey.Struct, "scheduler", "Schedule finished")
		wg.Done()
	}()
	if err := p.setupDebugServer(loadedConfig, logger, scheduler); err != nil {
		return err
	}
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
