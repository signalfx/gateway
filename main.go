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

	"context"
	"expvar"
	"fmt"
	"github.com/signalfx/golib/eventcounter"
	"github.com/signalfx/golib/httpdebug"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/dp/dpbuffered"
	"github.com/signalfx/metricproxy/protocol/demultiplexer"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	// Version is set by a build flag to the built version
	Version = "0.9.10+"
	// BuildDate is set by a build flag to the date of the build
	BuildDate = ""
)

func writePidFile(pidFileName string) error {
	pid := os.Getpid()
	return ioutil.WriteFile(pidFileName, []byte(strconv.FormatInt(int64(pid), 10)), os.FileMode(0644))
}

type proxyFlags struct {
	configFileName string
}

type proxy struct {
	flags               proxyFlags
	listeners           []protocol.Listener
	forwarders          []protocol.Forwarder
	logger              log.Logger
	setupDoneSignal     chan struct{}
	tk                  timekeeper.TimeKeeper
	debugServer         *httpdebug.Server
	debugServerListener net.Listener
	stdout              io.Writer
	gomaxprocs          func(int) int
	debugContext        web.HeaderCtxFlag
	debugSink           dpsink.ItemFlagger
	ctxDims             log.CtxDimensions
	signalChan          chan os.Signal
	config              *config.ProxyConfig
}

var mainInstance = proxy{
	tk:         timekeeper.RealTime{},
	logger:     log.DefaultLogger.CreateChild(),
	stdout:     os.Stdout,
	gomaxprocs: runtime.GOMAXPROCS,
	debugContext: web.HeaderCtxFlag{
		HeaderName: "X-Debug-Id",
	},
	debugSink: dpsink.ItemFlagger{
		EventMetaName:       "dbg_events",
		MetricDimensionName: "sf_metric",
	},
	signalChan: make(chan os.Signal, 1),
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

func getHostname(osHostname func() (string, error)) string {
	name, err := osHostname()
	if err != nil {
		return "unknown"
	}
	return name
}

func setupForwarders(ctx context.Context, hostname string, tk timekeeper.TimeKeeper, loader *config.Loader, loadedConfig *config.ProxyConfig, logger log.Logger, scheduler *sfxclient.Scheduler, Checker *dpsink.ItemFlagger, cdim *log.CtxDimensions) ([]protocol.Forwarder, error) {
	allForwarders := make([]protocol.Forwarder, 0, len(loadedConfig.ForwardTo))
	for idx, forwardConfig := range loadedConfig.ForwardTo {
		logCtx := log.NewContext(logger).With(logkey.Protocol, forwardConfig.Type, logkey.Direction, "forwarder")
		forwarder, err := loader.Forwarder(forwardConfig)
		if err != nil {
			logCtx.Log(log.Err, err, "unable to load config")
			return nil, err
		}
		name := forwarderName(forwardConfig)
		logCtx = logCtx.With(logkey.Name, name)
		// Buffering -> counting -> (forwarder)
		limitedLogger := &log.RateLimitedLogger{
			EventCounter: eventcounter.New(tk.Now(), time.Second),
			Limit:        16,
			Logger:       logCtx,
			Now:          tk.Now,
		}
		count := &dpsink.Counter{
			Logger: limitedLogger,
		}
		endingSink := dpsink.FromChain(forwarder, dpsink.NextWrap(count))
		bconf := &dpbuffered.Config{
			Checker:            Checker,
			BufferSize:         forwardConfig.BufferSize,
			MaxTotalDatapoints: forwardConfig.BufferSize,
			MaxTotalEvents:     forwardConfig.BufferSize,
			MaxDrainSize:       forwardConfig.MaxDrainSize,
			NumDrainingThreads: forwardConfig.DrainingThreads,
			Cdim:               cdim,
		}
		bf := dpbuffered.NewBufferedForwarder(ctx, bconf, endingSink, limitedLogger)
		allForwarders = append(allForwarders, bf)

		groupName := fmt.Sprintf("%s_f_%d", name, idx)

		scheduler.AddGroupedCallback(groupName, forwarder)
		scheduler.AddGroupedCallback(groupName, bf)
		scheduler.GroupedDefaultDimensions(groupName, map[string]string{
			"name":      name,
			"direction": "forwarder",
			"source":    "proxy",
			"host":      hostname,
			"type":      forwardConfig.Type,
		})
	}
	return allForwarders, nil
}

func setupListeners(tk timekeeper.TimeKeeper, hostname string, loader *config.Loader, loadedConfig *config.ProxyConfig, multiplexer dpsink.Sink, logger log.Logger, scheduler *sfxclient.Scheduler) ([]protocol.Listener, error) {
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
				Limit:        16,
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
			"source":    "proxy",
			"host":      hostname,
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
	listener, err := net.Listen("tcp", *conf.LocalDebugServer)
	if err != nil {
		return errors.Annotate(err, "cannot setup debug server")
	}
	p.debugServerListener = listener
	p.debugServer = httpdebug.New(&httpdebug.Config{
		Logger:        log.NewContext(logger).With(logkey.Protocol, "debugserver"),
		ExplorableObj: p,
	})
	p.debugServer.Mux.Handle("/debug/dims", &p.debugSink)

	p.debugServer.Exp2.Exported["config"] = conf.Var()
	p.debugServer.Exp2.Exported["datapoints"] = scheduler.Var()
	p.debugServer.Exp2.Exported["goruntime"] = expvar.Func(func() interface{} {
		return runtime.Version()
	})
	p.debugServer.Exp2.Exported["debugdims"] = p.debugSink.Var()
	p.debugServer.Exp2.Exported["proxy_version"] = expvar.Func(func() interface{} {
		return Version
	})
	p.debugServer.Exp2.Exported["build_date"] = expvar.Func(func() interface{} {
		return BuildDate
	})
	p.debugServer.Exp2.Exported["source"] = expvar.Func(func() interface{} {
		return fmt.Sprintf("https://github.com/signalfx/metricproxy/tree/%s", Version)
	})

	go func() {
		err := p.debugServer.Serve(listener)
		logger.Log(log.Err, err, "Finished serving debug server")
	}()
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

func (p *proxy) gracefulShutdown() (err error) {
	p.logger.Log("Starting graceful shutdown")
	totalWaitTime := p.tk.After(*p.config.MaxGracefulWaitTimeDuration)
	errs := make([]error, len(p.listeners)+len(p.forwarders)+1)

	// close health checks on all first
	for _, l := range p.listeners {
		l.CloseHealthCheck()
	}

	// defer close of listeners and forwarders till we exit
	defer func() {
		for _, l := range p.listeners {
			errs = append(errs, l.Close())
		}
		for _, f := range p.forwarders {
			errs = append(errs, f.Close())
		}

		errs = append(errs, p.Close())
		err = errors.NewMultiErr(errs)
		p.logger.Log("Graceful shutdown done")
	}()

	p.logger.Log("Waiting for connections to drain")
	startingTimeGood := p.tk.Now()
	for {
		select {
		case <-totalWaitTime:
			totalPipeline := p.Pipeline()
			p.logger.Log(logkey.TotalPipeline, totalPipeline, "Connections never drained.  This could be bad ...")
			return

		case <-p.tk.After(*p.config.GracefulCheckIntervalDuration):
			now := p.tk.Now()
			totalPipeline := p.Pipeline()
			p.logger.Log(logkey.TotalPipeline, totalPipeline, "Waking up for graceful shutdown")
			if totalPipeline > 0 {
				p.logger.Log(logkey.TotalPipeline, totalPipeline, "Items are still draining")
				startingTimeGood = now
				continue
			}
			if now.Sub(startingTimeGood) >= *p.config.SilentGracefulTimeDuration {
				p.logger.Log(logkey.TotalPipeline, totalPipeline, "I've been silent.  Graceful shutdown done")
				return
			}
		}
	}
}

func (p *proxy) Pipeline() int64 {
	var totalForwarded int64
	for _, f := range p.forwarders {
		totalForwarded += f.Pipeline()
	}
	return totalForwarded
}

func (p *proxy) Close() error {
	errs := make([]error, len(p.forwarders)+1)
	for _, f := range p.forwarders {
		errs = append(errs, f.Close())
	}
	if p.debugServer != nil {
		errs = append(errs, p.debugServerListener.Close())
	}
	return errors.NewMultiErr(errs)
}

func (p *proxy) main(ctx context.Context) error {
	// Disable the default logger to make sure nobody else uses it
	err := p.run(ctx)
	return errors.NewMultiErr([]error{err, p.Close()})
}

func (p *proxy) setup(loadedConfig *config.ProxyConfig) {
	if loadedConfig.DebugFlag != nil && *loadedConfig.DebugFlag != "" {
		p.debugContext.SetFlagStr(*loadedConfig.DebugFlag)
	}
	p.config = loadedConfig
	p.logger = log.NewContext(p.getLogger(loadedConfig)).With(logkey.Time, log.DefaultTimestamp, logkey.Caller, log.DefaultCaller)
	p.debugSink.Logger = p.logger
	log.DefaultLogger.Set(p.logger)
	pidFilename := *loadedConfig.PidFilename
	if err := writePidFile(pidFilename); err != nil {
		p.logger.Log(log.Err, err, logkey.Filename, pidFilename, "cannot store pid in pid file")
	}
	defer func() {
		log.IfErr(p.logger, os.Remove(pidFilename))
	}()
	defer func() {
		log.DefaultLogger.Set(log.Discard)
	}()
}

func (p *proxy) createCommonHTTPChain(loadedConfig *config.ProxyConfig) web.NextConstructor {
	h := web.HeadersInRequest{
		Headers: map[string]string{
			"X-Proxy-Name": *loadedConfig.ServerName,
		},
	}
	cf := &web.CtxWithFlag{
		CtxFlagger: &p.ctxDims,
		HeaderName: "X-Response-Id",
	}
	return web.NextConstructor(func(ctx context.Context, rw http.ResponseWriter, r *http.Request, next web.ContextHandler) {
		cf.ServeHTTPC(ctx, rw, r, h.CreateMiddleware(next))
	})
}

func (p *proxy) run(ctx context.Context) error {
	p.debugSink.CtxFlagCheck = &p.debugContext
	hostname := getHostname(os.Hostname)
	p.logger.Log(logkey.ConfigFile, p.flags.configFileName, "Looking for config file")
	p.logger.Log(logkey.Env, strings.Join(os.Environ(), "-"), "Looking for config file")

	loadedConfig, err := config.Load(p.flags.configFileName, p.logger)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to load config")
		return err
	}
	p.setup(loadedConfig)
	logger := p.logger

	logger.Log(logkey.Config, loadedConfig, logkey.Env, strings.Join(os.Environ(), "-"), "config loaded")

	setupGoMaxProcs(loadedConfig.NumProcs, p.gomaxprocs)

	chain := p.createCommonHTTPChain(loadedConfig)
	loader := config.NewLoader(ctx, logger, Version, &p.debugContext, &p.debugSink, &p.ctxDims, chain)
	scheduler := sfxclient.NewScheduler()
	scheduler.AddCallback(sfxclient.GoMetricsSource)
	scheduler.DefaultDimensions(map[string]string{
		"source": "proxy",
		"host":   hostname,
	})

	forwarders, err := setupForwarders(ctx, hostname, p.tk, loader, loadedConfig, logger, scheduler, &p.debugSink, &p.ctxDims)
	if err != nil {
		return errors.Annotate(err, "unable to setup forwarders")
	}
	p.forwarders = forwarders
	dpSinks, eSinks := splitSinks(forwarders)

	multiplexer := dpsink.FromChain(&demultiplexer.Demultiplexer{
		DatapointSinks: dpSinks,
		EventSinks:     eSinks,
	}, dpsink.NextWrap(&p.debugSink))

	listeners, err := setupListeners(p.tk, hostname, loader, loadedConfig, multiplexer, logger, scheduler)
	if err != nil {
		return errors.Annotate(err, "cannot setup listeners from configuration")
	}
	p.listeners = listeners

	// We still want to schedule stat collection so people can debug the server if they want
	scheduler.Sink = dpsink.Discard
	scheduler.ReportingDelayNs = (time.Second * 30).Nanoseconds()
	wg := sync.WaitGroup{}
	finishedContext, cancelFunc := context.WithCancel(ctx)
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
		err := scheduler.Schedule(finishedContext)
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
	select {
	case <-ctx.Done():
	case <-p.signalChan:
		err = p.gracefulShutdown()
	}
	cancelFunc()
	wg.Wait()
	return err
}

var flagParse = flag.Parse

func main() {
	flagParse()
	signal.Notify(mainInstance.signalChan, syscall.SIGTERM)
	log.IfErr(log.DefaultLogger, mainInstance.main(context.Background()))
}
