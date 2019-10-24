package main

import (
	"context"
	"expvar"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	etcdcli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/gorilla/mux"
	"github.com/signalfx/embetcd/embetcd"
	"github.com/signalfx/gateway/collectorhandler"
	"github.com/signalfx/gateway/config"
	"github.com/signalfx/gateway/dp/dpbuffered"
	"github.com/signalfx/gateway/etcdIntf"
	"github.com/signalfx/gateway/flaghelpers"
	"github.com/signalfx/gateway/hub"
	"github.com/signalfx/gateway/hub/hubclient"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/gateway/protocol"
	"github.com/signalfx/gateway/protocol/demultiplexer"
	"github.com/signalfx/gateway/protocol/signalfx"
	_ "github.com/signalfx/go-distribute"
	_ "github.com/signalfx/go-metrics"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/eventcounter"
	"github.com/signalfx/golib/httpdebug"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/reportsha"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/golib/web"
	_ "github.com/signalfx/ondiskencoding"
	_ "github.com/signalfx/tdigest"
	_ "github.com/spaolacci/murmur3"
	"gopkg.in/natefinch/lumberjack.v2"
	_ "stathat.com/c/consistent"
)

const (
	clusterOpFlag        = "cluster-op"
	versionFlag          = "version"
	gatewayMetricsPrefix = "gateway."
)

var (
	// Version is set by a build flag to the built version
	Version = "0.9.10+"
	// BuildDate is set by a build flag to the date of the build
	BuildDate = ""
)

// gatewayFlags is a struct used to store runtime flags for the gateway
type gatewayFlags struct {
	configFileName string
	operation      flaghelpers.StringFlag
	version        bool
}

// addFlagsToConfig applies the flags to a config struct
func (g *gatewayFlags) addFlagsToConfig(loadedConfig *config.GatewayConfig) {
	if g.operation.IsSet() {
		loadedConfig.ClusterOperation = pointer.String(g.operation.String())
	}
}

var flags *gatewayFlags
var flagParse func()

// package init
func init() {
	// initialize the runtime flags for the package
	flags = &gatewayFlags{}
	flagParse = flag.Parse
	flag.StringVar(&flags.configFileName, "configfile", "sf/gateway.conf", "Name of the db gateway configuration file")
	flag.Var(&flags.operation, clusterOpFlag, "operation to perform if running in cluster mode [\"seed\", \"join\", \"\"] this overrides the ClusterOperation set in the config file")
	flag.BoolVar(&flags.version, versionFlag, false, "positional argument to check gateway version")
}

// TODO: don't make this part of the gateway itself
func (p *gateway) getLogOutput(loadedConfig *config.GatewayConfig) io.Writer {
	logDir := *loadedConfig.LogDir
	if logDir == "-" {
		p.logger.Log("Sending logging to stdout")
		return p.stdout
	}
	logMaxSize := *loadedConfig.LogMaxSize
	logMaxBackups := *loadedConfig.LogMaxBackups
	lumberjackLogger := &lumberjack.Logger{
		Filename:   path.Join(logDir, "gateway.log"),
		MaxSize:    logMaxSize, // megabytes
		MaxBackups: logMaxBackups,
	}
	p.logger.Log(logkey.Filename, lumberjackLogger.Filename, logkey.Dir, os.TempDir(), "Logging redirect setup")
	return lumberjackLogger
}

// TODO: don't make this part of the gateway itself
func (p *gateway) getLogger(loadedConfig *config.GatewayConfig) log.Logger {
	out := p.getLogOutput(loadedConfig)
	useJSON := *loadedConfig.LogFormat == "json"
	if useJSON {
		return log.NewJSONLogger(out, log.DefaultErrorHandler)
	}
	return log.NewLogfmtLogger(out, log.DefaultErrorHandler)
}

// TODO: put gateway and related functions into a dedicated package that main.go imports
// gateway is a struct representing a gateway.  It must be instantiated, configured, started, and stopped
type gateway struct {
	listeners               []protocol.Listener
	forwarders              []protocol.Forwarder
	logger                  log.Logger
	setupDoneSignal         chan struct{}
	tk                      timekeeper.TimeKeeper
	debugServer             *httpdebug.Server
	debugServerListener     net.Listener
	internalMetricsServer   *collectorhandler.CollectorHandler
	internalMetricsListener net.Listener
	stdout                  io.Writer
	debugContext            web.HeaderCtxFlag
	debugSink               dpsink.ItemFlagger
	ctxDims                 log.CtxDimensions
	signalChan              chan os.Signal
	config                  *config.GatewayConfig
	hub                     hub.GatewayHub
	etcdServer              *embetcd.Server
	etcdClient              *embetcd.Client
	versionMetric           reportsha.SHA1Reporter
}

// newGateway returns a new gateway instance with any loaded flags
// flags are loaded as part of package init()
func newGateway() *gateway {
	return &gateway{
		tk:     timekeeper.RealTime{},
		logger: log.DefaultLogger.CreateChild(),
		stdout: os.Stdout,
		debugContext: web.HeaderCtxFlag{
			HeaderName: "X-Debug-Id",
		},
		debugSink: dpsink.ItemFlagger{
			EventMetaName:       "dbg_events",
			MetricDimensionName: "sf_metric",
		},
		setupDoneSignal: make(chan struct{}),
		signalChan:      make(chan os.Signal, 1),
	}
}

func forwarderName(f *config.ForwardTo) string {
	if f.Name != nil {
		return *f.Name
	}
	return f.Type
}

var errDupeForwarder = errors.New("cannot duplicate forwarder names or types without names")

func setupForwarders(ctx context.Context, tk timekeeper.TimeKeeper, loader *config.Loader, loadedConfig *config.GatewayConfig, logger log.Logger, debugMetricsScheduler *sfxclient.Scheduler, metricsScheduler *sfxclient.Scheduler, Checker *dpsink.ItemFlagger, cdim *log.CtxDimensions, etcdClient *embetcd.Client) ([]protocol.Forwarder, error) {
	allForwarders := make([]protocol.Forwarder, 0, len(loadedConfig.ForwardTo))
	nameMap := make(map[string]bool)
	for idx, forwardConfig := range loadedConfig.ForwardTo {
		logCtx := log.NewContext(logger).With(logkey.Protocol, forwardConfig.Type, logkey.Direction, "forwarder")
		if etcdClient != nil {
			forwardConfig.Client = etcdClient
		}
		forwardConfig.ClusterName = loadedConfig.ClusterName
		forwardConfig.AdditionalDimensions = datapoint.AddMaps(loadedConfig.AdditionalDimensions, forwardConfig.AdditionalDimensions)
		forwarder, err := loader.Forwarder(forwardConfig)
		if err != nil {
			return nil, err
		}
		name := forwarderName(forwardConfig)
		if nameMap[name] {
			logger.Log(fmt.Sprintf("Cannot add two forwarders with name '%s' or two unnamed forwarders of same type", name))
			return nil, errDupeForwarder
		}
		nameMap[name] = true
		logCtx = logCtx.With(logkey.Name, name)
		// Buffering -> counting -> (forwarder)
		limitedLogger := &log.RateLimitedLogger{
			EventCounter: eventcounter.New(tk.Now(), time.Second),
			Limit:        16,
			Logger:       logCtx,
			Now:          tk.Now,
		}
		dcount := &dpsink.Counter{
			Logger:        limitedLogger,
			DroppedReason: "downstream",
		}
		metricsScheduler.AddCallback(dcount)
		count := signalfx.UnifyNextSinkWrap(dcount)
		endingSink := signalfx.FromChain(forwarder, signalfx.NextWrap(count))
		bconf := &dpbuffered.Config{
			Checker:            Checker,
			BufferSize:         forwardConfig.BufferSize,
			MaxTotalDatapoints: forwardConfig.BufferSize,
			MaxTotalEvents:     forwardConfig.BufferSize,
			MaxTotalSpans:      forwardConfig.BufferSize,
			MaxDrainSize:       forwardConfig.MaxDrainSize,
			NumDrainingThreads: forwardConfig.DrainingThreads,
			Name:               forwardConfig.Name,
			Cdim:               cdim,
			UseAuthFromRequest: forwardConfig.UseAuthFromRequest,
		}
		bf := dpbuffered.NewBufferedForwarder(ctx, bconf, endingSink, forwarder.Close, forwarder.StartupFinished, limitedLogger, forwarder.DebugEndpoints)
		allForwarders = append(allForwarders, bf)

		groupName := fmt.Sprintf("%s_f_%d", name, idx)

		metricsScheduler.GroupedDefaultDimensions(groupName, datapoint.AddMaps(loadedConfig.AdditionalDimensions, map[string]string{
			"name":      name,
			"direction": "forwarder",
			"source":    "gateway",
			"host":      *loadedConfig.ServerName,
			"type":      forwardConfig.Type,
			"cluster":   *loadedConfig.ClusterName,
		}))
		metricsScheduler.AddGroupedCallback(groupName, sfxclient.CollectorFunc(forwarder.DefaultDatapoints))
		metricsScheduler.AddGroupedCallback(groupName, sfxclient.CollectorFunc(bf.DefaultDatapoints))

		debugMetricsScheduler.GroupedDefaultDimensions(groupName, datapoint.AddMaps(loadedConfig.AdditionalDimensions, map[string]string{
			"name":      name,
			"direction": "forwarder",
			"source":    "gateway",
			"host":      *loadedConfig.ServerName,
			"type":      forwardConfig.Type,
			"cluster":   *loadedConfig.ClusterName,
		}))
		debugMetricsScheduler.AddGroupedCallback(groupName, sfxclient.CollectorFunc(forwarder.DebugDatapoints))
		debugMetricsScheduler.AddGroupedCallback(groupName, sfxclient.CollectorFunc(bf.DebugDatapoints))

	}
	return allForwarders, nil
}

var errDupeListener = errors.New("cannot duplicate listener names or types without names")

func setupListeners(tk timekeeper.TimeKeeper, hostname string, loadedConfig *config.GatewayConfig, loader *config.Loader, listenFrom []*config.ListenFrom, multiplexer signalfx.Sink, logger log.Logger, debugMetricsScheduler *sfxclient.Scheduler, metricsScheduler *sfxclient.Scheduler) ([]protocol.Listener, error) {
	listeners := make([]protocol.Listener, 0, len(listenFrom))
	nameMap := make(map[string]bool)
	for idx, listenConfig := range listenFrom {
		logCtx := log.NewContext(logger).With(logkey.Protocol, listenConfig.Type, logkey.Direction, "listener")
		name := func() string {
			if listenConfig.Name != nil {
				return *listenConfig.Name
			}
			return listenConfig.Type
		}()
		if nameMap[name] {
			logger.Log(fmt.Sprintf("Cannot add two listeners with name '%s' or two unnamed listners of same type", name))
			return nil, errDupeListener
		}
		nameMap[name] = true
		limitedLogger := &log.RateLimitedLogger{
			EventCounter: eventcounter.New(tk.Now(), time.Second),
			Limit:        16,
			Logger:       logCtx,
			Now:          tk.Now,
		}
		listenConfig.Counter = &dpsink.Counter{
			Logger:        limitedLogger,
			DroppedReason: "mux",
		}

		listener, err := loader.Listener(multiplexer, listenConfig)
		if err != nil {
			logCtx.Log(log.Err, err, "unable to load config")
			return nil, err
		}
		listeners = append(listeners, listener)
		groupName := fmt.Sprintf("%s_l_%d", name, idx)
		debugMetricsScheduler.AddGroupedCallback(groupName, sfxclient.CollectorFunc(listener.DebugDatapoints))
		debugMetricsScheduler.GroupedDefaultDimensions(groupName, datapoint.AddMaps(loadedConfig.AdditionalDimensions, map[string]string{
			"name":      name,
			"direction": "listener",
			"source":    "gateway",
			"host":      hostname,
			"type":      listenConfig.Type,
			"cluster":   *loadedConfig.ClusterName,
		}))
		metricsScheduler.AddGroupedCallback(groupName, sfxclient.CollectorFunc(listener.DefaultDatapoints))
		metricsScheduler.GroupedDefaultDimensions(groupName, datapoint.AddMaps(loadedConfig.AdditionalDimensions, map[string]string{
			"name":      name,
			"direction": "listener",
			"source":    "gateway",
			"host":      hostname,
			"type":      listenConfig.Type,
			"cluster":   *loadedConfig.ClusterName,
		}))
	}
	return listeners, nil
}

func splitSinks(forwarders []protocol.Forwarder) ([]dpsink.DSink, []dpsink.ESink, []trace.Sink) {
	dsinks := make([]dpsink.DSink, 0, len(forwarders))
	esinks := make([]dpsink.ESink, 0, len(forwarders))
	tsinks := make([]trace.Sink, 0, len(forwarders))
	for _, f := range forwarders {
		dsinks = append(dsinks, f)
		esinks = append(esinks, f)
		tsinks = append(tsinks, f)
	}

	return dsinks, esinks, tsinks
}

func (p *gateway) setupInternalMetricsServer(conf *config.GatewayConfig, logger log.Logger, schedulersMap map[string]*sfxclient.Scheduler) error {
	if conf.InternalMetricsListenerAddress != nil && *conf.InternalMetricsListenerAddress != "" {
		listener, err := net.Listen("tcp", *conf.InternalMetricsListenerAddress)
		if err != nil {
			return errors.Annotate(err, "cannot setup internal metrics server")
		}
		p.internalMetricsListener = listener

		schedulers := make([]*sfxclient.Scheduler, 0, len(schedulersMap))
		for _, s := range schedulersMap {
			schedulers = append(schedulers, s)
		}

		collector := collectorhandler.NewCollectorHandler(schedulers...)
		handler := mux.NewRouter()
		handler.Path("/internal-metrics").HandlerFunc(collector.DatapointsHandler)
		p.internalMetricsServer = collector

		go func() {
			err := http.Serve(listener, handler)
			logger.Log(log.Msg, err, "Finished serving internal metrics server")
		}()
	}
	return nil
}

func (p *gateway) setupDebugServer(conf *config.GatewayConfig, logger log.Logger, debugMetricsScheduler *sfxclient.Scheduler, metricsScheduler *sfxclient.Scheduler) error {
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
	p.debugServer.Exp2.Exported["datapoints"] = expvar.Func(func() interface{} {
		return debugMetricsScheduler.CollectDatapoints()
	})
	p.debugServer.Exp2.Exported["goruntime"] = expvar.Func(func() interface{} {
		return runtime.Version()
	})
	p.debugServer.Exp2.Exported["debugdims"] = p.debugSink.Var()
	p.debugServer.Exp2.Exported["gateway_version"] = expvar.Func(func() interface{} {
		return Version
	})
	p.debugServer.Exp2.Exported["build_date"] = expvar.Func(func() interface{} {
		return BuildDate
	})
	p.debugServer.Exp2.Exported["source"] = expvar.Func(func() interface{} {
		return fmt.Sprintf("https://github.com/signalfx/gateway/tree/%s", Version)
	})
	p.debugServer.Exp2.Exported["gateway_metrics"] = expvar.Func(func() interface{} {
		return metricsScheduler.CollectDatapoints()
	})

	go func() {
		err := p.debugServer.Serve(listener)
		log.IfErrWithKeys(logger, err, log.Err, "error encountered in debug server")
		logger.Log(log.Msg, "Finished serving debug server")
	}()
	return nil
}

func (p *gateway) handleEndpoints(debugEndpoints map[string]http.Handler) {
	if p.debugServer != nil {
		for k, v := range debugEndpoints {
			p.debugServer.Mux.Handle(k, v)
		}
	}
}

// closeListenerHealthChecks
func closeListenerHealthChecks(listeners []protocol.Listener) {
	for _, l := range listeners {
		l.CloseHealthCheck()
	}
}

// closeListeners concurrently closes all of the listeners and wait for them to all close
func closeListeners(listeners []protocol.Listener) []error {
	errs := make([]error, len(listeners))
	wg := sync.WaitGroup{}
	wg.Add(len(listeners))
	for index, l := range listeners {
		go func(index int, l protocol.Listener, errs []error) {
			errs[index] = l.Close()
			wg.Done()
		}(index, l, errs)
	}
	wg.Wait()
	return errs
}

// closeForwarders concurrently close and drain all of the forwarders
func closeForwarders(forwarders []protocol.Forwarder) []error {
	errs := make([]error, len(forwarders))
	wg := sync.WaitGroup{}
	wg.Add(len(forwarders))
	for index, f := range forwarders {
		go func(index int, f protocol.Forwarder, errs []error) {
			errs[index] = f.Close()
			wg.Done()
		}(index, f, errs)
	}
	wg.Wait()
	return errs
}

// waitForForwardersToDrain waits for the pipeline of inflight things to drain across all forwarders
// or for the context to expire.  The passed in context should be the graceful shutdown timeout context.
// It should be cancelled when this function exceeds the configured graceful timeout duration
func (p *gateway) waitForForwardersToDrain(ctx context.Context, startTime time.Time) {
	p.logger.Log("Waiting for connections to drain")
	for {
		select {
		case <-ctx.Done():
			if totalPipeline := p.Pipeline(); totalPipeline != 0 {
				p.logger.Log(logkey.TotalPipeline, totalPipeline, "Connections never drained.  This could be bad ...")
			}
			return
		case <-p.tk.After(*p.config.GracefulCheckIntervalDuration):
			// wait for the total pipeline to get to 0
			if totalPipeline := p.Pipeline(); totalPipeline == 0 {
				return
			} else if time.Since(startTime) > *p.config.SilentGracefulTimeDuration {
				//p.logger.Log(logkey.TotalPipeline, totalPipeline, "Waking up for graceful shutdown")
				p.logger.Log(logkey.TotalPipeline, totalPipeline, "Items are still draining...")
			} // else continue looping
		}
	}
}

// GetContext returns the context passed in or creates a background context if the context passed in is nil
func GetContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func logIfCtxExceeded(ctx context.Context, logger log.Logger) {
	if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
		logger.Log("Exceeded graceful shutdown period")
	} else {
		logger.Log("Graceful shutdown complete")
	}
}

// Stop shutsdown a running gateway and utilizes the graceful shutdown timeout
func (p *gateway) stop(ctx context.Context) (err error) {
	startTime := p.tk.Now() // keeps track of the time when graceful shutdown began
	p.logger.Log("Starting graceful shutdown")

	errs := make([]error, len(p.listeners)+len(p.forwarders)+1)

	// close health checks on all first
	closeListenerHealthChecks(p.listeners)

	// create timeout context for graceful shutdown period
	timeout, cancel := context.WithTimeout(GetContext(ctx), *p.config.MaxGracefulWaitTimeDuration) // max graceful timeout context
	defer cancel()

	// unregister from the hub
	if *p.config.Cluster && p.hub.IsOpen() {
		errs = append(errs, log.IfErrWithKeysAndReturn(p.logger, p.hub.Unregister(timeout), log.Msg, "unable to deregister from hub"))
	}

	// wait for forwarder pipeline to drain
	p.waitForForwardersToDrain(timeout, startTime)

	// close listeners
	p.logger.Log("Close listeners")
	listenErrs := closeListeners(p.listeners)
	log.IfErr(p.logger, errors.NewMultiErr(listenErrs))
	errs = append(errs, listenErrs...)

	// close forwarders
	p.logger.Log("Close forwarders")
	fwdErrs := closeForwarders(p.forwarders)
	log.IfErr(p.logger, errors.NewMultiErr(fwdErrs))
	errs = append(errs, fwdErrs...)

	// Stop the etcd server using the timeout context from above for graceful shutdown
	// If the context is already expired it will do a hard stop of the etcd server.
	// if there is time left on the timeout it will attempt a graceful shutdown.
	// etcd should not be stopped until all forwarders have been stopped
	if p.etcdServer != nil && p.etcdServer.IsRunning() {
		errs = append(errs, p.etcdServer.Shutdown(timeout))
	}

	// The graceful part of shutdown is complete when all of the inflight pipeline is cleared and the
	// etcd server is shutdown.  Log whether we made it here before the graceful shutdown context timedout
	logIfCtxExceeded(ctx, p.logger)

	// close the etcd client if it is not nil
	if p.etcdClient != nil {
		errs = append(errs, p.etcdClient.Close())
	}

	// stop the GatewayHub instance
	if *p.config.Cluster && p.hub.IsOpen() {
		p.hub.Close()
		p.logger.Log("Stopped gateway hub routines")
	}

	// stop debug server listener
	if p.debugServer != nil {
		errs = append(errs, p.debugServerListener.Close())
	}

	// stop internal metric server listener
	if p.internalMetricsServer != nil {
		errs = append(errs, p.internalMetricsListener.Close())
	}

	// remove the pid file
	removePidFile(p.config, p.logger)

	return errors.NewMultiErr(errs)
}

// Pipeline returns the number of items in flight that need to be drained across all configured forwarders
func (p *gateway) Pipeline() int64 {
	var totalForwarded int64
	for _, f := range p.forwarders {
		totalForwarded += f.Pipeline()
	}
	return totalForwarded
}

// takes a gateway config and configures the gateway with it
func (p *gateway) configure(loadedConfig *config.GatewayConfig) error {
	if loadedConfig == nil {
		return fmt.Errorf("unable to configure gateway with nil config")
	}
	// save config to the gateway
	p.config = loadedConfig

	// set debug context from loaded config
	if loadedConfig.DebugFlag != nil && *loadedConfig.DebugFlag != "" {
		p.debugContext.SetFlagStr(*loadedConfig.DebugFlag)
	}

	// set debugSink ctx flag
	p.debugSink.CtxFlagCheck = &p.debugContext

	// TODO: allow arbitrary logger to be passed into configure function
	// set lumberjack log formatting using a child derived from the default logger
	// this is used as a back up by p.getLogger when we try to create a logger using the loaded config
	p.logger = log.NewContext(log.DefaultLogger.CreateChild()).With(logkey.Time, log.DefaultTimestamp, logkey.Caller, log.DefaultCaller)

	// NOTE: the main reason to do that ^ and apply the keys is so that our log messages look consistent.
	// there is are messages logged by p.getLogger before the keys are applied in the following statement

	// create a new logger using the loaded config
	p.logger = log.NewContext(p.getLogger(loadedConfig)).With(logkey.Time, log.DefaultTimestamp, logkey.Caller, log.DefaultCaller)

	// assign logger to debug sink
	p.debugSink.Logger = p.logger

	// setup logger on versionMetric which reports our SHA1
	p.versionMetric.Logger = p.logger

	// sets up the hub
	err := p.setupHub(p.config)
	log.IfErrWithKeys(p.logger, err, "Failed to set up gateway hub")

	return err
}

func (p *gateway) createCommonHTTPChain(loadedConfig *config.GatewayConfig) web.NextConstructor {
	h := web.HeadersInRequest{
		Headers: map[string]string{
			"X-Gateway-Name": *loadedConfig.ServerName,
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

func (p *gateway) setupScheduler(loadedConfig *config.GatewayConfig, prefix string) *sfxclient.Scheduler {
	scheduler := sfxclient.NewScheduler()
	scheduler.DefaultDimensions(datapoint.AddMaps(loadedConfig.AdditionalDimensions, map[string]string{
		"source":  "gateway",
		"host":    *loadedConfig.ServerName,
		"cluster": *loadedConfig.ClusterName,
	}))
	scheduler.Prefix = prefix
	return scheduler
}

func (p *gateway) setupForwardersAndListeners(ctx context.Context, loader *config.Loader, loadedConfig *config.GatewayConfig, logger log.Logger, debugMetricsScheduler *sfxclient.Scheduler, metricsScheduler *sfxclient.Scheduler) (signalfx.Sink, map[string]http.Handler, error) {
	var err error
	p.forwarders, err = setupForwarders(ctx, p.tk, loader, loadedConfig, logger, debugMetricsScheduler, metricsScheduler, &p.debugSink, &p.ctxDims, p.etcdClient)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to setup forwarders")
		return nil, nil, errors.Annotate(err, "unable to setup forwarders")
	}

	// register with the hub
	if *p.config.Cluster && p.hub.IsOpen() {
		// TODO extract distributor status and pass that along
		// TODO get ServerPayload for forwarders
		err = p.hub.Register(*loadedConfig.ServerName, *loadedConfig.ClusterName, Version, hubclient.ServerPayload(make(map[string][]byte)), false)
		p.logger.Log("Registered with the gateway hub with the following error", err)
	}

	dpSinks, eSinks, tSinks := splitSinks(p.forwarders)

	dmux := &demultiplexer.Demultiplexer{
		DatapointSinks: dpSinks,
		EventSinks:     eSinks,
		TraceSinks:     tSinks,
		Logger:         log.NewOnePerSecond(logger),
		LateDuration:   loadedConfig.LateThresholdDuration,
		FutureDuration: loadedConfig.FutureThresholdDuration,
	}

	// Add metrics from dmux to the to debug metrics scheduler
	debugMetricsScheduler.AddCallback(dmux)

	multiplexer := signalfx.FromChain(dmux, signalfx.NextWrap(signalfx.UnifyNextSinkWrap(&p.debugSink)))

	p.listeners, err = setupListeners(p.tk, *loadedConfig.ServerName, loadedConfig, loader, loadedConfig.ListenFrom, multiplexer, logger, debugMetricsScheduler, metricsScheduler)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to setup listeners")
		return nil, nil, errors.Annotate(err, "cannot setup listeners from configuration")
	}

	var errs []error
	endpoints := make(map[string]http.Handler)
	for _, f := range p.forwarders {
		err = f.StartupFinished()
		errs = append(errs, err)
		log.IfErr(logger, err)
		p.addEndpoints(f, endpoints)
	}

	return multiplexer, endpoints, FirstNonNil(errs...)
}

func (p *gateway) addEndpoints(f protocol.DebugEndpointer, endpoints map[string]http.Handler) {
	for k, v := range f.DebugEndpoints() {
		if _, ok := endpoints[k]; !ok {
			endpoints[k] = v
		}
	}
}

func (p *gateway) setClusterName(ctx context.Context, etcdClient etcdIntf.Client, clusterName string) (err error) {
	for ctx.Err() == nil {
		timeout, cancel := context.WithTimeout(ctx, *p.config.EtcdDialTimeout)
		_, err = etcdClient.Put(timeout, "/gateway/cluster/name", clusterName)
		cancel()
		if err == nil {
			break
		}
	}
	return err
}

func getTempEtcdClient(ctx context.Context, logger log.Logger, endpoints []string, etcdCfg *embetcd.Config) (tempCli *embetcd.Client, closeCli func(), err error) {
	closeCli = func() {
		if tempCli != nil {
			_ = tempCli.Close()
		}
	}

	for ctx.Err() == nil {
		// close previously existing fn
		if closeCli != nil {
			closeCli()
		}

		// create a temporary cli
		tempCli, err = embetcd.NewClient(etcdcli.Config{
			Endpoints:        endpoints,
			AutoSyncInterval: *etcdCfg.AutoSyncInterval,
			DialTimeout:      *etcdCfg.DialTimeout,
		})

		// if successful return
		if err == nil {
			break
		}
		logger.Log(log.Err, err, "attempt to create temp etcd client failed, retrying")
	}

	return tempCli, closeCli, err
}

func (p *gateway) handleClusterNameResponse(ctx context.Context, tempCli etcdIntf.Client, resp *etcdcli.GetResponse, clusterName string) (err error) {
	// if there is a key
	if resp != nil && len(resp.Kvs) != 0 {
		// the key doesn't match
		if string(resp.Kvs[0].Value) != clusterName {
			err = fmt.Errorf("the configured cluster name '%s' does not match the existing cluster name '%s'", string(resp.Kvs[0].Value), clusterName)
		}
	} else {
		err = p.setClusterName(ctx, tempCli, clusterName)
	}
	return err
}

func (p *gateway) checkForClusterNameConflict(ctx context.Context, logger log.Logger, etcdCfg *embetcd.Config) (err error) {
	var tempCli *embetcd.Client
	var closeCli func()

	// conflicts only occur if we're joining or a client
	if etcdCfg != nil && etcdCfg.ClusterState == "join" || etcdCfg.ClusterState == "client" {

		for ctx.Err() == nil {
			// get the a temporary cli for the cluster
			tempCli, closeCli, err = getTempEtcdClient(ctx, logger, etcdCfg.InitialCluster, etcdCfg)

			var resp *etcdcli.GetResponse
			if err == nil {
				timeout, cancel := context.WithTimeout(ctx, *p.config.EtcdDialTimeout)
				resp, err = tempCli.Get(timeout, "/gateway/cluster/name")
				cancel()
			}

			// handle cluster name stuff
			if err == nil {
				err = p.handleClusterNameResponse(ctx, tempCli, resp, etcdCfg.ClusterName)
			}

			// close the temporary client
			closeCli()

			if err == nil {
				break
			}
		}
	}
	return err
}

// setupEtcdClient sets up the etcd client on the gateway
func (p *gateway) setupEtcdClient(etcdCfg *embetcd.Config) (err error) {
	var endpoints []string
	if etcdCfg.ClusterState == embed.ClusterStateFlagNew {
		endpoints = embetcd.URLSToStringSlice(etcdCfg.ACUrls)
	} else {
		endpoints = etcdCfg.InitialCluster
	}
	// setup the etcd client
	if etcdCfg.ClusterName != "" && len(endpoints) > 0 {
		p.etcdClient, err = embetcd.NewClient(etcdcli.Config{
			Endpoints:        endpoints,
			AutoSyncInterval: *etcdCfg.AutoSyncInterval,
			DialTimeout:      *etcdCfg.DialTimeout,
		})
	}
	return err
}

// setupEtcdServer sets up the etcd server on the gateway
func (p *gateway) setupEtcdServer(ctx context.Context, etcdCfg *embetcd.Config) (err error) {
	// if the cluster op is invalid short circuit and return
	if !(etcdCfg.ClusterState == embed.ClusterStateFlagExisting || etcdCfg.ClusterState == embed.ClusterStateFlagNew || etcdCfg.ClusterState == "") {
		return fmt.Errorf("unsupported cluster-op specified \"%s\"", etcdCfg.ClusterState)
	}

	// instantiate the etcdServer
	if etcdCfg.ClusterState != "" {
		p.etcdServer = embetcd.New()

		// set up the etcd server
		timeout, cancel := context.WithTimeout(ctx, time.Second*120)
		defer cancel()
		err = p.etcdServer.Start(timeout, etcdCfg)
	}

	return err
}

func validateHubConfig(cfg *config.GatewayConfig) (err error) {
	if cfg.HubAddress == nil {
		err = errors.New("a hub address must be specified")
	} else if cfg.AuthToken == nil {
		err = errors.New("an auth token must be configured to connect to the hub")
	} else if cfg.HubClientTimeout == nil {
		err = errors.New("a hub client timeout must be specified")
	}
	return err
}

// setupHub creates a new hub
func (p *gateway) setupHub(cfg *config.GatewayConfig) (err error) {
	if *cfg.Cluster {
		err = validateHubConfig(cfg)
		if err == nil {
			rateLimited := log.NewOnePerSecond(p.logger)
			userAgent := fmt.Sprintf("gateway/%s (gover %s)", Version, runtime.Version())
			p.hub, err = hub.NewHub(rateLimited, *cfg.HubAddress, *cfg.AuthToken, *cfg.HubClientTimeout, userAgent)
			p.logger.Log("setupHub()", err)
		}
	}

	return err
}

// setupEtcd sets up the etcd server and client if applicable and returns errors if there's any problems
func (p *gateway) setupEtcd(ctx context.Context, loadedConfig *config.GatewayConfig) error {
	// short circuit if there is no cluster operation defined because that means we're running in non-cluster mode
	// This IS NOT an error state!
	if loadedConfig.ClusterOperation == nil || *loadedConfig.ClusterOperation == "" {
		return nil
	}

	// get an etcd config struct from our loaded gateway config
	etcdCfg := loadedConfig.ToEtcdConfig()

	// set up a timeout for the etcd server startup
	timeout := ctx
	if etcdCfg.StartupGracePeriod != nil {
		var cancel context.CancelFunc
		timeout, cancel = context.WithTimeout(context.Background(), *etcdCfg.StartupGracePeriod)
		defer cancel()
	}

	var err error

	// check the for cluster name conflicts
	err = p.checkForClusterNameConflict(timeout, p.logger, etcdCfg)

	// start the server
	if *loadedConfig.ClusterOperation != "client" && err == nil {
		err = p.setupEtcdServer(timeout, etcdCfg)

		// once the server starts
		if err == nil && etcdCfg.ClusterState == embed.ClusterStateFlagNew {
			endpoints := embetcd.URLSToStringSlice(etcdCfg.ACUrls)
			var tempCli *embetcd.Client
			var cancel func()
			tempCli, cancel, err = getTempEtcdClient(timeout, p.logger, endpoints, etcdCfg)
			defer cancel()

			// set the cluster name for new clusters
			if err == nil {
				err = p.setClusterName(timeout, tempCli, etcdCfg.ClusterName)
			}

		}
	}

	if err != nil {
		return err
	}

	// create the client
	return log.IfErrWithKeysAndReturn(p.logger, p.setupEtcdClient(etcdCfg), log.Err, "unable to create etcd client")
}

// runningLoop is the where we block in the main gateway routine
func (p *gateway) runningLoop(ctx context.Context) (err error) {
	// getEtcdStopCh returns the gateway's etcd server's stop notify channel or returns a blocking channel if etcd is nil
	getEtcdStopCh := func() <-chan struct{} {
		if p.etcdServer != nil && p.etcdServer.Server != nil {
			return p.etcdServer.Server.StopNotify()
		}
		return make(chan struct{})
	}
	// main loop
	for {
		select {
		case <-ctx.Done():
			return err
		case <-p.signalChan: // shutdown the gateway if the gateway is signaled
			return p.stop(ctx)
		case <-getEtcdStopCh(): // shutdown the gateway if the etcd server goes down
			// TODO: try to relaod the etcd server some # of times if it fails
			//  instead of shutting down the whole gateway.  There is an err chan
			//  on etcd server that we could use to identify if we've errored out
			// signal to the running routine to close the gateway
			p.signalChan <- syscall.SIGTERM
			p.logger.Log(log.Msg, "etcd server has stopped")
		}
	}
}

func (p *gateway) scheduleStatCollection(ctx context.Context, schedulers map[string]*sfxclient.Scheduler, multiplexer signalfx.Sink) func() {
	// Schedule datapoint collection to a Discard sink so we can get the stats in Expvar()
	wg := sync.WaitGroup{}

	// finishedContext and cancelFunc are used
	finishedContext, cancelFunc := context.WithCancel(ctx)

	// schedule the schedulers
	for name, scheduler := range schedulers {
		scheduler.Sink = dpsink.Discard
		if (p.config.InternalMetricsListenerAddress == nil || *p.config.InternalMetricsListenerAddress == "") && (p.config.StatsDelayDuration != nil && *p.config.StatsDelayDuration != 0) {
			// only configure the scheduler sink to emit through the multiplexer
			// when the internal metrics server is off and StatsDelay is greater than 0
			scheduler.ReportingDelayNs = p.config.StatsDelayDuration.Nanoseconds()
			scheduler.Sink = multiplexer
			wg.Add(1)
			go func(scheduler *sfxclient.Scheduler, name string) {
				err := scheduler.Schedule(finishedContext)
				p.logger.Log(log.Err, err, logkey.Struct, name, "Schedule finished")
				wg.Done()
			}(scheduler, name)
		}
	}

	return func() {
		cancelFunc()
		wg.Wait()
	}
}

func (p *gateway) start(ctx context.Context) error {
	if p.config == nil {
		return fmt.Errorf("gateway was not configured properly")
	}

	// setup the metrics scheduler
	metricsScheduler := p.setupScheduler(p.config, gatewayMetricsPrefix)

	// setup debugMetricsScheduler
	debugMetricsScheduler := p.setupScheduler(p.config, "")

	// attach go metrics to debug metric scheduler
	debugMetricsScheduler.AddCallback(sfxclient.GoMetricsSource)

	// report version metric to debug metric scheduler
	p.versionMetric.RepoURL = "https://github.com/signalfx/gateway"
	p.versionMetric.FileName = "/buildInfo.json"
	debugMetricsScheduler.AddCallback(&p.versionMetric)

	// instantiate schedulers
	schedulers := map[string]*sfxclient.Scheduler{"gateway_metrics": metricsScheduler}

	// only create the debug scheduler if EmitDebugMetrics is true
	if p.config.EmitDebugMetrics != nil && *p.config.EmitDebugMetrics {
		schedulers["debug_metrics"] = debugMetricsScheduler
	}

	// setup debug server
	if err := p.setupDebugServer(p.config, p.logger, debugMetricsScheduler, metricsScheduler); err != nil {
		p.logger.Log(log.Err, "debug server failed", err)
		return err
	}

	// handle etcd configurations start server and/or open client if applicable to config
	if err := p.setupEtcd(ctx, p.config); err != nil {
		p.logger.Log(log.Err, "failed to set up the etcd server")
		return err
	}

	// create http chain
	chain := p.createCommonHTTPChain(p.config)
	loader := config.NewLoader(ctx, p.logger, Version, &p.debugContext, &p.debugSink, &p.ctxDims, chain)

	multiplexer, additionalEndpoints, err := p.setupForwardersAndListeners(ctx, loader, p.config, p.logger, debugMetricsScheduler, metricsScheduler)
	if err == nil {
		p.handleEndpoints(additionalEndpoints)

		// schedule the schedulers
		stopSchedulers := p.scheduleStatCollection(ctx, schedulers, multiplexer)

		// setup the internal metrics server
		if err := p.setupInternalMetricsServer(p.config, p.logger, schedulers); err != nil {
			p.logger.Log(log.Err, "internal metrics server failed", err)
			return err
		}

		// wait for setup to complete
		if p.setupDoneSignal != nil {
			close(p.setupDoneSignal)
		}

		// wait for the gateway to shutdown
		p.logger.Log("Setup done.  Blocking!")
		err = p.runningLoop(ctx)

		stopSchedulers()
	}

	return err
}

// loadConfig loads a config file for the Gateway, the returned error
// is used for testing and should be refactored out later
func loadConfig(configFilePath string, logger log.Logger) (*config.GatewayConfig, error) {
	logger.Log(logkey.ConfigFile, configFilePath, "Looking for config file")

	// load the config file
	loadedConfig, err := config.Load(configFilePath, logger)

	// log an error and return if we fail to load the config file
	if err != nil {
		return nil, err
	}

	// add flag values to the loadedConfig.  This overrides any values in the config file with runtime flags.
	flags.addFlagsToConfig(loadedConfig)

	logger.Log("config loaded")
	return loadedConfig, nil
}

// setupGoMaxProcs is a function that takes a pointer to an int and a function(int) int and if the int pointer
// is not nil it feeds the value into the function
func setupGoMaxProcs(loadedConfig *config.GatewayConfig, gomaxprocs func(int) int) int {
	if loadedConfig != nil && loadedConfig.NumProcs != nil {
		return gomaxprocs(*loadedConfig.NumProcs)
	}
	// go does this by default in most modern version of go
	return gomaxprocs(runtime.NumCPU())
}

// writePidFile writes the pid file for the gateway server
func writePidFile(loadedConfig *config.GatewayConfig, logger log.Logger) {
	if loadedConfig != nil {
		pid := os.Getpid()
		if err := ioutil.WriteFile(*loadedConfig.PidFilename, []byte(strconv.FormatInt(int64(pid), 10)), os.FileMode(0644)); err != nil {
			logger.Log(log.Err, err, logkey.Filename, *loadedConfig.PidFilename, "cannot store pid in pid file")
		}
	}
}

// removePidFile removes the pid file for the gateway server
func removePidFile(loadedConfig *config.GatewayConfig, logger log.Logger) {
	if loadedConfig != nil {
		log.IfErr(logger, os.Remove(*loadedConfig.PidFilename))
	}
}

// main function for gateway server
func main() {
	// create a logger before we load config
	// TODO: make logger a package variable so we don't have to pass it around
	logger := log.NewContext(log.DefaultLogger.CreateChild()).With(logkey.Time, log.DefaultTimestamp, logkey.Caller, log.DefaultCaller)

	// parse runtime flags only once
	flagParse()

	// if the version flag is present, print the version and return
	if flags != nil && flags.version {
		fmt.Println(Version)
		fmt.Println(BuildDate)
		flags.version = false
		return
	}

	// instantiate a gateway
	mainInstance := newGateway()

	// send sigterm to the signalChan
	signal.Notify(mainInstance.signalChan, syscall.SIGTERM)

	// when main completes stop sending sigterms to the channel
	defer func() {
		signal.Stop(mainInstance.signalChan)
		close(mainInstance.signalChan)
	}()

	// load the config file using runtime flag
	loadedConfig, err := loadConfig(flags.configFileName, logger)
	log.IfErrWithKeys(logger, err, log.Err, "an error occurred while loading the config file")

	// setup pid file if one is configured
	writePidFile(loadedConfig, logger)

	// use the config value for NumProcs to set the maximum processes for go to schedule against
	logger.Log(log.Msg, "setting go maximum number of processes to ", setupGoMaxProcs(loadedConfig, runtime.GOMAXPROCS))

	// configure the gateway
	log.IfErr(logger, mainInstance.configure(loadedConfig))

	// start the gateway
	log.IfErr(logger, mainInstance.start(context.Background()))
}

// FirstNonNil returns what it says it does
func FirstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
