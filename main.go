package main

import (
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strconv"

	"context"
	"expvar"
	"fmt"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"encoding/json"
	"github.com/quentin-m/etcd-cloud-operator/pkg/etcd"
	"github.com/signalfx/gateway/config"
	"github.com/signalfx/gateway/dp/dpbuffered"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/gateway/protocol"
	"github.com/signalfx/gateway/protocol/demultiplexer"
	"github.com/signalfx/gateway/protocol/signalfx"
	_ "github.com/signalfx/go-metrics"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/eventcounter"
	"github.com/signalfx/golib/httpdebug"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/reportsha"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/golib/web"
	_ "github.com/signalfx/ondiskencoding"
	_ "github.com/spaolacci/murmur3"
	"gopkg.in/natefinch/lumberjack.v2"
	_ "net/http/pprof"
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

// getCommaSeparatedStringEnvVar returns the given env var key's value split by comma or the default values
func getCommaSeparatedStringEnvVar(envVar string, def []string) []string {
	if val := os.Getenv(envVar); val != "" {
		def = def[:0]
		for _, addr := range strings.Split(strings.Replace(val, " ", "", -1), ",") {
			def = append(def, addr)
		}
	}
	return def
}

// getStringEnvVar returns the given env var key's value or the default value
func getStringEnvVar(envVar string, def string) string {
	if val := os.Getenv(envVar); val != "" {
		return val
	}
	return def
}

// getDurationEnvVar returns the given env var key's value or the default value
func getDurationEnvVar(envVar string, def time.Duration) time.Duration {
	if strVal := os.Getenv(envVar); strVal != "" {
		if dur, err := time.ParseDuration(strVal); err == nil {
			return dur
		}
	}
	return def
}

func isStringInSlice(target string, strs []string) bool {
	for _, addr := range strs {
		if addr == target {
			return true
		}
	}
	return false
}

type proxyFlags struct {
	configFileName string
}

type etcdManager struct {
	etcd.ServerConfig
	logger        log.Logger
	removeTimeout time.Duration
	operation     string
	targetCluster []string
	server        *etcd.Server
	client        *etcd.Client
}

func (mgr *etcdManager) setup(conf *config.ProxyConfig) {
	mgr.LPAddress = getStringEnvVar("SFX_LISTEN_ON_PEER_ADDRESS", *conf.ListenOnPeerAddress)
	mgr.APAddress = getStringEnvVar("SFX_ADVERTISE_PEER_ADDRESS", *conf.AdvertisePeerAddress)
	mgr.LCAddress = getStringEnvVar("SFX_LISTEN_ON_CLIENT_ADDRESS", *conf.ListenOnClientAddress)
	mgr.ACAddress = getStringEnvVar("SFX_ADVERTISE_CLIENT_ADDRESS", *conf.AdvertiseClientAddress)
	mgr.MAddress = getStringEnvVar("SFX_ETCD_METRICS_ADDRESS", *conf.ETCDMetricsAddress)
	mgr.UnhealthyMemberTTL = getDurationEnvVar("SFX_UNHEALTHY_MEMBER_TTL", *conf.UnhealthyMemberTTL)
	mgr.removeTimeout = getDurationEnvVar("SFX_REMOVE_MEMBER_TIMEOUT", *conf.RemoveMemberTimeout)
	mgr.DataDir = getStringEnvVar("SFX_CLUSTER_DATA_DIR", *conf.ClusterDataDir)
	mgr.Name = getStringEnvVar("SFX_SERVER_NAME", *conf.ServerName)
	mgr.ServerConfig.Name = mgr.Name

	// if already set, then a command line flag was provided and takes precedence
	if mgr.operation == "" {
		mgr.operation = getStringEnvVar("SFX_CLUSTER_OPERATION", *conf.ClusterOperation)
	}

	mgr.targetCluster = getCommaSeparatedStringEnvVar("SFX_TARGET_CLUSTER_ADDRESSES", conf.TargetClusterAddresses)
}

func (mgr *etcdManager) start() (err error) {

	// use a default server name if one is not provided
	if mgr.ServerConfig.Name == "" {
		mgr.ServerConfig.Name = fmt.Sprintf("%s", mgr.ServerConfig.ACAddress)
	}

	mgr.server = etcd.NewServer(mgr.ServerConfig)
	switch strings.ToLower(mgr.operation) {
	case "": // this is a valid option and means we shouldn't run etcd
		return

	case "seed":
		mgr.logger.Log(fmt.Sprintf("starting etcd server %s to seed cluster", mgr.ServerConfig.Name))
		if err = mgr.server.Seed(nil); err == nil {
			if !isStringInSlice(mgr.AdvertisedClientAddress(), mgr.targetCluster) {
				mgr.targetCluster = append(mgr.targetCluster, mgr.AdvertisedClientAddress())
			}
			mgr.client, err = etcd.NewClient(mgr.targetCluster, etcd.SecurityConfig{}, true)
		}

	case "join":
		mgr.logger.Log(fmt.Sprintf("joining cluster with etcd server name: %s", mgr.ServerConfig.Name))
		if mgr.client, err = etcd.NewClient(mgr.targetCluster, etcd.SecurityConfig{}, true); err == nil {
			mgr.logger.Log(fmt.Sprintf("joining etcd cluster @ %s", mgr.client.Endpoints()))
			if err = mgr.server.Join(mgr.client); err == nil {
				mgr.logger.Log(fmt.Sprintf("successfully joined cluster at %s", mgr.targetCluster))
			}
		}

	default:
		err = fmt.Errorf("unsupported cluster-op specified \"%s\"", mgr.operation)
	}

	return err
}

func (mgr *etcdManager) getMemberID(ctx context.Context) (uint64, error) {
	var memberID uint64
	// use the client to retrieve this instance's member id
	members, err := mgr.client.MemberList(ctx)
	if members != nil {
		for _, m := range members.Members {
			if m.Name == mgr.Name {
				memberID = m.ID
			}
		}
	}
	return memberID, err
}

func (mgr *etcdManager) removeMember() error {
	var err error
	var memberID uint64
	ctx, cancel := context.WithTimeout(context.Background(), mgr.removeTimeout)
	defer cancel()
	// only remove yourself from the cluster if the server is running
	if mgr.server.IsRunning() {
		if memberID, err = mgr.getMemberID(ctx); err == nil {
			removed := make(chan error, 1)
			go func() {
				defer close(removed)
				removed <- mgr.client.RemoveMember(mgr.Name, memberID)
			}()
			select {
			case err = <-removed:
				cancel()
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				err = ctx.Err()
			}
		}
	}
	return err
}

func (mgr *etcdManager) shutdown(graceful bool) (err error) {
	if mgr.server.IsRunning() {
		// stop the etcd server
		mgr.server.Stop(graceful, false) // graceful shutdown true, snapshot false
	}
	if mgr.client != nil {
		// close the client if applicable
		err = mgr.client.Close()
	}
	return err
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
	etcdMgr             *etcdManager
	versionMetric       reportsha.SHA1Reporter
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
	etcdMgr:    &etcdManager{ServerConfig: etcd.ServerConfig{}, logger: log.DefaultLogger.CreateChild()},
}

func init() {
	flag.StringVar(&mainInstance.flags.configFileName, "configfile", "sf/gateway.conf", "Name of the db proxy configuration file")
	flag.StringVar(&mainInstance.etcdMgr.operation, "cluster-op", "", "operation to perform if running in cluster mode [\"seed\", \"join\", \"\"] this overrides the ClusterOperation set in the config file")
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
		Filename:   path.Join(logDir, "gateway.log"),
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

var errDupeForwarder = errors.New("cannot duplicate forwarder names or types without names")

func setupForwarders(ctx context.Context, tk timekeeper.TimeKeeper, loader *config.Loader, loadedConfig *config.ProxyConfig, logger log.Logger, scheduler *sfxclient.Scheduler, Checker *dpsink.ItemFlagger, cdim *log.CtxDimensions, manager *etcdManager) ([]protocol.Forwarder, error) {
	allForwarders := make([]protocol.Forwarder, 0, len(loadedConfig.ForwardTo))
	nameMap := make(map[string]bool)
	for idx, forwardConfig := range loadedConfig.ForwardTo {
		logCtx := log.NewContext(logger).With(logkey.Protocol, forwardConfig.Type, logkey.Direction, "forwarder")
		forwardConfig.Server = manager.server
		forwardConfig.Client = manager.client
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
			Logger: limitedLogger,
		}
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
		}
		bf := dpbuffered.NewBufferedForwarder(ctx, bconf, endingSink, forwarder.Close, forwarder.StartupFinished, limitedLogger)
		allForwarders = append(allForwarders, bf)

		groupName := fmt.Sprintf("%s_f_%d", name, idx)

		scheduler.AddGroupedCallback(groupName, forwarder)
		scheduler.AddGroupedCallback(groupName, bf)
		scheduler.AddGroupedCallback(groupName, dcount)
		scheduler.GroupedDefaultDimensions(groupName, map[string]string{
			"name":      name,
			"direction": "forwarder",
			"source":    "proxy",
			"host":      *loadedConfig.ServerName,
			"type":      forwardConfig.Type,
		})
	}
	return allForwarders, nil
}

var errDupeListener = errors.New("cannot duplicate listener names or types without names")

func setupListeners(tk timekeeper.TimeKeeper, hostname string, loader *config.Loader, listenFrom []*config.ListenFrom, multiplexer signalfx.Sink, logger log.Logger, scheduler *sfxclient.Scheduler) ([]protocol.Listener, error) {
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
		count := &dpsink.Counter{
			Logger: &log.RateLimitedLogger{
				EventCounter: eventcounter.New(tk.Now(), time.Second),
				Limit:        16,
				Logger:       logCtx,
				Now:          tk.Now,
			},
		}
		endingSink := signalfx.FromChain(multiplexer, signalfx.NextWrap(signalfx.UnifyNextSinkWrap(count)))

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
		return fmt.Sprintf("https://github.com/signalfx/gateway/tree/%s", Version)
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
		p.logger.Log("close listeners")
		for _, l := range p.listeners {
			errs = append(errs, l.Close())
		}
		log.IfErr(p.logger, errors.NewMultiErr(errs))
		p.logger.Log("Graceful shutdown done")
	}()

	p.logger.Log("Waiting for connections to drain")
	startingTimeGood := p.tk.Now()
	for {
		select {
		case <-totalWaitTime:
			totalPipeline := p.Pipeline()
			if totalPipeline > 0 {
				p.logger.Log(logkey.TotalPipeline, totalPipeline, "Connections never drained.  This could be bad ...")
			}
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
	errs := make([]error, 0, len(p.forwarders)+1)
	for _, f := range p.forwarders {
		errs = append(errs, f.Close())
	}
	if p.etcdMgr != nil && p.etcdMgr.server != nil {
		errs = append(errs, p.etcdMgr.removeMember())
		errs = append(errs, p.etcdMgr.shutdown(true)) // shutdown the etcd server and close the client
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

func (p *proxy) setupScheduler(hostname string) *sfxclient.Scheduler {
	scheduler := sfxclient.NewScheduler()
	scheduler.AddCallback(sfxclient.GoMetricsSource)
	scheduler.DefaultDimensions(map[string]string{
		"source": "proxy",
		"host":   hostname,
	})
	return scheduler
}

func (p *proxy) scheduleStatCollection(ctx context.Context, scheduler *sfxclient.Scheduler, loadedConfig *config.ProxyConfig, multiplexer signalfx.Sink) (context.Context, context.CancelFunc) {
	// We still want to schedule stat collection so people can debug the server if they want
	scheduler.Sink = dpsink.Discard
	scheduler.ReportingDelayNs = (time.Second * 30).Nanoseconds()
	finishedContext, cancelFunc := context.WithCancel(ctx)
	if loadedConfig.StatsDelayDuration != nil && *loadedConfig.StatsDelayDuration != 0 {
		scheduler.Sink = multiplexer
		scheduler.ReportingDelayNs = loadedConfig.StatsDelayDuration.Nanoseconds()
	} else {
		p.logger.Log("skipping stat keeping")
	}
	return finishedContext, cancelFunc
}

func (p *proxy) setupForwardersAndListeners(ctx context.Context, loader *config.Loader, loadedConfig *config.ProxyConfig, logger log.Logger, scheduler *sfxclient.Scheduler) (signalfx.Sink, error) {
	var err error
	p.forwarders, err = setupForwarders(ctx, p.tk, loader, loadedConfig, logger, scheduler, &p.debugSink, &p.ctxDims, p.etcdMgr)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to setup forwarders")
		return nil, errors.Annotate(err, "unable to setup forwarders")
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
	scheduler.AddCallback(dmux)

	p.versionMetric.RepoURL = "https://github.com/signalfx/gateway"
	p.versionMetric.FileName = "/buildInfo.json"
	scheduler.AddCallback(&p.versionMetric)

	multiplexer := signalfx.FromChain(dmux, signalfx.NextWrap(signalfx.UnifyNextSinkWrap(&p.debugSink)))

	p.listeners, err = setupListeners(p.tk, *loadedConfig.ServerName, loader, loadedConfig.ListenFrom, multiplexer, logger, scheduler)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to setup listeners")
		return nil, errors.Annotate(err, "cannot setup listeners from configuration")
	}

	var errs []error
	for _, f := range p.forwarders {
		err = f.StartupFinished()
		errs = append(errs, err)
		log.IfErr(logger, err)
	}
	return multiplexer, FirstNonNil(errs...)
}

func (p *proxy) run(ctx context.Context) error {
	p.debugSink.CtxFlagCheck = &p.debugContext
	p.logger.Log(logkey.ConfigFile, p.flags.configFileName, "Looking for config file")
	p.logger.Log(logkey.Env, strings.Join(os.Environ(), "-"), "Looking for config file")

	loadedConfig, err := config.Load(p.flags.configFileName, p.logger)
	if err != nil {
		p.logger.Log(log.Err, err, "Unable to load config")
		return err
	}

	p.setup(loadedConfig)
	p.versionMetric.Logger = p.logger

	logger := p.logger
	scheduler := p.setupScheduler(*loadedConfig.ServerName)

	if err := p.setupDebugServer(loadedConfig, logger, scheduler); err != nil {
		p.logger.Log(log.Err, "debug server failed", err)
		return err
	}

	p.etcdMgr.setup(loadedConfig)

	if err := p.etcdMgr.start(); err != nil {
		p.logger.Log(log.Err, "unable to start etcd server", err)
		return err
	}

	var bb []byte
	if bb, err = json.Marshal(loadedConfig); err == nil {
		logger.Log(logkey.Config, string(bb), logkey.Env, strings.Join(os.Environ(), "-"), "config loaded")
	}

	setupGoMaxProcs(loadedConfig.NumProcs, p.gomaxprocs)

	chain := p.createCommonHTTPChain(loadedConfig)
	loader := config.NewLoader(ctx, logger, Version, &p.debugContext, &p.debugSink, &p.ctxDims, chain)

	multiplexer, err := p.setupForwardersAndListeners(ctx, loader, loadedConfig, logger, scheduler)
	if err == nil {
		finishedContext, cancelFunc := p.scheduleStatCollection(ctx, scheduler, loadedConfig, multiplexer)

		// Schedule datapoint collection to a Discard sink so we can get the stats in Expvar()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			err := scheduler.Schedule(finishedContext)
			logger.Log(log.Err, err, logkey.Struct, "scheduler", "Schedule finished")
			wg.Done()
		}()
		if p.setupDoneSignal != nil {
			close(p.setupDoneSignal)
		}

		logger.Log("Setup done.  Blocking!")
		select {
		case <-ctx.Done():
		case <-p.signalChan:
			err = p.gracefulShutdown()
		}
		cancelFunc()
		wg.Wait()
	}
	return err
}

var flagParse = flag.Parse

func main() {
	flagParse()
	signal.Notify(mainInstance.signalChan, syscall.SIGTERM)
	log.IfErr(log.DefaultLogger, mainInstance.main(context.Background()))
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
