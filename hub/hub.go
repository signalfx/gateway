package hub

import (
	"context"
	"errors"
	"fmt"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"sync"
	"time"

	"github.com/mailru/easyjson"
	"github.com/signalfx/gateway/hub/httpclient"
	"github.com/signalfx/gateway/hub/hubclient"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/timekeeper"
)

const defaultHeartBeatInterval = 5000
const defaultMinSuccessfulHb = 3

// ErrAlreadyClosed is the error returned when an operation is called on the hub but the connection was already closed
var ErrAlreadyClosed = errors.New("hub client has already closed")

// ErrAlreadyRegistered is returned when the gateway is already registered with the hub
var ErrAlreadyRegistered = errors.New("server is already registered and should be unregistered first")

// ErrAlreadyUnregistered is returned when the gateway is already unregistered from the hub
var ErrAlreadyUnregistered = errors.New("server has already unregistered")

// ErrInvalidClusterName is returned when an invalid cluster name is configured
var ErrInvalidClusterName = errors.New("invalid cluster name")

// ErrInvalidServerName is returned when an invalid server name is configured
var ErrInvalidServerName = errors.New("invalid server name")

// ErrInvalidVersion is returned when an invalid version is reported
var ErrInvalidVersion = errors.New("invalid ")

// waitForErrCh is a helper function to wait for an error on channel with a timeout context
func waitForErrCh(ctx context.Context, respCh chan error) (err error) {
	select {
	case err = <-respCh:
		close(respCh)
	case <-ctx.Done():
		err = ctx.Err()
	}
	return err
}

type datapointReq struct {
	respCh chan []*datapoint.Datapoint
	debug  bool
}

func newDatapointReq(debug bool) *datapointReq {
	return &datapointReq{respCh: make(chan []*datapoint.Datapoint, 1), debug: debug}
}

type registrationReq struct {
	registration *hubclient.Registration
	respCh       chan error
}

func newRegistrationReq(reg *hubclient.Registration) *registrationReq {
	return &registrationReq{registration: reg, respCh: make(chan error, 1)}
}

func drainRegistrationReqChan(ctx context.Context, ch chan *registrationReq) {
	for {
		select {
		case <-ch:
		case <-ctx.Done():
			return
		default:
			return
		}
	}
}

// newRegistration is a helper function to take configurations and build a hubclient registration
func newRegistration(serverName string, clusterName string, version string, payload hubclient.ServerPayload, distributor bool) (*hubclient.Registration, error) {
	var err error
	var registration = &hubclient.Registration{
		Name:        serverName,
		Cluster:     clusterName,
		Version:     version,
		Distributor: distributor,
	}

	if clusterName == "" {
		err = ErrInvalidClusterName
	}
	if serverName == "" {
		err = ErrInvalidServerName
	}
	if version == "" {
		err = ErrInvalidVersion
	}

	registration.Payload, _ = easyjson.Marshal(payload)

	return registration, err
}

// Hub is the structure and corresponding routines that maintain a connection to the gateway hub
type Hub struct {
	// logger
	logger log.Logger

	// running context
	ctx     context.Context
	closeFn context.CancelFunc

	// client is an http client for interacting with the hub
	client httpclient.Client

	// tk is the timekeeper used to manage
	tk timekeeper.TimeKeeper

	// Heartbeat

	// hb is a timer for when to make the next heartbeat request
	hb timekeeper.Timer
	// hbInterval is the number of milliseconds of time to wait between beats.
	hbInterval time.Duration
	// beatCount is a counter of the number of successful heartbeats
	beatCount uint64
	// minSuccessfulHb is the minimum number of heartbeats that must
	minSuccessfulHb uint64

	// Channels

	// registerCh is used to signal registration and re-registration to the hub
	registerCh chan *registrationReq
	// clusterCh is used to push cluster updates to the client routine
	clusterCh chan *hubclient.Cluster
	// configCh is used to push config changes to the client routine
	configCh chan *hubclient.Config
	// datapointCh is used to collect datapoints about the hub connection
	datapointCh chan *datapointReq

	// concurrency features

	// wg wait group for routines
	wg sync.WaitGroup
	// registrationMutex synchronizes hub.Register()/hub.Unregister()
	registrationMutex sync.Mutex
	// registered indicates if a registration has been sent to the requestLoop
	// and should only be accessed with the registrationMutex
	registered bool

	// Registration

	// configurations about the hub
	clusterName  string
	registration *hubclient.Registration

	// Things Returned by the Hub

	// etag
	etag string
	// lease returned by the gateway hub
	lease string
	// config returned by the gateway hub
	config *hubclient.Config
	// servers are a list of servers returned by the hub
	servers []*hubclient.ServerResponse
	// distributors are the list of distributors returned by the hub
	distributors []*hubclient.ServerResponse
}

func (h *Hub) updateState(etag string, config *hubclient.Config, cluster *hubclient.Cluster) {
	// update etag
	if etag != "" {
		h.etag = etag
	}

	// push config to client routine
	if config != nil {
		// extract the heartbeat interval from the config and save the duration
		if config.Heartbeat > 0 {
			h.hbInterval = time.Duration(config.Heartbeat) * time.Millisecond
		}
		h.configCh <- config
	}

	// push cluster to client routine
	if cluster != nil {
		h.clusterCh <- cluster
	}
}

// unregister
func (h *Hub) unregister() error {
	// set registration to nil so we know to pass cluster state on the next successful registration
	h.registration = nil
	return h.client.Unregister(h.lease)
}

// Lifecycle loop functions
func (h *Hub) register(ctx context.Context, reg *hubclient.Registration) error {
	// reset the heartbeat counter
	h.beatCount = 0

	// send reg request to hub
	resp, etag, err := h.client.Register(reg.Cluster, reg.Name, reg.Version, reg.Payload, reg.Distributor)

	// when reg errors out
	if err != nil {
		// TODO gradually spread out reg attempts and don't block heart beat loop
		// wait a second if reg fails
		time.Sleep(time.Second * 1)
		// push the reg back on the channel to retry
		// and don't set the heart beat timer
		h.registerCh <- newRegistrationReq(reg)
		return err
	}

	// save the lease returned by the hub
	h.lease = resp.Lease

	// update the cluster state as long as we are not re-registering
	if h.registration == nil {
		// Only update the config and cluster state if h.reg is nil.  This means the reg is new.
		// If the previous reg is not nil, this means we are re-registering and most likely that is because
		// we either lost connectivity or the hub told us to re-register. In either case we do not want to process
		// the cluster state immediately.  The heartbeat interval will update the cluster state after some number of
		// successful updates.  This gives other cluster members time to rejoin if they were erroneously dropped out of
		// the hub's cluster state.
		h.updateState(etag, resp.Config, resp.Cluster)
	}

	// save the new reg
	h.registration = reg
	h.clusterName = reg.Name

	return nil
}

// heartbeatInner facilitates heart beats or cluster state updates
func (h *Hub) heartbeat() {
	state, etag, err := h.client.Heartbeat(h.lease, h.etag)
	switch err {
	case nil:
		h.beatCount++
		// Only update the state if the beatCount is greater than the minimum successful hb threshold. The reason for
		// this is to mitigate network outages.  During an outage the we hold the last known cluster cluster state.
		// When we finally reconnect we should wait a few heart beats for the hub's cluster state to rebuild before we
		// accept the returned cluster state.  This prevents re-balancing storms on reconnection.
		if h.beatCount >= h.minSuccessfulHb && state != nil {
			h.updateState(etag, state.Config, state.Cluster)
		}
	case httpclient.ErrExpiredLease:
		// our lease has expired so we must re-register
		h.registerCh <- newRegistrationReq(h.registration)
	default:
		// reset the heart beat count on error
		h.beatCount = 0
	}
	return
}

// registerChInner does work on elements received over the registerCh
func (h *Hub) registerChInner(ctx context.Context, req *registrationReq) {
	if req != nil {
		if req.registration == nil {
			// if reg is nil that means we should unregister.
			// draining the reg channel also ensures that any queued re-registraton attempts are dropped
			drainRegistrationReqChan(ctx, h.registerCh)
			req.respCh <- log.IfErrAndReturn(h.logger, h.unregister())
			fmt.Println("unregistered registration is", h.registration)
		} else {
			req.respCh <- log.IfErrAndReturn(h.logger, h.register(ctx, req.registration))
		}
	}
}

func (h *Hub) hbInner() {
	if h.registration != nil {
		h.heartbeat()
	} else if h.clusterName != "" {
		var err error
		var config *hubclient.Config
		var cluster *hubclient.Cluster
		cluster, err = h.client.Cluster(h.clusterName)
		log.IfErr(h.logger, fmt.Errorf("error occurreed while fetching cluster state: %v", err))
		config, err = h.client.Config(h.clusterName)
		log.IfErr(h.logger, fmt.Errorf("error occurred while fetching cluster config: %v", err))
		h.updateState("", config, cluster)
	}
}

// requestLoop is a routine for facilitating requests to the hub and heartbeating
func (h *Hub) requestLoop(ctx context.Context) {
	// start the heartbeat interval
	h.hb = h.tk.NewTimer(h.hbInterval)
	for ctx.Err() == nil {
		select {
		case req := <-h.registerCh:
			fmt.Println("entering registration 1", h.registration)
			h.registerChInner(ctx, req)
			fmt.Println("exiting registration", h.registration)
		case <-h.hb.Chan():
			h.hbInner()
		case <-ctx.Done():
			fmt.Println("returning from request loop")
			return
		}

		// reset the heartbeat interval
		h.hb.Stop()
		h.hb = h.tk.NewTimer(h.hbInterval)

		// TODO: remove this or put behind a debug flag
		//h.logger.Log(log.Key("Config"), h.config)
		//h.logger.Log(log.Key("Servers"), fmt.Sprintf("%v", h.servers))
		//h.logger.Log(log.Key("Distributors"), fmt.Sprintf("%v", h.distributors))
	}
}

func (h *Hub) debugDatapoints() []*datapoint.Datapoint {
	var hbInterval int64
	if h.config != nil {
		hbInterval = h.config.Heartbeat
	}
	dps := []*datapoint.Datapoint{
		sfxclient.Gauge("cluster.heartbeatInterval", map[string]string{}, hbInterval),
	}

	return dps
}

func (h *Hub) datapoints() []*datapoint.Datapoint {
	dps := []*datapoint.Datapoint{
		sfxclient.Gauge("hub.clusterSize", map[string]string{"type": "server"}, int64(len(h.servers))),
		sfxclient.Gauge("hub.clusterSize", map[string]string{"type": "distributor"}, int64(len(h.distributors))),
	}
	return dps
}

func (h *Hub) datapointChInner(req *datapointReq) {
	if req != nil {
		if req.debug {
			req.respCh <- h.debugDatapoints()
		} else {
			req.respCh <- h.datapoints()
		}
	}
}

func drainConfigCh(ctx context.Context, config *hubclient.Config, configCh chan *hubclient.Config) *hubclient.Config {
	for {
		select {
		case config = <-configCh:
		case <-ctx.Done():
			return config
		default:
			return config
		}
	}
}

func drainClusterCh(ctx context.Context, cluster *hubclient.Cluster, clusterCh chan *hubclient.Cluster) *hubclient.Cluster {
	for {
		select {
		case cluster = <-clusterCh:
		case <-ctx.Done():
			return cluster
		default:
			return cluster
		}
	}
}

// clientLoop is a routine that services client requests / watchers
func (h *Hub) clientLoop(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case req := <-h.datapointCh:
			h.datapointChInner(req)
		case cluster := <-h.clusterCh:
			// dump all buffered cluster updates to get the latest cluster state
			cluster = drainClusterCh(ctx, cluster, h.clusterCh)
			h.servers = cluster.Servers
			h.distributors = cluster.Distributors
			// TODO notify watchers
		case config := <-h.configCh:
			// dump all buffered config updates to get the latest value
			config = drainConfigCh(ctx, config, h.configCh)
			h.config = config
			// TODO notify watchers
		// TODO add cases for requests for servers, distributors, and config
		case <-ctx.Done():
			return
		}
	}
}

func waitForDatapoints(ctx context.Context, respCh chan []*datapoint.Datapoint) []*datapoint.Datapoint {
	var resp []*datapoint.Datapoint

	select {
	case <-ctx.Done():
	case resp = <-respCh:
		close(respCh)
	}

	return resp
}

// DebugDatapoints returns a set of debug level datapoints about the hub connection
func (h *Hub) DebugDatapoints() []*datapoint.Datapoint {
	var resp []*datapoint.Datapoint
	req := newDatapointReq(true)
	select {
	case <-h.ctx.Done():
	case h.datapointCh <- req:
		resp = waitForDatapoints(h.ctx, req.respCh)
	}
	return resp
}

// Datapoints returns a set of datapoints about the hub connection
func (h *Hub) Datapoints() []*datapoint.Datapoint {
	var resp []*datapoint.Datapoint
	req := newDatapointReq(false)
	select {
	case <-h.ctx.Done():
	case h.datapointCh <- req:
		resp = waitForDatapoints(h.ctx, req.respCh)
	}
	return resp
}

// Register registers the gateway to the hub and starts the heartbeat
func (h *Hub) Register(serverName string, clusterName string, version string, payload hubclient.ServerPayload, distributor bool) error {
	h.registrationMutex.Lock()
	defer h.registrationMutex.Unlock()

	// check if the hub has been closed
	if h.ctx.Err() != nil {
		return ErrAlreadyClosed
	}

	// check if already registered, and maybe remove this in the future for re-registrations
	if h.registered {
		return ErrAlreadyRegistered
	}

	// validate config
	registration, err := newRegistration(serverName, clusterName, version, payload, distributor)
	if err != nil {
		return err
	}

	// send config for registration or re-registration
	h.registerCh <- newRegistrationReq(registration)

	h.registered = true

	return nil
}

// Unregister unregisters the gateway from the hub and stops heartbeats,
// but client requests will continue to be serviced using the last known state
func (h *Hub) Unregister(ctx context.Context) error {
	h.registrationMutex.Lock()
	defer h.registrationMutex.Unlock()

	// check if the hub has been closed
	if h.ctx.Err() != nil {
		return ErrAlreadyClosed
	}

	// if already unregistered
	if !h.registered {
		return ErrAlreadyUnregistered
	}

	// signal de-registration in the requestLoop by sending a *registrationReq with a nil registration
	req := newRegistrationReq(nil)
	h.registerCh <- req
	err := waitForErrCh(ctx, req.respCh)
	if err == nil {
		h.registered = false
	}
	return err
}

// Close stops all routines pertaining to the hub. You should explicitly call Unregister() first. This is destructive.
// If you need to restart the hub connection, you must create a new instance of Hub.
func (h *Hub) Close() {
	// stop the running routines and wait for them to complete
	h.closeFn()
	h.wg.Wait()
}

// IsOpen indicates whether the hub has been closed or not
func (h *Hub) IsOpen() bool {
	select {
	case <-h.ctx.Done():
	default:
		return true
	}
	return false
}

// newHub returns a new hub but with out starting routines.  This is useful for testing
func newHub(logger log.Logger, hubAddress string, authToken string, timeout time.Duration, userAgent string) (*Hub, error) {
	var h *Hub
	// create the client
	client, err := httpclient.NewClient(hubAddress, authToken, timeout, userAgent)
	tk := timekeeper.RealTime{}
	h = &Hub{
		logger:            logger,
		tk:                tk,
		client:            client,
		registrationMutex: sync.Mutex{},
		hb:                tk.NewTimer(0),
		hbInterval:        defaultHeartBeatInterval,
		minSuccessfulHb:   defaultMinSuccessfulHb,

		// channels
		// registerCh is used to signal registration, re-registration, and de-registration with the hub
		registerCh: make(chan *registrationReq, 10),
		// clusterCh is used to push cluster updates to the client routine
		clusterCh: make(chan *hubclient.Cluster, 10),
		// configCh is used to push config changes to the client routine
		configCh: make(chan *hubclient.Config, 10),
		// datapointCh is used to return datapoints about the hub connection
		datapointCh: make(chan *datapointReq, 100),
	}

	// stop the heart before starting routines.  It will get started by calls to register
	h.hb.Stop()

	// create context for servicing client requests
	h.ctx, h.closeFn = context.WithCancel(context.Background())

	return h, err
}

// startHub is a helper function that starts the routines that support the hub.  This is useful for testing
func startHub(h *Hub) {
	// client loop services client connections (i.e. requests for watchers or on demand server, distributor, and config requests)
	h.wg.Add(1)
	go func() {
		h.clientLoop(h.ctx)
		h.wg.Done()
	}()

	// create loop
	h.wg.Add(1)
	go func() {
		h.requestLoop(h.ctx)
		h.wg.Done()
	}()
}

// GatewayHub is an interface for interacting with the GatewayHub and should allow us to mock the hub in main gateway tests
type GatewayHub interface {
	Register(serverName string, clusterName string, version string, payload hubclient.ServerPayload, distributor bool) error
	Unregister(ctx context.Context) error
	IsOpen() bool
	Close()
}

// NewHub returns a new hub instance with running routines
func NewHub(logger log.Logger, hubAddress string, authToken string, timeout time.Duration, userAgent string) (GatewayHub, error) {
	h, err := newHub(logger, hubAddress, authToken, timeout, userAgent)

	if err != nil {
		h.Close()
	} else {
		// start the routines that back the hub
		startHub(h)
	}

	return h, err
}
