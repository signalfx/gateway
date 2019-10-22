package hub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mailru/easyjson"
	"github.com/signalfx/gateway/hub/httpclient"
	"github.com/signalfx/gateway/hub/hubclient"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/timekeeper"
)

const defaultHeartBeatInterval = 10

// ErrAlreadyClosed is the error returned when an operation is called on the hub but the connection was already closed
var ErrAlreadyClosed = errors.New("hub client has already closed")

// waitForErrCh is a helper function to wait for an error on channel with a timeout context
func waitForErrCh(ctx context.Context, respCh chan error) (err error) {
	select {
	case err = <-respCh:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return err
}

// signalableErrCh is a channel that accepts a (chan error). It is intended to signal operations in a routine
// and wait for response error. It has a method attached called signalAndWaitForError to facilitate this.
type signalableErrCh chan chan error

// signalAndWaitForError is a method attached to signalableErrCh that will signal the channel and wait for a returned error
func (e signalableErrCh) signalAndWaitForError(ctx context.Context) (err error) {
	// resp channel
	respCh := make(chan error, 1)

	// register and wait for response
	select {
	case e <- respCh:
		// send the respCh onto the signalableErrCh and wait for the response
		err = waitForErrCh(ctx, respCh)
		if err == nil {
			close(respCh)
		}
	case <-ctx.Done():
		// if the context completes prematurely return context's error
		err = ctx.Err()
	}
	return err
}

// ErrInvalidClusterName is returned when an invalid cluster name is configured
var ErrInvalidClusterName = errors.New("invalid cluster name")

// ErrInvalidServerName is returned when an invalid server name is configured
var ErrInvalidServerName = errors.New("invalid server name")

// ErrInvalidVersion is returned when an invalid version is reported
var ErrInvalidVersion = errors.New("invalid ")

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

	// configurations about the hub
	registration *hubclient.Registration

	// tk is the timekeeper used to manage
	tk timekeeper.TimeKeeper

	// heartbeat is a timer for when to make the next heart beat request
	heartbeat timekeeper.Timer

	// heartbeatCount is a counter of the number of successful heartbeats
	heartbeatCount uint64

	// Channels

	// registerCh is used to signal registration and re-registration to the hub
	registerCh chan *hubclient.Registration
	// unregisterCh is used to signal deregistration from the hub
	unregisterCh signalableErrCh
	// clusterCh is used to push cluster updates to the client routine
	clusterCh chan *hubclient.Cluster
	// configCh is used to push config changes to the client routine
	configCh chan *hubclient.Config

	// concurrency features

	// wg wait group for routines
	wg sync.WaitGroup
	// heartbeatMutex synchronizes Register/Unregister commands
	heartbeatMutex sync.Mutex
	// registered indicates if a registration has been sent to the requestLoop
	// and should only be accessed with the heartbeatMutex
	registered bool

	// Things Returned by the Hub

	// lease returned by the gateway hub
	lease string
	// etag
	etag string
	// distributors are the list of distributors returned by the hub
	distributors []*hubclient.ServerResponse
	// servers are a list of servers returned by the hub
	servers []*hubclient.ServerResponse
	// config returned by the gateway hub
	config *hubclient.Config
	// heartbeatInterval is the number of seconds to wait between heartbeats
	heartbeatInterval int64
}

func (h *Hub) handleRegistrationResponse(resp hubclient.RegistrationResponse) {

	// push config to client routine
	if resp.Config != nil {
		// TODO pass the config to the client loop
		h.configCh <- resp.Config
	}

	// push cluster to client routine
	if resp.Cluster != nil {
		h.clusterCh <- resp.Cluster
	}

}

// Lifecycle loop functions
func (h *Hub) register(registration *hubclient.Registration) error {
	// reset the heartbeat counter
	h.heartbeatCount = 0

	// send registration request to hub
	var resp hubclient.RegistrationResponse
	var err error
	resp, h.etag, err = h.client.Register(registration.Cluster, registration.Name, registration.Version, registration.Payload, registration.Distributor)

	// when registration errors out
	if err != nil {
		// wait a second if registration fails
		time.Sleep(time.Second * 1)
		// push the registration back on the channel to retry
		// and don't set the heart beat timer
		h.registerCh <- registration
		return err
	}

	// extract the heartbeat interval from the config
	if resp.Config != nil && resp.Config.Heartbeat > 0 {
		atomic.StoreInt64(&h.heartbeatInterval, resp.Config.Heartbeat)
	}

	if h.registration == nil {
		// If the previous registration is not nil, this means that we either lost connectivity or the hub told us to
		// reregister.  In this case we do not want to process the cluster state immediately.  We will reset the
		// heartbeats counter and after some threshold we will update the cluster state
		h.handleRegistrationResponse(resp)
	}

	// save the new registration
	h.registration = registration

	// reset heartbeat timer
	h.heartbeat = h.tk.NewTimer(time.Duration(atomic.LoadInt64(&h.heartbeatInterval)) * time.Second)
	return nil
}

// unregister
func (h *Hub) unregister() error {
	// set registration to nil so we know to pass cluster state on the next successful registration
	h.registration = nil
	// unregister
	return h.client.Unregister(h.lease)
}

// requestLoop is a routine for facilitating requests to the hub and heartbeating
func (h *Hub) requestLoop(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case registration := <-h.registerCh:
			log.IfErr(h.logger, h.register(registration))
		//TODO case <- h.heartbeat.Chan():
		case resp := <-h.unregisterCh:
			resp <- h.unregister()
		case <-ctx.Done():
			return
		}
	}
}

// clientLoop is a routine that services client requests / watchers
func (h *Hub) clientLoop(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case cluster := <-h.clusterCh:
			h.servers = cluster.Servers
			h.distributors = cluster.Distributors
			// TODO notify watchers
		case config := <-h.configCh:
			h.config = config
			// TODO notify watchers
			// TODO add cases for requests for servers, distributors, and config
		case <-ctx.Done():
			return
		}
	}
}

// ErrAlreadyRegistered is returned when the gateway is already registered with the hub
var ErrAlreadyRegistered = errors.New("server is already registered and should be unregistered first")

// Register registers the gateway to the hub and starts the heartbeat
func (h *Hub) Register(serverName string, clusterName string, version string, payload hubclient.ServerPayload, distributor bool) error {
	h.heartbeatMutex.Lock()
	defer h.heartbeatMutex.Unlock()

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
	h.registerCh <- registration

	h.registered = true

	return nil
}

// ErrAlreadyUnregistered is returned when the gateway is already unregistered from the hub
var ErrAlreadyUnregistered = errors.New("server has already unregistered")

// Unregister unregisters the gateway from the hub and stops heartbeats,
// but client requests will continue to be serviced using the last known state
func (h *Hub) Unregister(ctx context.Context) error {
	h.heartbeatMutex.Lock()
	defer h.heartbeatMutex.Unlock()

	// check if the hub has been closed
	if h.ctx.Err() != nil {
		return ErrAlreadyClosed
	}

	// if already unregistered
	if !h.registered {
		return ErrAlreadyUnregistered
	}

	// signal unregistration in the requestLoop
	err := h.unregisterCh.signalAndWaitForError(ctx)
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

// newHub returns a new hub but with out starting routines.  This is useful for testing
func newHub(logger log.Logger, hubAddress string, authToken string, timeout time.Duration) (*Hub, error) {
	var h *Hub
	// create the client
	// TODO validate that the client works with the authToken by requesting Clusters
	//  maybe do that in the NewClient() call?
	client, err := httpclient.NewClient(hubAddress, authToken, timeout)

	if err == nil {
		h = &Hub{
			logger:            logger,
			tk:                timekeeper.RealTime{},
			client:            client,
			heartbeatMutex:    sync.Mutex{},
			heartbeatInterval: defaultHeartBeatInterval,
			// channels
			// registerCh is used to signal registration and re-registration to the hub
			registerCh: make(chan *hubclient.Registration, 10),
			// unregisterCh is used to signal deregistration from the hub
			unregisterCh: signalableErrCh(make(chan chan error, 10)),
			// clusterCh is used to push cluster updates to the client routine
			clusterCh: make(chan *hubclient.Cluster, 10),
			// configCh is used to push config changes to the client routine
			configCh: make(chan *hubclient.Config, 10),
		}

		// create context for servicing client requests
		h.ctx, h.closeFn = context.WithCancel(context.Background())
	}
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

// NewHub returns a new hub instance with running routines
func NewHub(logger log.Logger, hubAddress string, authToken string, timeout time.Duration) (*Hub, error) {
	h, err := newHub(logger, hubAddress, authToken, timeout)

	// start the routines that back the hub
	if err == nil {
		startHub(h)
	}

	return h, err
}
