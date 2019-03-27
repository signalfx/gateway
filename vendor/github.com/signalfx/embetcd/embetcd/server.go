package embetcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver/api/v3client"
)

const (
	// EtcdClusterNamespace is the key namespace used for this package's etcd cluster
	EtcdClusterNamespace = "__etcd-cluster__"
	// DefaultUnhealthyTTL is the grace period to wait before removing an unhealthy member
	DefaultUnhealthyTTL = time.Second * 15
	// DefaultCleanUpInterval is the interval at which to poll for the health of the cluster
	DefaultCleanUpInterval = time.Second * 5
	// DefaultStartUpGracePeriod is the graceperiod to wait for new cluster members to startup
	// before they're subject to health checks
	DefaultStartUpGracePeriod = time.Second * 60
	// DefaultShutdownTimeout is the default time to wait for the server to shutdown cleanly
	DefaultShutdownTimeout = time.Minute * 1
	// DefaultDialTimeout is the default etcd dial timeout
	DefaultDialTimeout = time.Second * 5
	// DefaultAutoSyncInterval is the default etcd autosync interval
	DefaultAutoSyncInterval = time.Second * 1
)

// setupClusterNamespace configures the client with the EtcdClusterNamespace prefix
func setupClusterNamespace(client *Client) {
	// this package reserves a key namespace defined by the constant
	client.KV = namespace.NewKV(client.KV, EtcdClusterNamespace)
	client.Watcher = namespace.NewWatcher(client.Watcher, EtcdClusterNamespace)
	client.Lease = namespace.NewLease(client.Lease, EtcdClusterNamespace)
}

// ServerNameConflicts returns true if the server name conflicts or returns false if it doesn't
func ServerNameConflicts(ctx context.Context, client *Client, name string) (conflicts bool, err error) {
	// get the cluster members
	var members *cli.MemberListResponse
	if members, err = client.MemberList(ctx); err == nil && ctx.Err() != context.DeadlineExceeded && members != nil {
		// add members to the initial cluster
		for _, member := range members.Members {
			if member.Name == name {
				conflicts = true
				err = ErrNameConflict
			}
		}
	}
	return conflicts, err
}

// dedupPeerString take a peer string and deduplicates it
func dedupPeerString(peer string) (peers string) {
	// map to keep track of already used substrings
	set := make(map[string]struct{})

	// break on , and check if the substring has already been used
	for _, substr := range strings.Split(peer, ",") {
		cleansubstr := strings.TrimSpace(substr)
		if _, ok := set[cleansubstr]; !ok {
			parts := strings.Split(cleansubstr, "=")
			if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
				if len(set) == 0 {
					// don't use a comma if first element
					peers = cleansubstr
				} else {
					// use comma for everything after the first element
					peers = fmt.Sprintf("%s,%s", peers, cleansubstr)
				}
				set[cleansubstr] = struct{}{}
			}
		}
	}
	return peers
}

// getServerPeers returns the peer urls for the cluster formatted for the initialCluster server configuration.
// The context that is passed in should have a configured timeout.
func getServerPeers(ctx context.Context, c *Client, initialCluster string) (peers string, err error) {
	var members *cli.MemberListResponse

	for ctx.Err() == nil {
		// initialize peers with the supplied initial cluster string
		peers = initialCluster

		// get the list of members
		members, err = c.MemberList(ctx)

		if err == nil {
			// add members to the initial cluster
			for _, member := range members.Members {

				// if there's at least one peer url add it to the initial cluster
				if pURLS := member.GetPeerURLs(); len(pURLS) > 0 {
					// peers should already have this server's address so we can safely append ",%s=%s"
					for _, url := range member.GetPeerURLs() {
						peers = fmt.Sprintf("%s,%s=%s", peers, member.Name, url)
					}
				}

			}
			break
		}
	}
	peers = dedupPeerString(peers)
	return peers, err
}

// clusterNameConflicts returns an error if the cluster name conflicts
func clusterNameConflicts(ctx context.Context, client *Client, clusterName string) (err error) {
	var resp *cli.GetResponse
	for ctx.Err() == nil {
		// verify that the cluster name matches before we add a member to the cluster
		// if you add the member to the cluster and check for the cluster name before you start the server,
		// then you run the risk of breaking quorum and stalling out on the cluster name check
		resp, err = client.Get(ctx, "/name")
		if err == nil {

			if len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) == "" {
				_, _ = client.Put(ctx, "/name", clusterName)
				return nil
			}
			if len(resp.Kvs) != 0 && string(resp.Kvs[0].Value) != clusterName {
				return ErrClusterNameConflict
			}
			break
		}
	}
	return err
}

// addMemberToExistingCluster informs an etcd cluster that a server is about to be added to the cluster.  The cluster
// can premptively reject this addition if it violates quorum
func addMemberToExistingCluster(ctx context.Context, tempcli *Client, serverName string, apURLs []url.URL) (err error) {
	// loop while the context hasn't closed
	var conflict bool
	for ctx.Err() == nil {

		// Ensure that the server name does not already exist in the cluster.
		// We want to ensure uniquely named cluster members.
		// If this member died and is trying to rejoin, we want to retry until
		// the cluster removes it or our parent context expires.
		conflict, err = ServerNameConflicts(ctx, tempcli, serverName)

		if !conflict && err == nil {

			// add the member
			_, err = tempcli.MemberAdd(ctx, URLSToStringSlice(apURLs))

			// break out of loop if we added ourselves cleanly
			if err == nil {
				break
			}
		}

		time.Sleep(time.Second * 1)
	}

	return err
}

// Server manages an etcd embedded server
type Server struct {
	// the currently running etcd server
	*embed.Etcd

	// the config for the current etcd server
	config *Config

	// mutex for managing the server
	mutex sync.RWMutex

	// routineContext is the context for the cluster clean up routine,
	// error watching routine, and member key routine
	routineContext context.Context

	// routineCancel is the context cancel function for stopping cluster
	// clean up routine, error watching routine, and member key routine
	routineCancel context.CancelFunc

	// routineWg is a wait group used to wait for running routines to complete
	routineWg sync.WaitGroup
}

// indicates whether the server has been stopped or not and is not thread safe
func (s *Server) isRunning() bool {
	if s != nil {
		if s.Etcd != nil && s.Etcd.Server != nil {
			select {
			// StopNotify() returns a channel that returns nil when the server is stopped or blocks if it's running
			case <-s.Etcd.Server.StopNotify():
				return false
			default:
				// if StopNotify is blocking then return true
				return true
			}
		}
	}

	return false
}

// IsRunning indicates whether the server has been stopped or not and is thread safe
func (s *Server) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.isRunning()
}

// prepare a new cluster
func (s *Server) prepareForNewCluster(ctx context.Context) (err error) {
	s.config.Config.InitialCluster = s.config.InitialClusterFromName(s.config.Name)
	return err
}

// prepare for an existing cluster
func (s *Server) prepareForExistingCluster(ctx context.Context) (err error) {
	// create a temporary client
	var tempcli *Client

	// get an etcdclient to the cluster using the config file
	for ctx.Err() == nil {
		tempcli, err = s.config.GetClientFromConfig(ctx)
		if err == nil {
			// set up the temp cli for the cluster namespace
			setupClusterNamespace(tempcli)
			break
		}
	}

	// check for conflicting server names
	if err == nil {
		err = clusterNameConflicts(ctx, tempcli, s.config.ClusterName)
	}

	// get the peer address string for joining the cluster
	if err == nil {
		s.config.Config.InitialCluster, err = getServerPeers(ctx, tempcli, s.config.InitialClusterFromName(s.config.Name))
	}

	// announce to the cluster that we're going to add this server
	if err == nil {
		err = addMemberToExistingCluster(ctx, tempcli, s.config.Name, s.config.APUrls)
	}

	return err
}

// prepare will either prepare the server to start a new cluster or join an existing cluster
func (s *Server) prepare(ctx context.Context) (err error) {
	// prepare the server to start
	if s.config.ClusterState == embed.ClusterStateFlagNew {
		err = s.prepareForNewCluster(ctx)
	} else {
		err = s.prepareForExistingCluster(ctx)
	}
	return err
}

// startupValidation validates if the clsuter is running and that the config file makes sense
func (s *Server) startupValidation(cfg *Config) error {
	// return if the server is already running
	if s.isRunning() {
		return ErrAlreadyRunning
	}

	// validate the etcd configuration
	return cfg.Validate()
}

// start starts the etcd server after it has been prepared and config has been validated
// it will retry starting the etcd server until the context is cancelled
func (s *Server) start(ctx context.Context, cfg *Config) (err error) {
	// retry starting the etcd server until it succeeds
	for ctx.Err() == nil {
		// remove the data dir because we require each server to be completely removed
		// from the cluster before we can rejoin
		// TODO: if we ever use snapshotting or want to restore a cluster this will need to be revised
		os.RemoveAll(cfg.Dir)

		// create a context for this server
		s.Etcd, err = embed.StartEtcd(cfg.Config)
		if err == nil {
			break
		}
	}
	return err
}

// Start starts the server with the given config
func (s *Server) Start(ctx context.Context, cfg *Config) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// validate current state and config
	if err = s.startupValidation(cfg); err != nil {
		return err
	}

	// save the config to the server for reference
	s.config = cfg

	// prepare the server to either start as a new cluster or join an existing cluster
	err = s.prepare(ctx)

	// start the server
	if err == nil {
		err = s.start(ctx, cfg)
	}

	// wait for the server to be ready or error out
	if err == nil && s.Etcd != nil {
		err = WaitForStructChOrErrCh(ctx, s.Etcd.Server.ReadyNotify(), s.Etcd.Err())
	}

	// initialize the routines that clean up the cluster
	if err == nil && s.isRunning() {
		err = s.initializeAdditionalServerRoutines(ctx, s.Etcd, cfg)
	}

	// clean up unsuccessful start up
	s.cleanUpStart(err)

	return err
}

// cleanupStart is a dedicated function for cleaning up failed start ups
// This is a dedicated function for test coverage purposes.
func (s *Server) cleanUpStart(err error) {
	if err != nil && s.isRunning() {
		s.Shutdown(context.Background())
	}
}

// waits for the etcd server to stop or return an err and then revokes the member key lease and closes the client
func memberKeyRoutine(ctx context.Context, client *Client, lease *cli.LeaseGrantResponse, stopNotify <-chan struct{}, errCh <-chan error) {
	// wait for the server to stop or for an error to occur
	WaitForStructChOrErrCh(ctx, stopNotify, errCh)

	// revoke the lease when the server stops or gets into an error state
	// swallow the error because if the server is stopped we expect an error to occur on revocation
	client.Revoke(context.Background(), lease.ID)

	// close the client
	client.Close()
}

// errorHandlerRoutine waits for errors to occur and attempts
// to shutdown the cluster with a 30 second timeout
func (s *Server) errorHandlerRoutine(ctx context.Context, stopCh <-chan struct{}, errCh <-chan error) {
	select {
	case <-ctx.Done():
		s.routineWg.Done()
	case <-stopCh:
		s.routineWg.Done()
	case <-errCh:
		// encountered an error
		// shutdown the server if it is running
		timeout, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
		// Shutdown() blocks and waits for the wait group. We need to mark it as done before
		// invoking shutdown since this routine is part of that wait group.
		s.routineWg.Done()
		s.Shutdown(timeout)
		cancel()
	}
}

func (s *Server) cleanCluster(ctx context.Context, members *Members, client *Client, ttl *time.Duration, cleanUpInterval *time.Duration, memberRemoveTimeout *time.Duration, gracePeriod *time.Duration) *Members {
	if s != nil && s.Etcd != nil && s.Server != nil {
		timeout, cancel := context.WithTimeout(ctx, DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval))
		unlock, err := client.Lock(timeout, "remove")
		cancel()
		if err != nil {
			return members
		}

		// get the list of members the server is aware of
		currentMembers := s.Server.Cluster().Members()

		// create a wait group to wait for health status from each member of the cluster
		wg := sync.WaitGroup{}

		// members.Clean will use this map to check for members that may have already moved
		currentMemberIDs := make(map[uint64]struct{}, len(currentMembers))

		// get the cluster member health concurrently
		for _, cmember := range currentMembers {

			// wait to check health until the member is listed as started

			currentMemberIDs[uint64(cmember.ID)] = struct{}{}

			// fetch the health of the member in a separate go routine
			wg.Add(1)
			go func(m *Member) {
				m.Update(client)
				wg.Done()
			}(members.Get(cmember))

		}

		// wait for all members to update their health status
		wg.Wait()

		// clean up the member list
		members.Clean(ctx, ttl, gracePeriod, memberRemoveTimeout, currentMemberIDs)

		// clean up any members that exceed their ttl and ignore context deadline exceeded error
		unlock(ctx)
	}
	return members
}

// clusterCleanupRoutine iteratively checks member health and removes bad members
func (s *Server) clusterCleanupRoutine(ctx context.Context, stopCh <-chan struct{}, ttl *time.Duration, cleanUpInterval *time.Duration, memberRemoveTimeout *time.Duration, gracePeriod *time.Duration, client *Client) {
	// close the client on exit
	defer client.Close()

	// set up ticker
	ticker := time.NewTicker(DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval))
	defer ticker.Stop()

	// members is a map of known members and the times the member was discovered and last seen healthy
	members := NewMembers(client)

	// continuously check the cluster health on the ticker interval
	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			members = s.cleanCluster(ctx, members, client, ttl, cleanUpInterval, memberRemoveTimeout, gracePeriod)
		}
	}
}

// newServerClient returns a new client for the server with the server prefix
func (s *Server) newServerClient() (client *Client, err error) {
	// v3client.New() creates a new v3client that doesn't go out over grpc,
	// but rather goes directly through the server itself.  This should be fast!
	client = &Client{Client: v3client.New(s.Etcd.Server)}

	// this package reserves a key namespace defined by the constant
	setupClusterNamespace(client)

	return client, nil
}

// initializeAdditionalServerRoutines launches routines for managing the etcd server
// including periodic member/server clean up, etcd server errors, and keeping a key alive for this server/member instance.
func (s *Server) initializeAdditionalServerRoutines(ctx context.Context, server *embed.Etcd, cfg *Config) (err error) {
	// set up the memberKeyClient client used to keep the member key alive
	var memberKeyClient *Client

	// s.newServerClient() creates a new etcd client that doesn't go out over the wire via grpc,
	// but rather invokes functions directly on the server itself.  This should be fast.
	memberKeyClient, err = s.newServerClient()

	// create cancelable context to signal for the routines to stop.  Shutdown() will cancel the context
	s.routineContext, s.routineCancel = context.WithCancel(context.Background())

	// store the member name because we've successfully started up
	if err == nil {
		// TODO: log this in the future when a upcoming version of etcd gives us access to the etcd logger
		// put the cluster name will put the configured cluster name
		// this should already have been validated by join if we're joining an existing cluster
		_, _ = memberKeyClient.Put(ctx, "/name", cfg.ClusterName)
	}

	// Use the server client to create a key for this server/member under "__etcd-cluster__/members/<name>" with a keep alive lease
	// this lease will expire when the server goes down indicating to the rest of the cluster that the server actually went down
	// this offers a little more protection for when a member is unhealthy but still sending keep alives
	var lease *cli.LeaseGrantResponse
	lease, _, _ = memberKeyClient.PutWithKeepAlive(ctx, path.Join("", "members", cfg.Name), fmt.Sprintf("%v", s.Server.ID()), 5)

	// Set up the clusterCleanUp client for the cluster clean up routine to use.
	// s.newServerClient() creates a new etcd client that doesn't go out over the wire via grpc,
	// but rather invokes functions directly on the server itself.  This should be fast.
	var clusterCleanUpClient *Client
	clusterCleanUpClient, err = s.newServerClient()

	// actually launch the routines
	if err == nil {
		s.routineWg.Add(3)

		// routine to handle revocation of the lease
		go func() {
			memberKeyRoutine(s.routineContext, memberKeyClient, lease, server.Server.StopNotify(), s.Err())
			s.routineWg.Done()
		}()

		// routine to shutdown the cluster on error
		// The wait group is marked done inside of this method because it has to invoke Shutdown() which is where
		// the wait is contained.
		go s.errorHandlerRoutine(s.routineContext, s.Server.StopNotify(), s.Err())

		// routine to remove unhealthy members from the cluster
		go func() {
			s.clusterCleanupRoutine(s.routineContext, s.Server.StopNotify(), cfg.UnhealthyTTL, cfg.CleanUpInterval, cfg.RemoveMemberTimeout, cfg.StartupGracePeriod, clusterCleanUpClient)
			s.routineWg.Done()
		}()
	}

	return err
}

// waitForShutdown waits for the context to conclude, or the done channel to return
func (s *Server) waitForShutdown(ctx context.Context, done chan struct{}) (err error) {
	select {
	// wait for the context to complete
	case <-ctx.Done():
		if ctx.Err() != nil {
			// we timed out so do a hard stop on the server
			if s != nil && s.Etcd != nil && s.Etcd.Server != nil {
				s.Server.HardStop()
				// invoke close after hard stop to free up what ever port we're bound too
				s.Close()
			}
		}
	case <-done:
	}
	return
}

// removeSelfFromCluster removes this server from it's cluster
func (s *Server) removeSelfFromCluster(ctx context.Context) (err error) {
	members := s.Server.Cluster().Members()

	if len(members) > 1 {
		endpoints := make([]string, 0, len(members))
		for _, member := range members {
			if len(member.ClientURLs) > 0 {
				endpoints = append(endpoints, member.ClientURLs...)
			}
		}

		// use a temporary client to try removing ourselves from the cluster
		var tempcli *Client
		tempcli, err = NewClient(cli.Config{
			Endpoints:        endpoints,
			DialTimeout:      DurationOrDefault(s.config.DialTimeout, DefaultDialTimeout),
			TLS:              &tls.Config{InsecureSkipVerify: true}, // insecure for now
			AutoSyncInterval: DurationOrDefault(s.config.AutoSyncInterval, DefaultAutoSyncInterval),
			Context:          ctx,
		})

		if err == nil {
			setupClusterNamespace(tempcli)
			defer tempcli.Close()
		}

		// loop while the context hasn't closed
		for ctx.Err() == nil {

			// create a child context with its own timeout
			timeout, cancel := context.WithTimeout(ctx, DurationOrDefault(s.config.DialTimeout, DefaultDialTimeout))

			// use the temporary client to try removing ourselves from the cluster
			var unlock func(context.Context) error
			if unlock, err = tempcli.Lock(timeout, s.Server.Cfg.Name); err == nil {
				_, err = tempcli.MemberRemove(ctx, uint64(s.Server.ID()))
				unlock(timeout)
				// mask the member not found err because it could mean a cluster clean up routine cleaned us up already
				if err == nil || err == rpctypes.ErrMemberNotFound {
					err = nil
					cancel()
					break
				}
			}

			// wait for the until timeout to try again
			<-timeout.Done()

			// cancel the timeout context
			cancel()

		}
	}
	return err
}

// Shutdown shuts down the server with a cancelable context
func (s *Server) Shutdown(ctx context.Context) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.isRunning() {
		return ErrAlreadyStopped
	}

	// stop any running routines
	if s.routineCancel != nil {
		s.routineCancel()
	}

	// remove server from cluster
	err = s.removeSelfFromCluster(ctx)

	// try to gracefully close the server

	// done is channel that is used to signal if the following routine is complete
	done := make(chan struct{})

	// kick off a routine to close the etcd server.  This is so projects embedding this server can shutdown disgracefully
	// if the etcd server stalls while shutting down or exceeds the shutdown context
	go func() {
		// close the server and signals routines to stop
		s.Close()

		// wait for the running routines to stop
		s.routineWg.Wait()

		// close the done channel signaling the main routine that the server closed
		close(done)
	}()

	// wait for the preceding routine to signal that it shut down the server or for the shutdown
	// context to expire
	s.waitForShutdown(ctx, done)

	return err
}

// New returns a new etcd Server
func New() *Server {
	return &Server{mutex: sync.RWMutex{}}
}
