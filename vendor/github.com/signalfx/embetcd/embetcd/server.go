package embetcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/pkg/types"
)

const (
	// EtcdClusterNamespace is the key namespace used for this package's etcd cluster
	EtcdClusterNamespace = "__etcd-cluster__/"
	// DefaultUnhealthyTTL is the grace period to wait before removing an unhealthy member
	DefaultUnhealthyTTL = time.Second * 15
	// DefaultCleanUpInterval is the interval at which to poll for the health of the cluster
	DefaultCleanUpInterval = time.Second * 10
	// DefaultStartUpGracePeriod is the graceperiod to wait for new cluster members to startup
	// before they're subject to health checks
	DefaultStartUpGracePeriod = time.Second * 60
	// DefaultShutdownTimeout is the default time to wait for the server to shutdown cleanly
	DefaultShutdownTimeout = time.Second * 60
	// DefaultDialTimeout is the default etcd dial timeout
	DefaultDialTimeout = time.Second * 5
	// DefaultAutoSyncInterval is the default etcd autosync interval
	DefaultAutoSyncInterval = time.Second * 5
)

// errNilOrNotServerStopped checks whether the error is nil or is not equal to the etcdserver.ErrStopped error
func errNilOrNotServerStopped(err error) bool {
	// note that etcd errors sometimes get mutated somewhere so we can't directly compare errors
	return err == nil || err.Error() != etcdserver.ErrStopped.Error()
}

// printIfErr prints an error to the console
func printIfErr(msg string, err error) bool {
	// TODO: figure out if we can get access to the etcd logger
	if err != nil {
		fmt.Println(msg, " err: ", err)
		// this is super hacky but return a bool for testing validation
		return true
	}
	return false
}

// getRemainingTime calculates the time difference between the elapsed time and the interval up to 0 and returns the value
func getRemainingTime(start time.Time, interval time.Duration) (remainingTime time.Duration) {
	// if there is remaining time in the configured clean up interval reset the timer with that time
	remainingTime = interval - time.Since(start)

	// if we're running behind, then return 0
	if remainingTime < 0 {
		remainingTime = 0
	}

	return remainingTime
}

// shutdownServerIfErr is a helper function to shutdown a server if there is a non nil error
func shutdownServerIfErr(s *Server, err error) bool {
	if err != nil {
		if s != nil {
			timeout, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
			defer cancel()
			s.Shutdown(timeout)
			return true
		}
	}
	return false
}

// waitForCtxErrOrServerStop waits for errors to occur, the context to close, or the stopCh to return
func waitForCtxErrOrServerStop(ctx context.Context, stopCh <-chan struct{}, errCh <-chan error) error {
	select {
	case <-ctx.Done():
		return nil
	case <-stopCh:
		return nil
	case err := <-errCh:
		return err
	}
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

// setClusterName sets the cluster name
func (s *Server) setClusterName(ctx context.Context, clusterName string) (err error) {
	var tempcli *Client
	defer CloseClient(tempcli)

	for ctx.Err() == nil && errNilOrNotServerStopped(err) {
		CloseClient(tempcli)

		tempcli = s.newServerClient()
		_, err = tempcli.Put(ctx, "name", clusterName)

		// break if cluster name set successfully
		if err == nil {
			break
		}
	}

	return err
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

func getClusterClientWithServerNamespace(ctx context.Context, cfg *Config) (*Client, error) {
	var tempcli *Client
	var err error
	defer CloseClient(tempcli)

	// get an etcdclient to the cluster using the config file
	for ctx.Err() == nil {
		// close the temporary client if it was created in a previous iteration of the loop
		CloseClient(tempcli)

		// create the client
		tempcli, err = cfg.GetClientFromConfig(ctx)
		if err == nil {
			// set up the temp cli for the cluster namespace
			setupClusterNamespace(tempcli)
			break
		}
	}
	return tempcli, err
}

func (s *Server) startAsJoiner(ctx context.Context, cfg *Config, tempcli *Client) (err error) {
	var timeout context.Context
	var cancel context.CancelFunc
	defer CancelContext(cancel)

	// continually try to prepare and start the server
	for ctx.Err() == nil {
		CloseServer(s)

		// remove old data directory
		os.RemoveAll(cfg.Dir)

		// Ensure that the server name does not already exist in the cluster.
		// We want to ensure uniquely named cluster members.
		// If this member died and is trying to rejoin, we want to retry until
		// the cluster removes it or our parent context expires.
		timeout, cancel = context.WithTimeout(ctx, DurationOrDefault(cfg.DialTimeout, DefaultDialTimeout))
		_, err = tempcli.serverNameConflicts(timeout, cfg.Name)
		cancel()

		// check for conflicts, get peer urls, and announce that we're joining the cluster
		if err == nil {
			serverName := cfg.Name
			s.config.Config.InitialCluster, err = tempcli.getServerPeers(ctx, cfg.InitialClusterFromName(cfg.Name), &serverName, cfg.APUrls, cfg.DialTimeout)
		}

		// Announce only once to the cluster that we're going to add this server.
		// this offers some protection for errors while joining an existing one node cluster.
		// If we announce this new node and then need to denounce it, we won't be able to because of quorum violations
		// the cluster is configured to 2 members, but only 1 is started.  Removing 1 member from a 2 node cluster
		// breaks quorum.
		if err == nil {
			timeout, cancel = context.WithTimeout(ctx, DurationOrDefault(cfg.DialTimeout, DefaultDialTimeout))
			var isIn bool
			isIn, err = tempcli.arePeerURLSInCluster(timeout, cfg.APUrls)
			cancel()
			if err == nil && !isIn {
				timeout, cancel = context.WithTimeout(ctx, DurationOrDefault(cfg.DialTimeout, DefaultDialTimeout))
				_, err = tempcli.MemberAdd(timeout, URLSToStringSlice(cfg.APUrls))
				cancel()
			}
		}

		// start the server
		if err == nil {
			s.Etcd, err = embed.StartEtcd(cfg.Config)
		}

		// wait for the server to be ready or error out
		if err == nil && s.Etcd != nil {
			err = WaitForStructChOrErrCh(ctx, s.Etcd.Server.ReadyNotify(), s.Etcd.Err())
		}

		// break the loop if successful
		if err == nil {
			break
		}
	}
	return err
}

// starts the etcd server and joins an existing cluster
func (s *Server) join(ctx context.Context, cfg *Config) (err error) {
	// create a temporary client
	var tempcli *Client
	defer CloseClient(tempcli)

	tempcli, err = getClusterClientWithServerNamespace(ctx, cfg)

	if err == nil {
		// check for conflicting cluster names
		var clusterName string
		if clusterName, err = tempcli.clusterName(ctx); err == nil && (clusterName != "" && clusterName != s.config.ClusterName) {
			err = ErrClusterNameConflict
			return
		}
	}

	// start as a joiner
	if err == nil {
		err = s.startAsJoiner(ctx, cfg, tempcli)
	}

	return err
}

// starts the etcd server as a seed node
func (s *Server) seed(ctx context.Context, cfg *Config) (err error) {
	for ctx.Err() == nil {
		CloseServer(s)

		// remove old data directory
		os.RemoveAll(cfg.Dir)

		// set the initial cluster string
		s.config.Config.InitialCluster = s.config.InitialClusterFromName(s.config.Name)

		// start the server
		if err == nil {
			s.Etcd, err = embed.StartEtcd(cfg.Config)
		}

		// wait for the server to be ready or error out
		if err == nil && s.Etcd != nil {
			err = WaitForStructChOrErrCh(ctx, s.Etcd.Server.ReadyNotify(), s.Etcd.Err())
		}

		// break the loop if successful
		if err == nil {
			break
		}
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

	// start the etcd server as either a seeder or a joiner
	if s.config.ClusterState == embed.ClusterStateFlagNew {
		err = s.seed(ctx, cfg)
	} else {
		err = s.join(ctx, cfg)
	}

	// set the cluster name now that the cluster has started without error
	if err == nil && s.isRunning() {
		s.setClusterName(ctx, cfg.ClusterName)
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
		s.shutdown(context.Background())
	}
}

// shouldKeepLeaseAlive waits for a series of stop conditions or for the member key's lease to expire
func shouldKeepLeaseAlive(ctx context.Context, watchCh cli.WatchChan, stopNotify <-chan struct{}, errCh <-chan error) bool {
	for {
		select {
		case watchMsg := <-watchCh:
			// for now there are only 2 types of events PUT and DELETE...
			// We don't care about PUTs because that's us putting the key in
			// We only want to return true if the key is deleted, signaling that we need to put the key back
			if watchMsg.Events == nil || watchMsg.Events != nil && len(watchMsg.Events) > 0 && watchMsg.Events[0].Type == mvccpb.DELETE {
				return true
			}
		case <-ctx.Done():
			return false
		case <-stopNotify:
			return false
		case <-errCh:
			return false
		}
	}

}

// waits for the etcd server to stop or return an err and then revokes the member key lease and closes the client
func (s *Server) memberKeyRoutine(ctx context.Context, client *Client, stopNotify <-chan struct{}, errCh <-chan error) {
	// Use the server client to create a key for this server/member under "__etcd-cluster__/members/<name>" with a keep alive lease
	// this lease will expire when the server goes down indicating to the rest of the cluster that the server actually went down
	// this offers a little more protection for when a member is unhealthy but still sending keep alives
	var lease *cli.LeaseGrantResponse
	var err error
	var cancelKeepAliveCtx context.CancelFunc
	var watchCtx context.Context
	var cancelWatchCtx context.CancelFunc

	// ttl for keeping the member lease alive
	ttl := DurationOrDefault(s.config.DialTimeout, DefaultDialTimeout)

	// watch for changes to the member key (like it randomly dropping out)
	watchCtx, cancelWatchCtx = context.WithCancel(ctx)
	watchCh := client.Watch(watchCtx, path.Join("members", s.config.Name))

	// continually keep the key in the cluster alive
	for ctx.Err() == nil && errNilOrNotServerStopped(err) {
		// clean up previous context
		CancelContext(cancelKeepAliveCtx)
		// revoke previous lease
		RevokeLease(ctx, client, lease)

		// create the client key
		lease, _, cancelKeepAliveCtx, err = client.PutWithKeepAlive(ctx, path.Join("members", s.config.Name), s.Server.ID().String(), int64(ttl.Seconds()))

		// if the lease shouldn't be kept alive break
		if err == nil {
			if !shouldKeepLeaseAlive(watchCtx, watchCh, stopNotify, errCh) {
				break
			}
		}
	}

	// explicitly cancel context because for some reason go vet doesn't honor them when deferred
	CancelContext(cancelKeepAliveCtx)
	CancelContext(cancelWatchCtx)
	RevokeLease(ctx, client, lease)

}

// updateCacheWithCurrentCluster will go through and insert current cluster members into the cache
// and update the client urls of existing cache members
func updateCacheWithCurrentCluster(cache map[types.ID]*memberHealth, current []*membership.Member) map[types.ID]struct{} {
	currentMemberMap := make(map[types.ID]struct{}, len(current))

	for _, member := range current {
		// add id to hash set so we can remove stuff later
		currentMemberMap[member.ID] = struct{}{}

		if member != nil {

			var healthStats *memberHealth
			var ok bool

			// retrieve the member from the cache or create a new health struct
			healthStats, ok = cache[member.ID]

			// create new memberHealth struct if one doesn't already exist in the cache
			if !ok {
				healthStats = &memberHealth{
					Discovered:  time.Now(),
					LastHealthy: time.Now(),
					ClientURLs:  []string{},
				}
			}

			healthStats.Name = member.Name

			// update client urls on the memberHealthStruct if they're not nil
			if member.ClientURLs != nil {
				healthStats.ClientURLs = member.ClientURLs[:]
			}

			// save health stats back to the cache
			cache[member.ID] = healthStats
		}
	}

	return currentMemberMap
}

// removeCacheMembersThatAreNotCurrent will remove members from the cache that aren't in the current cluster
func removeCacheMembersThatAreNotCurrent(cache map[types.ID]*memberHealth, currentMemberMap map[types.ID]struct{}) {
	// remove members from the cache that are not current
	for id := range cache {
		if _, ok := currentMemberMap[id]; !ok {
			delete(cache, id)
		}
	}
}

// removeUnhealthyMembers removes unhealthy members from the cluster
func (s *Server) removeUnhealthyMembers(ctx context.Context, cache map[types.ID]*memberHealth, graceperiod *time.Duration, ttl *time.Duration) {
	for id, m := range cache {
		if time.Since(m.Discovered) > DurationOrDefault(graceperiod, DefaultStartUpGracePeriod) && time.Since(m.LastHealthy) > DurationOrDefault(ttl, DefaultUnhealthyTTL) {
			// don't remove yourself
			if s != nil && s.Server != nil && !s.Server.IsIDRemoved(uint64(s.Server.ID())) && s.Server.ID() != id {
				// close member's client because we won't be using it any more
				CloseClient(m.Client)

				// mark the member unreachable
				s.Server.ReportUnreachable(uint64(id))

				// remove the member from the cluster
				// we'll clear it out out it's health stat on the next clean up interval
				_, err := s.Server.RemoveMember(ctx, uint64(id))

				// check for errors and print them if we encountered an unexpected error
				if err != membership.ErrIDNotFound && err != membership.ErrIDRemoved {
					// TODO: add a real logger here
					printIfErr(fmt.Sprintf("embetcd: an error was encountered while removing member: %s", m.Name), err)
				}

				// only remove one thing at a time ... this should help prevent catastrophes
				break
			}
		}
	}
}

// cleanCluster is a series of steps to get the current cluster members, check their health, and remove unhealthy members
func (s *Server) cleanCluster(ctx context.Context, cache map[types.ID]*memberHealth, client *Client, ttl *time.Duration, cleanUpInterval *time.Duration, gracePeriod *time.Duration) {
	if s != nil && s.Etcd != nil && s.Server != nil {
		// get the list of members the server is aware of
		currentMemberMap := updateCacheWithCurrentCluster(cache, s.Server.Cluster().Members())

		// clean cache up if there are entries that the server is no longer aware of they were likely removed
		removeCacheMembersThatAreNotCurrent(cache, currentMemberMap)

		// timeout for updating cluster health stats
		timeout, cancel := context.WithTimeout(ctx, DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval))
		defer cancel()

		// fetch health status of each entry in the cache
		updateClusterHealthStats(timeout, client, cache)

		// give ourselves extra time to clean up members if necessary
		removeTimeout, removeCancel := context.WithTimeout(ctx, DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval))
		defer removeCancel()

		// remove find and remove unhealthy members based on cache
		s.removeUnhealthyMembers(removeTimeout, cache, gracePeriod, ttl)
	}
}

// clusterCleanupRoutine iteratively runs cleanCluster() to clean up the members in the cluster
func (s *Server) clusterCleanupRoutine(ctx context.Context, stopCh <-chan struct{}, ttl *time.Duration, cleanUpInterval *time.Duration, gracePeriod *time.Duration, client *Client) {
	// sleep some random amount of time to spread out member clean up
	time.Sleep(time.Duration(rand.Int63n(int64(DefaultCleanUpInterval.Seconds()))) * time.Second)

	// set up timer for the cluster clean up interval
	timer := time.NewTimer(DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval))
	defer timer.Stop()

	// cache is a map of known members and the times the member was discovered and last seen healthy
	cache := make(map[types.ID]*memberHealth)

	// cleanUpStartTime tracks the time we start cleaning
	var cleanUpStartTime time.Time

	// continuously check the cluster health on the ticker interval
	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-timer.C:
			// set the time we started cleaning up
			cleanUpStartTime = time.Now()

			// clean up the cluster
			s.cleanCluster(ctx, cache, client, ttl, cleanUpInterval, gracePeriod)

			// reset the timer
			timer.Reset(getRemainingTime(cleanUpStartTime, DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval)))
		}
	}
}

// newServerClient returns a new client for the server with the server prefix
// this function is not thread safe and must be called on the same routine as the server
// the returned clients are threadsafe
func (s *Server) newServerClient() (client *Client) {
	// v3client.New() creates a new v3client that doesn't go out over grpc,
	// but rather goes directly through the server itself.  This should be fast!
	client = &Client{Client: v3client.New(s.Etcd.Server)}

	// this package reserves a key namespace defined by the constant
	setupClusterNamespace(client)

	return client
}

// initializeAdditionalServerRoutines launches routines for managing the etcd server
// including periodic member/server clean up, etcd server errors, and keeping a key alive for this server/member instance.
func (s *Server) initializeAdditionalServerRoutines(ctx context.Context, server *embed.Etcd, cfg *Config) (err error) {
	// create cancelable context to signal for the routines to stop.  Shutdown() will cancel the context
	s.routineContext, s.routineCancel = context.WithCancel(context.Background())

	// actually launch the routines
	if s != nil && s.Server != nil {
		s.routineWg.Add(3)

		// routine to handle keeping the member key alive in the cluster
		go func(client *Client) {
			defer s.routineWg.Done()
			defer CloseClient(client)

			s.memberKeyRoutine(s.routineContext, client, s.Server.StopNotify(), s.Err())
		}(s.newServerClient())

		// routine to watch for errors from etcd and shutdown
		go func() {
			err := waitForCtxErrOrServerStop(s.routineContext, s.Server.StopNotify(), s.Err())
			printIfErr("embetcd: error routine returned an error", err)

			// in error conditions we have to mark the routine as done because shutdown checks the routineWg
			s.routineWg.Done()

			// shutdown the server if we encountered an error from etcd's error channel
			shutdownServerIfErr(s, err)
		}()

		// routine to remove unhealthy members from the cluster
		go func(client *Client) {
			defer s.routineWg.Done()
			defer CloseClient(client)

			s.clusterCleanupRoutine(s.routineContext, s.Server.StopNotify(), cfg.UnhealthyTTL, cfg.CleanUpInterval, cfg.StartupGracePeriod, client)
		}(s.newServerClient())
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
				CloseServer(s)
			}
		}
	case <-done:
	}
	return
}

// removeSelfFromCluster removes this server from it's cluster
func (s *Server) removeSelfFromCluster(ctx context.Context) (err error) {
	members := s.Server.Cluster().Members()
	// just return if we're the last member ...or somehow there are no members
	if len(members) < 2 {
		return err
	}

	// continually try transferring leadership from this server unless the server is already stopped or the cluster is already unhealthy
	// note that etcd errors sometimes get mutated somewhere so we can't directly compare errors
	for ctx.Err() == nil && (err == nil || (err.Error() != etcdserver.ErrStopped.Error() && err.Error() != etcdserver.ErrUnhealthy.Error())) {
		err = s.Server.TransferLeadership()
		if err == nil {
			break
		}
	}

	// continually try removing self from the cluster until we succeed or the server stops
	// note that etcd errors sometimes get mutated somewhere so we can't directly compare errors
	for ctx.Err() == nil && errNilOrNotServerStopped(err) {
		_, err = s.Server.RemoveMember(ctx, uint64(s.Server.ID()))
		if err == nil {
			break
		}

	}

	return err
}

// shutdown shuts down the server with a cancelable context and without locking
func (s *Server) shutdown(ctx context.Context) (err error) {
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
		CloseServer(s)

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

// Shutdown shuts down the server with a cancelable context
func (s *Server) Shutdown(ctx context.Context) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.shutdown(ctx)
}

// New returns a new etcd Server
func New() *Server {
	return &Server{mutex: sync.RWMutex{}}
}
