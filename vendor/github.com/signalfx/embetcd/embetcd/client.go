package embetcd

import (
	"context"
	"fmt"
	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/etcdserver"
	"net/url"
	"strings"
	"time"
)

// setupClusterNamespace configures the client with the EtcdClusterNamespace prefix
func setupClusterNamespace(client *Client) {
	// this package reserves a key namespace defined by the constant
	client.KV = namespace.NewKV(client.KV, EtcdClusterNamespace)
	client.Watcher = namespace.NewWatcher(client.Watcher, EtcdClusterNamespace)
	client.Lease = namespace.NewLease(client.Lease, EtcdClusterNamespace)
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

// Client wraps around an etcd v3 client and adds some helper functions
type Client struct {
	*cli.Client
}

// PutWithKeepAlive puts a key and value with a keep alive returns
// a lease, the keep alive response channel, and an err if one occurrs
func (c *Client) PutWithKeepAlive(ctx context.Context, key string, value string, ttl int64) (lease *cli.LeaseGrantResponse, keepAlive <-chan *cli.LeaseKeepAliveResponse, cancel context.CancelFunc, err error) {
	var keepAliveCtx context.Context
	keepAliveCtx, cancel = context.WithCancel(ctx)

	// continually try putting out key into the cluster until we succeed
	// note that etcdserver.ErrStopped is mutated somewhere in etcd so we can't directly compare the errors
	for ctx.Err() == nil && (err == nil || err.Error() != etcdserver.ErrStopped.Error()) {

		// create a lease for the member key
		if err == nil {
			// create a new lease with a 5 second ttl
			lease, err = c.Grant(keepAliveCtx, ttl)
		}

		// keep the lease alive if we successfully put the key in
		if err == nil {
			keepAlive, err = c.KeepAlive(context.Background(), lease.ID)
		}

		// put in a key for the server
		if err == nil {
			_, err = c.Put(ctx, key, value, cli.WithLease(lease.ID))
		}

		// break if se successfully put in the key
		if err == nil || err == etcdserver.ErrStopped {
			break
		}
	}

	return lease, keepAlive, cancel, err
}

func (c *Client) arePeerURLSInCluster(ctx context.Context, apURLS []url.URL) (areIn bool, err error) {
	var members *cli.MemberListResponse
	members, err = c.MemberList(ctx)

	if err == nil && members != nil && members.Members != nil {
		apStrings := URLSToStringSlice(apURLS)

		for _, member := range members.Members {
			memberPURLs := member.GetPeerURLs()
			for _, u := range apStrings {
				if StringIsInStringSlice(u, memberPURLs) {
					areIn = true
					break
				}
			}
		}
	}
	return areIn, err
}

// getServerPeers returns the peer urls for the cluster formatted for the initialCluster server configuration.
// The context that is passed in should have a configured timeout.
func (c *Client) getServerPeers(ctx context.Context, initialCluster string, serverName *string, apURLS []url.URL, dialTimeout *time.Duration) (peers string, err error) {
	var members *cli.MemberListResponse
	var timeout context.Context
	var cancel context.CancelFunc
	defer CancelContext(cancel)

	apURLStrings := URLSToStringSlice(apURLS)

	for ctx.Err() == nil && (err == nil || err.Error() != etcdserver.ErrStopped.Error()) {
		// initialize peers with the supplied initial cluster string
		peers = initialCluster

		// get the list of members
		timeout, cancel = context.WithTimeout(ctx, DurationOrDefault(dialTimeout, DefaultDialTimeout))
		members, err = c.MemberList(timeout)
		cancel()

		if err == nil {
			// add members to the initial cluster
			for _, member := range members.Members {
				// if there's at least one peer url add it to the initial cluster
				if pURLS := member.GetPeerURLs(); len(pURLS) > 0 {
					// peers should already have this server's address so we can safely append ",%s=%s"
					for _, url := range member.GetPeerURLs() {
						if !StringIsInStringSlice(url, apURLStrings) {
							peers = fmt.Sprintf("%s,%s=%s", peers, member.Name, url)
						}
					}
				}
			}
			break
		}
	}
	peers = dedupPeerString(peers)
	return peers, err
}

// serverNameConflicts returns true if the server name conflicts or returns false if it doesn't
func (c *Client) serverNameConflicts(ctx context.Context, name string) (conflicts bool, err error) {
	// get the cluster members
	var members *cli.MemberListResponse
	if members, err = c.MemberList(ctx); err == nil && ctx.Err() != context.DeadlineExceeded && members != nil {
		// add members to the initial cluster
		for _, member := range members.Members {
			if member.Name == name {
				conflicts = true
				err = ErrNameConflict
				break
			}
		}
	}
	return conflicts, err
}

// clusterName returns the name of the cluster an error if the cluster name conflicts
func (c *Client) clusterName(ctx context.Context) (name string, err error) {
	var resp *cli.GetResponse
	for ctx.Err() == nil && (err == nil || err.Error() != etcdserver.ErrStopped.Error()) {
		// verify that the cluster name matches before we add a member to the cluster
		// if you add the member to the cluster and check for the cluster name before you start the server,
		// then you run the risk of breaking quorum and stalling out on the cluster name check
		resp, err = c.Get(ctx, "name")
		if err == nil && resp != nil && len(resp.Kvs) > 0 {
			name = string(resp.Kvs[0].Value)
			break
		}
	}
	return name, err
}

// NewClient returns a new etcd v3client wrapped with some helper functions
func NewClient(cfg cli.Config) (client *Client, err error) {
	var etcdClient *cli.Client

	if etcdClient, err = cli.New(cfg); err == nil {
		client = &Client{Client: etcdClient}
	}

	return client, err
}
