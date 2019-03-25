package embetcd

import (
	"context"
	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"path"
)

// Client wraps around an etcd v3 client and adds some helper functions
type Client struct {
	*cli.Client
}

// PutWithKeepAlive puts a key and value with a keep alive returns
// a lease, the keep alive response channel, and an err if one occurrs
func (c *Client) PutWithKeepAlive(ctx context.Context, key string, value string, ttl int64) (lease *cli.LeaseGrantResponse, keepAlive <-chan *cli.LeaseKeepAliveResponse, err error) {
	// create a lease for the member key
	if err == nil {
		// create a new lease with a 5 second ttl
		lease, err = c.Grant(context.Background(), ttl)
	}

	// keep the lease alive if we successfully put the key in
	if err == nil {
		keepAlive, err = c.KeepAlive(context.Background(), lease.ID)
	}

	// put in a key for the server
	if err == nil {
		_, err = c.Put(ctx, key, value, cli.WithLease(lease.ID))
	}

	return lease, keepAlive, err
}

// Lock accepts an etcd client, context (with cancel), and name and creates a concurrent lock
func (c *Client) Lock(ctx context.Context, name string) (unlock func(context.Context) error, err error) {
	var session *concurrency.Session
	session, err = concurrency.NewSession(c.Client)

	var mutex *concurrency.Mutex
	if err == nil {
		// create a mutex using the session under /mutex/name
		mutex = concurrency.NewMutex(session, path.Join("", "mutex", name))

		// lock the mutex and return a function to unlock the mutex
		err = mutex.Lock(ctx)
	}

	// set unlock function
	if err == nil {
		unlock = func(ctx context.Context) (err error) {
			// we need to return the first error we encounter
			// but we need to do both operations
			errs := make([]error, 2)
			errs[0] = mutex.Unlock(ctx)
			errs[1] = session.Close()

			// return first error
			for _, err := range errs {
				if err != nil {
					return err
				}
			}

			// return nil
			return nil
		}
	}

	return unlock, err
}

// NewClient returns a new etcd v3client wrapped with some helper functions
func NewClient(cfg cli.Config) (client *Client, err error) {
	var etcdClient *cli.Client

	if etcdClient, err = cli.New(cfg); err == nil {
		client = &Client{Client: etcdClient}
	}

	return client, err
}
