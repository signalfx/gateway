package carbon

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/signalfx/golib/v3/errors"
	"github.com/signalfx/golib/v3/sfxclient"
)

// connPool pools connections for reuse
type connPool struct {
	conns []net.Conn

	mu sync.Mutex

	stats struct {
		reusuedConnections  int64
		returnedConnections int64
	}
}

// Get a connection from the pool.  Returns nil if there is none in the pool
func (pool *connPool) Get() net.Conn {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) > 0 {
		c := pool.conns[len(pool.conns)-1]
		pool.conns = pool.conns[0 : len(pool.conns)-1]
		atomic.AddInt64(&pool.stats.reusuedConnections, 1)
		return c
	}
	return nil
}

func (pool *connPool) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("reused_connections", map[string]string{"struct": "connPool"}, atomic.LoadInt64(&pool.stats.reusuedConnections)),
		sfxclient.Cumulative("returned_connections", map[string]string{"struct": "connPool"}, atomic.LoadInt64(&pool.stats.returnedConnections)),
	}
}

// Return a connection to the pool.  Do not return closed or invalid connections
func (pool *connPool) Return(conn net.Conn) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	atomic.AddInt64(&pool.stats.returnedConnections, 1)
	pool.conns = append(pool.conns, conn)
}

// Close clears the connection pool
func (pool *connPool) Close() error {
	var errs []error
	for {
		c := pool.Get()
		if c == nil {
			return errors.NewMultiErr(errs)
		}
		// Fix the reusuedConnections stat being incremented by Get()
		atomic.AddInt64(&pool.stats.reusuedConnections, -1)
		errs = append(errs, c.Close())
	}
}
