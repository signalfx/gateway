package carbon
import (
	"net"
	"sync"
	"time"
	"github.com/signalfx/golib/errors"
)

// connPool pools connections for reuse
type connPool struct {
	conns             []net.Conn
	connectionTimeout time.Duration

	mu sync.Mutex
}

// Get a connection from the pool.  Returns nil if there is none in the pool
func (pool *connPool) Get() net.Conn {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) > 0 {
		c := pool.conns[len(pool.conns)-1]
		pool.conns = pool.conns[0 : len(pool.conns)-1]
		return c
	}
	return nil
}

// Return a connection to the pool.  Do not return closed or invalid connections
func (pool *connPool) Return(conn net.Conn) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
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
		errs = append(errs, c.Close())
	}
}
