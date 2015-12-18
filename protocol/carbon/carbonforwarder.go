package carbon

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"errors"

	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dpbuffered"
	"github.com/signalfx/metricproxy/dp/dpdimsort"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/stats"
	"golang.org/x/net/context"
)

// Forwarder is a sink that forwards points to a carbon endpoint
type Forwarder struct {
	dimensionComparor dpdimsort.Ordering
	connectionAddress string
	connectionTimeout time.Duration

	pool   connPool
	dialer func(network, address string, timeout time.Duration) (net.Conn, error)
}

var _ dpsink.Sink = &Forwarder{}

// connPool pools connections for reuse
type connPool struct {
	conns             []net.Conn
	connectionTimeout time.Duration

	mutex sync.Mutex
}

// Get a connection from the pool.  Returns nil if there is none in the pool
func (pool *connPool) Get() net.Conn {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if len(pool.conns) > 0 {
		c := pool.conns[len(pool.conns)-1]
		pool.conns = pool.conns[0 : len(pool.conns)-1]
		return c
	}
	return nil
}

// Return a connection to the pool.  Do not return closed or invalid connections
func (pool *connPool) Return(conn net.Conn) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.conns = append(pool.conns, conn)
}

func nonNil(err1 error, err2 error) error {
	if err1 != nil {
		return err1
	}
	return err2
}

// Close clears the connection pool
func (pool *connPool) Close() error {
	var err error
	for {
		c := pool.Get()
		if c == nil {
			return err
		}
		e2 := c.Close()
		err = nonNil(err, e2)
	}
}

// NewForwarder creates a new unbuffered forwarder for sending points to carbon
func NewForwarder(host string, port uint16, timeout time.Duration, dimensionOrder []string, drainingThreads uint32) (*Forwarder, error) {
	connectionAddress := net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
	var d net.Dialer
	d.Deadline = time.Now().Add(timeout)
	conn, err := d.Dial("tcp", connectionAddress)
	if err != nil {
		return nil, err
	}
	ret := &Forwarder{
		dimensionComparor: dpdimsort.NewOrdering(dimensionOrder),
		connectionTimeout: timeout,
		connectionAddress: connectionAddress,
		dialer:            net.DialTimeout,
		pool: connPool{
			conns: make([]net.Conn, 0, drainingThreads),
		},
	}
	ret.pool.Return(conn)
	return ret, nil
}

var defaultForwarderConfig = &config.ForwardTo{
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	BufferSize:      workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(10000)),
	Port:            workarounds.GolangDoesnotAllowPointerToUint16Literal(2003),
	DrainingThreads: workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(5)),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("carbonforwarder"),
	MaxDrainSize:    workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(1000)),
	DimensionsOrder: []string{},
}

var errRequiredHost = errors.New("carbon forwarder requires host config")

// ForwarderLoader loads a carbon forwarder that is buffered
func ForwarderLoader(ctx context.Context, forwardTo *config.ForwardTo) (protocol.Forwarder, error) {
	structdefaults.FillDefaultFrom(forwardTo, defaultForwarderConfig)
	if forwardTo.Host == nil {
		return nil, errRequiredHost
	}
	fwd, err := NewForwarder(*forwardTo.Host, *forwardTo.Port, *forwardTo.TimeoutDuration, forwardTo.DimensionsOrder, *forwardTo.DrainingThreads)
	if err != nil {
		return nil, err
	}
	counter := &dpsink.Counter{}
	dims := protocol.ForwarderDims(*forwardTo.Name, "carbon")
	buffer := dpbuffered.NewBufferedForwarder(ctx, *(&dpbuffered.Config{}).FromConfig(forwardTo), fwd)
	return &protocol.CompositeForwarder{
		Sink:   dpsink.FromChain(buffer, dpsink.NextWrap(counter)),
		Keeper: stats.ToKeeperMany(dims, counter, buffer),
		Closer: protocol.CompositeCloser(protocol.OkCloser(buffer.Close), fwd),
	}, nil
}

// Close empties out the connections' pool of open connections
func (carbonConnection *Forwarder) Close() error {
	return carbonConnection.pool.Close()
}

func (carbonConnection *Forwarder) datapointToGraphite(dp *datapoint.Datapoint) string {
	dims := dp.Dimensions
	sortedDims := carbonConnection.dimensionComparor.Sort(dims)
	ret := make([]string, 0, len(sortedDims)+1)
	for _, dim := range sortedDims {
		ret = append(ret, dims[dim])
	}
	ret = append(ret, dp.Metric)
	return strings.Join(ret, ".")
}

// AddDatapoints sends the points to a carbon endpoint.  Tries to reuse open connections
func (carbonConnection *Forwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error) {
	openConnection := carbonConnection.pool.Get()
	if openConnection == nil {
		openConnection, err = carbonConnection.dialer("tcp", carbonConnection.connectionAddress, carbonConnection.connectionTimeout)
		if err != nil {
			return
		}
	}
	defer func() {
		if err == nil {
			carbonConnection.pool.Return(openConnection)
		} else {
			openConnection.Close()
		}
	}()

	err = openConnection.SetDeadline(time.Now().Add(carbonConnection.connectionTimeout))
	if err != nil {
		return
	}
	var buf bytes.Buffer
	for _, dp := range points {
		carbonLine, exists := NativeCarbonLine(dp)
		if exists {
			fmt.Fprintf(&buf, "%s\n", carbonLine)
		} else {
			fmt.Fprintf(&buf, "%s %s %d\n", carbonConnection.datapointToGraphite(dp),
				dp.Value,
				dp.Timestamp.UnixNano()/time.Second.Nanoseconds())
		}
	}
	_, err = buf.WriteTo(openConnection)
	if err != nil {
		return
	}

	return nil
}

// AddEvents does not send events to carbon
func (carbonConnection *Forwarder) AddEvents(ctx context.Context, points []*event.Event) (err error) {
	return nil
}
