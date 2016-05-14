package carbon

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/timekeeper"
	"github.com/signalfx/metricproxy/dp/dpdimsort"
	"github.com/signalfx/metricproxy/protocol/common"
	"golang.org/x/net/context"
)

// Forwarder is a sink that forwards points to a carbon endpoint
type Forwarder struct {
	filtering.FilteredForwarder
	dimensionComparor dpdimsort.Ordering
	connectionAddress string
	connectionTimeout time.Duration

	tk     timekeeper.TimeKeeper
	pool   connPool
	dialer func(network, address string, timeout time.Duration) (net.Conn, error)
}

// ForwarderConfig controls optional parameters for a carbon forwarder
type ForwarderConfig struct {
	Filters                *filtering.FilterObj
	Port                   *uint16
	Timeout                *time.Duration
	DimensionOrder         []string
	IdleConnectionPoolSize *int64
	Timer                  timekeeper.TimeKeeper
}

var defaultForwarderConfig = &ForwarderConfig{
	Filters: &filtering.FilterObj{},
	Timeout: pointer.Duration(time.Second * 30),
	Port:    pointer.Uint16(2003),
	IdleConnectionPoolSize: pointer.Int64(5),
	Timer: &timekeeper.RealTime{},
}

// NewForwarder creates a new unbuffered forwarder for sending points to carbon
func NewForwarder(host string, passedConf *ForwarderConfig) (*Forwarder, error) { //} port uint16, timeout time.Duration, dimensionOrder []string, drainingThreads uint32) (*Forwarder, error) {
	conf := pointer.FillDefaultFrom(passedConf, defaultForwarderConfig).(*ForwarderConfig)

	connectionAddress := net.JoinHostPort(host, strconv.FormatUint(uint64(*conf.Port), 10))
	var d net.Dialer
	d.Deadline = time.Now().Add(*conf.Timeout)
	conn, err := d.Dial("tcp", connectionAddress)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot dial address %s", connectionAddress)
	}
	ret := &Forwarder{
		dimensionComparor: dpdimsort.NewOrdering(conf.DimensionOrder),
		connectionTimeout: *conf.Timeout,
		connectionAddress: connectionAddress,
		tk:                conf.Timer,
		dialer:            net.DialTimeout,
		pool: connPool{
			conns: make([]net.Conn, 0, *conf.IdleConnectionPoolSize),
		},
	}
	ret.Setup(passedConf.Filters)

	ret.pool.Return(conn)
	return ret, nil
}

// Close empties out the connections' pool of open connections
func (f *Forwarder) Close() error {
	return f.pool.Close()
}

// Datapoints returns connection pool datapoints
func (f *Forwarder) Datapoints() []*datapoint.Datapoint {
	datapoints := f.pool.Datapoints()
	datapoints = append(datapoints, f.GetFilteredDatapoints()...)
	return datapoints
}

func (f *Forwarder) datapointToGraphite(dp *datapoint.Datapoint) string {
	dims := dp.Dimensions
	sortedDims := f.dimensionComparor.Sort(dims)
	ret := make([]string, 0, len(sortedDims)+1)
	for _, dim := range sortedDims {
		ret = append(ret, dims[dim])
	}
	ret = append(ret, dp.Metric)
	return strings.Join(ret, ".")
}

func minTime(times []time.Time) time.Time {
	if len(times) == 1 {
		return times[0]
	}
	if times[0].Before(times[1]) {
		return times[0]
	}
	return times[1]
}

func (f *Forwarder) setMinTime(ctx context.Context, openConnection net.Conn) error {
	var timesVar [2]time.Time
	timeSlice := timesVar[0:0:2]
	if f.connectionTimeout.Nanoseconds() != 0 {
		timeSlice = append(timeSlice, f.tk.Now().Add(f.connectionTimeout))
	}
	if ctxTimeout, ok := ctx.Deadline(); ok {
		timeSlice = append(timeSlice, ctxTimeout)
	}
	if len(timeSlice) > 0 {
		min := minTime(timeSlice)
		if err := openConnection.SetDeadline(min); err != nil {
			return errors.Annotate(err, "cannot set connection deadline")
		}
	}
	return nil
}

// AddDatapoints sends the points to a carbon endpoint.  Tries to reuse open connections
func (f *Forwarder) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error) {
	openConnection := f.pool.Get()
	if openConnection == nil {
		openConnection, err = f.dialer("tcp", f.connectionAddress, f.connectionTimeout)
		if err != nil {
			err = errors.Annotatef(err, "cannot dial %s", f.connectionAddress)
			return
		}
	}
	defer func() {
		if err == nil {
			f.pool.Return(openConnection)
		} else {
			err = errors.NewMultiErr([]error{err, openConnection.Close()})
		}
	}()

	if err := f.setMinTime(ctx, openConnection); err != nil {
		return err
	}

	points = f.FilterDatapoints(points)
	if len(points) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, dp := range points {
		if carbonLine, exists := NativeCarbonLine(dp); exists {
			_, err = fmt.Fprintf(&buf, "%s\n", carbonLine)
			errors.PanicIfErr(err, "buffer writes should not error out")
			continue
		}
		_, err = fmt.Fprintf(&buf, "%s %s %d\n", f.datapointToGraphite(dp),
			dp.Value,
			dp.Timestamp.UnixNano()/time.Second.Nanoseconds())
		errors.PanicIfErr(err, "buffer writes should not error out")
	}
	_, err = buf.WriteTo(openConnection)
	if err != nil {
		return errors.Annotate(err, "cannot fully write buf to carbon connection")
	}
	return nil
}
