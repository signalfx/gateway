package carbon

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dplocal"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/carbon/metricdeconstructor"
	"github.com/signalfx/metricproxy/stats"
	"golang.org/x/net/context"
)

type listenerConfig struct {
	serverAcceptDeadline time.Duration
	connectionTimeout    time.Duration
	name                 string
}

// Listener once setup will listen for carbon protocol points to forward on
type Listener struct {
	psocket             net.Listener
	sink                dpsink.Sink
	metricDeconstructor metricdeconstructor.MetricDeconstructor

	st stats.Keeper

	stats listenerStats
	conf  listenerConfig
	wg    sync.WaitGroup
	ctx   context.Context
}

var _ protocol.Listener = &Listener{}

type listenerStats struct {
	totalDatapoints     int64
	idleTimeouts        int64
	retriedListenErrors int64
	totalEOFCloses      int64
	invalidDatapoints   int64
	totalConnections    int64
	activeConnections   int64
}

// Stats reports information about the total points seen by carbon
func (listener *Listener) Stats() []*datapoint.Datapoint {
	ret := []*datapoint.Datapoint{}
	stats := map[string]int64{
		"invalid_datapoints": atomic.LoadInt64(&listener.stats.invalidDatapoints),
		"total_connections":  atomic.LoadInt64(&listener.stats.totalConnections),
		"active_connections": atomic.LoadInt64(&listener.stats.activeConnections),
	}
	for k, v := range stats {
		var t datapoint.MetricType
		if k == "active_connections" {
			t = datapoint.Gauge
		} else {
			t = datapoint.Counter
		}
		ret = append(
			ret,
			dplocal.NewOnHostDatapointDimensions(
				k,
				datapoint.NewIntValue(v),
				t,
				map[string]string{"listener": listener.conf.name}))
	}
	return append(ret, listener.st.Stats()...)
}

// Close the exposed carbon port
func (listener *Listener) Close() error {
	err := listener.psocket.Close()
	listener.wg.Wait()
	return err
}

type carbonListenConn interface {
	io.Reader
	Close() error
	SetDeadline(t time.Time) error
}

func (listener *Listener) handleConnection(conn carbonListenConn) error {
	reader := bufio.NewReader(conn)
	atomic.AddInt64(&listener.stats.totalConnections, 1)
	atomic.AddInt64(&listener.stats.activeConnections, 1)
	defer conn.Close()
	defer atomic.AddInt64(&listener.stats.activeConnections, -1)
	for {
		conn.SetDeadline(time.Now().Add(listener.conf.connectionTimeout))
		bytes, err := reader.ReadBytes((byte)('\n'))
		if err != nil && err != io.EOF {
			atomic.AddInt64(&listener.stats.idleTimeouts, 1)
			log.WithField("err", err).Warn("Listening for carbon data returned an error (Note: We timeout idle connections)")
			return err
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp, err := NewCarbonDatapoint(line, listener.metricDeconstructor)
			if err != nil {
				atomic.AddInt64(&listener.stats.invalidDatapoints, 1)
				log.WithFields(log.Fields{"line": line, "err": err}).Warn("Received data on a carbon port, but it doesn't look like carbon data")
				return err
			}
			listener.sink.AddDatapoints(listener.ctx, []*datapoint.Datapoint{dp})
			atomic.AddInt64(&listener.stats.totalDatapoints, 1)
		}

		if err == io.EOF {
			atomic.AddInt64(&listener.stats.totalEOFCloses, 1)
			return nil
		}
	}
}

func (listener *Listener) startListening() {
	defer listener.wg.Done()
	defer log.Info("Carbon listener closed")
	for {
		deadlineable, ok := listener.psocket.(*net.TCPListener)
		if ok {
			deadlineable.SetDeadline(time.Now().Add(listener.conf.serverAcceptDeadline))
		}
		conn, err := listener.psocket.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() || netErr.Temporary() {
					atomic.AddInt64(&listener.stats.retriedListenErrors, 1)
					log.Debug("Timeout (or temp) waiting for connection.  Expected, will continue")
					continue
				}
			}
			log.WithField("err", err).Info("Unable to accept a socket connection")
			return
		}
		go listener.handleConnection(conn)
	}
}

var defaultListenerConfig = &config.ListenFrom{
	ListenAddr:                 workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:2003"),
	Name:                       workarounds.GolangDoesnotAllowPointerToStringLiteral("carbonlistener"),
	TimeoutDuration:            workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	MetricDeconstructor:        workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	MetricDeconstructorOptions: workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	ServerAcceptDeadline:       workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second),
}

// ListenerLoader loads a listener for the carbon/graphite protocol from config
func ListenerLoader(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (*Listener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultListenerConfig)
	conf := listenerConfig{
		serverAcceptDeadline: *listenFrom.ServerAcceptDeadline,
		connectionTimeout:    *listenFrom.TimeoutDuration,
		name:                 *listenFrom.Name,
	}
	//  *listenFrom.Name
	return NewListener(
		ctx, sink, conf, *listenFrom.ListenAddr,
		*listenFrom.MetricDeconstructor, *listenFrom.MetricDeconstructorOptions, listenFrom.MetricDeconstructorOptionsJSON)
}

// NewListener creates a new listener for carbon datapoints
func NewListener(ctx context.Context, sink dpsink.Sink, conf listenerConfig, listenAddr string,
	metricDeconstructor string, metricDeconstructorOptions string, metricDeconstructorJSON map[string]interface{}) (*Listener, error) {
	psocket, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	deconstructor, err := metricdeconstructor.Load(metricDeconstructor, metricDeconstructorOptions)
	if err != nil {
		deconstructor, err = metricdeconstructor.LoadJSON(metricDeconstructor, metricDeconstructorJSON)
		if err != nil {
			return nil, err
		}
	}

	counter := &dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, dpsink.NextWrap(counter))

	receiver := Listener{
		sink:                finalSink,
		psocket:             psocket,
		conf:                conf,
		metricDeconstructor: deconstructor,
		ctx:                 ctx,
		st:                  stats.ToKeeperMany(protocol.ListenerDims(conf.name, "carbon"), counter),
	}
	receiver.wg.Add(1)

	go receiver.startListening()
	return &receiver, nil
}
