package carbon

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"sync"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/carbon/metricdeconstructor"
	"golang.org/x/net/context"
)

// Listener once setup will listen for carbon protocol points to forward on
type Listener struct {
	psocket              net.Listener
	sink                 dpsink.Sink
	metricDeconstructor  metricdeconstructor.MetricDeconstructor
	serverAcceptDeadline time.Duration
	connectionTimeout    time.Duration

	logger log.Logger
	stats  listenerStats
	wg     sync.WaitGroup
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

// Datapoints reports information about the total points seen by carbon
func (listener *Listener) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("invalid_datapoints", nil, atomic.LoadInt64(&listener.stats.invalidDatapoints)),
		sfxclient.Cumulative("total_connections", nil, atomic.LoadInt64(&listener.stats.totalConnections)),
		sfxclient.Gauge("active_connections", nil, atomic.LoadInt64(&listener.stats.activeConnections)),
		sfxclient.Cumulative("idle_timeouts", nil, atomic.LoadInt64(&listener.stats.idleTimeouts)),
		sfxclient.Cumulative("retry_listen_errors", nil, atomic.LoadInt64(&listener.stats.retriedListenErrors)),
	}
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
	RemoteAddr() net.Addr
}

func (listener *Listener) handleConnection(ctx context.Context, conn carbonListenConn) error {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	atomic.AddInt64(&listener.stats.totalConnections, 1)
	atomic.AddInt64(&listener.stats.activeConnections, 1)
	defer atomic.AddInt64(&listener.stats.activeConnections, -1)
	connLogger := log.NewContext(listener.logger).With(logkey.RemoteAddr, conn.RemoteAddr())
	for {
		conn.SetDeadline(time.Now().Add(listener.connectionTimeout))
		bytes, err := reader.ReadBytes((byte)('\n'))
		if err != nil && err != io.EOF {
			atomic.AddInt64(&listener.stats.idleTimeouts, 1)
			connLogger.Log(log.Err, err, "Listening for carbon data returned an error (Note: We timeout idle connections)")
			return err
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp, err := NewCarbonDatapoint(line, listener.metricDeconstructor)
			if err != nil {
				atomic.AddInt64(&listener.stats.invalidDatapoints, 1)
				connLogger.Log(logkey.CarbonLine, line, log.Err, err, "Received data on a carbon port, but it doesn't look like carbon data")
				return err
			}
			listener.sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp})
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
	defer listener.logger.Log("Carbon listener closed")
	for {
		deadlineable, ok := listener.psocket.(*net.TCPListener)
		if ok {
			deadlineable.SetDeadline(time.Now().Add(listener.serverAcceptDeadline))
		}
		conn, err := listener.psocket.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() || netErr.Temporary() {
					atomic.AddInt64(&listener.stats.retriedListenErrors, 1)
					continue
				}
			}
			listener.logger.Log(log.Err, err, "Unable to accept a socket connection")
			return
		}
		go func() {
			listener.handleConnection(context.Background(), conn)
		}()
	}
}

//var defaultListenerConfig = &config.ListenFrom{
//	ListenAddr:                 workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:2003"),
//	Name:                       workarounds.GolangDoesnotAllowPointerToStringLiteral("carbonlistener"),
//	TimeoutDuration:            workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
//	MetricDeconstructor:        workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
//	MetricDeconstructorOptions: workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
//	ServerAcceptDeadline:       workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second),
//}

//// ListenerLoader loads a listener for the carbon/graphite protocol from config
//func ListenerLoader(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom, logger log.Logger) (*Listener, error) {
//	structdefaults.FillDefaultFrom(listenFrom, defaultListenerConfig)
//	conf := listenerConfig{
//		serverAcceptDeadline: *listenFrom.ServerAcceptDeadline,
//		connectionTimeout:    *listenFrom.TimeoutDuration,
//		name:                 *listenFrom.Name,
//	}
//	//  *listenFrom.Name
//	return NewListener(
//		ctx, sink, conf, *listenFrom.ListenAddr,
//		*listenFrom.MetricDeconstructor, *listenFrom.MetricDeconstructorOptions, listenFrom.MetricDeconstructorOptionsJSON, logger)
//}

// ListenerConfig controls optional parameters for carbon listeners
type ListenerConfig struct {
	ServerAcceptDeadline *time.Duration
	ConnectionTimeout    *time.Duration
	ListenAddr           *string
	MetricDeconstructor  metricdeconstructor.MetricDeconstructor
	Logger               log.Logger
}

var defaultListenerConfig = &ListenerConfig{
	ServerAcceptDeadline: pointer.Duration(time.Second),
	ConnectionTimeout:    pointer.Duration(time.Second * 30),
	ListenAddr:           pointer.String("127.0.0.1:2003"),
	MetricDeconstructor:  &metricdeconstructor.IdentityMetricDeconstructor{},
}

// Addr returns the listening address of this carbon listener
func (listener *Listener) Addr() net.Addr {
	return listener.psocket.Addr()
}

// NewListener creates a new listener for carbon datapoints
func NewListener(sendTo dpsink.Sink, passedConf *ListenerConfig) (*Listener, error) {
	conf := pointer.FillDefaultFrom(passedConf, defaultListenerConfig).(*ListenerConfig)
	psocket, err := net.Listen("tcp", *conf.ListenAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot listen to addr %s", *conf.ListenAddr)
	}

	receiver := Listener{
		sink:                 sendTo,
		psocket:              psocket,
		metricDeconstructor:  conf.MetricDeconstructor,
		serverAcceptDeadline: *conf.ServerAcceptDeadline,
		connectionTimeout:    *conf.ConnectionTimeout,
		logger:               log.NewContext(conf.Logger).With(logkey.Protocol, "carbon", logkey.Direction, "listener"),
	}
	receiver.wg.Add(1)

	go receiver.startListening()
	return &receiver, nil
}
