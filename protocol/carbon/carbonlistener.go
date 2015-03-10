package carbon

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/protocol/carbon/metricdeconstructor"
	"github.com/signalfx/metricproxy/stats"
)

type carbonListener struct {
	totalPoints          *uint64
	psocket              net.Listener
	Streamer             datapoint.Streamer
	connectionTimeout    time.Duration
	isClosed             int32
	metricDeconstructor  metricdeconstructor.MetricDeconstructor
	name                 string
	totalDatapoints      int64
	invalidDatapoints    int64
	totalConnections     int64
	activeConnections    int64
	serverAcceptDeadline time.Duration
}

func (listener *carbonListener) Stats() []datapoint.Datapoint {
	ret := []datapoint.Datapoint{}
	stats := map[string]int64{
		"total_datapoints":   atomic.LoadInt64(&listener.totalDatapoints),
		"invalid_datapoints": atomic.LoadInt64(&listener.invalidDatapoints),
		"total_connections":  atomic.LoadInt64(&listener.totalConnections),
		"active_connections": atomic.LoadInt64(&listener.activeConnections),
	}
	for k, v := range stats {
		var t com_signalfuse_metrics_protobuf.MetricType
		if k == "active_connections" {
			t = com_signalfuse_metrics_protobuf.MetricType_GAUGE
		} else {
			t = com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER
		}
		ret = append(
			ret,
			datapoint.NewOnHostDatapointDimensions(
				k,
				datapoint.NewIntValue(v),
				t,
				map[string]string{"listener": listener.name}))
	}
	return ret
}

func (listener *carbonListener) Close() {
	listener.psocket.Close()
	atomic.StoreInt32(&listener.isClosed, 1)
}

type carbonListenConn interface {
	io.Reader
	Close() error
	SetDeadline(t time.Time) error
}

func (listener *carbonListener) handleConnection(conn carbonListenConn) error {
	reader := bufio.NewReader(conn)
	atomic.AddInt64(&listener.totalConnections, 1)
	atomic.AddInt64(&listener.activeConnections, 1)
	defer conn.Close()
	defer atomic.AddInt64(&listener.activeConnections, -1)
	for {
		conn.SetDeadline(time.Now().Add(listener.connectionTimeout))
		bytes, err := reader.ReadBytes((byte)('\n'))
		if err != nil && err != io.EOF {
			log.WithField("err", err).Warn("Listening for carbon data returned an error (Note: We timeout idle connections)")
			return err
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp, err := NewCarbonDatapoint(line, listener.metricDeconstructor)
			if err != nil {
				atomic.AddInt64(&listener.invalidDatapoints, 1)
				log.WithFields(log.Fields{"line": line, "err": err}).Warn("Received data on a carbon port, but it doesn't look like carbon data")
				return err
			}
			atomic.AddInt64(&listener.totalDatapoints, 1)
			listener.Streamer.Channel() <- dp
		}

		if err == io.EOF {
			return nil
		}
	}
}

func (listener *carbonListener) startListening() {
	for atomic.LoadInt32(&listener.isClosed) == 0 {
		deadlineable, ok := listener.psocket.(*net.TCPListener)
		if ok {
			deadlineable.SetDeadline(time.Now().Add(listener.serverAcceptDeadline))
		}
		conn, err := listener.psocket.Accept()
		if err != nil {
			timeoutError, ok := err.(net.Error)
			if ok && timeoutError.Timeout() {
				log.Debug("Timeout waiting for connection.  Expected, will continue")
				continue
			}
			log.WithField("err", err).Debug("Unable to accept a socket connection")
			continue
		}
		go listener.handleConnection(conn)
	}
	log.Info("Carbon listener closed")
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
func ListenerLoader(Streamer datapoint.Streamer, listenFrom *config.ListenFrom) (stats.ClosableKeeper, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultListenerConfig)
	return startListeningCarbonOnPort(
		*listenFrom.ListenAddr, Streamer, *listenFrom.TimeoutDuration,
		*listenFrom.MetricDeconstructor, *listenFrom.MetricDeconstructorOptions, *listenFrom.Name, *listenFrom.ServerAcceptDeadline)
}

func startListeningCarbonOnPort(listenAddr string, Streamer datapoint.Streamer,
	timeout time.Duration, metricDeconstructor string,
	metricDeconstructorOptions string, name string, serverAcceptDeadline time.Duration) (stats.ClosableKeeper, error) {
	psocket, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	deconstructor, err := metricdeconstructor.Load(metricDeconstructor, metricDeconstructorOptions)
	if err != nil {
		return nil, err
	}
	receiver := carbonListener{
		totalPoints:          new(uint64),
		psocket:              psocket,
		Streamer:             Streamer,
		connectionTimeout:    timeout,
		metricDeconstructor:  deconstructor,
		name:                 name,
		serverAcceptDeadline: serverAcceptDeadline,
	}
	go receiver.startListening()
	return &receiver, nil
}
