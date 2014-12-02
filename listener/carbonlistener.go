package listener

import (
	"bufio"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/listener/metricdeconstructor"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

func originalReaderReadBytes(reader *bufio.Reader, delim byte) ([]byte, error) {
	return reader.ReadBytes(delim)
}

var readerReadBytes = originalReaderReadBytes

type carbonListener struct {
	totalPoints           *uint64
	psocket               net.Listener
	DatapointStreamingAPI core.DatapointStreamingAPI
	connectionTimeout     time.Duration
	isClosed              int32
	metricDeconstructor   metricdeconstructor.MetricDeconstructor
	name                  string
	totalDatapoints       int64
	invalidDatapoints     int64
	totalConnections      int64
	activeConnections     int64
}

func (listener *carbonListener) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
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
			protocoltypes.NewOnHostDatapointDimensions(
				k,
				value.NewIntWire(v),
				t,
				map[string]string{"listener": listener.name}))
	}
	return ret
}

func (listener *carbonListener) Close() {
	listener.psocket.Close()
	atomic.StoreInt32(&listener.isClosed, 1)
}

func (listener *carbonListener) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	atomic.AddInt64(&listener.totalConnections, 1)
	atomic.AddInt64(&listener.activeConnections, 1)
	defer conn.Close()
	defer atomic.AddInt64(&listener.activeConnections, -1)
	for {
		conn.SetDeadline(time.Now().Add(listener.connectionTimeout))
		bytes, err := readerReadBytes(reader, (byte)('\n'))
		if err != nil && err != io.EOF {
			log.WithField("err", err).Warn("Listening for carbon data returned an error (Note: We timeout idle connections)")
			return
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp, err := protocoltypes.NewCarbonDatapoint(line, listener.metricDeconstructor)
			if err != nil {
				atomic.AddInt64(&listener.invalidDatapoints, 1)
				log.WithFields(log.Fields{"line": line, "err": err}).Warn("Received data on a carbon port, but it doesn't look like carbon data")
				return
			}
			atomic.AddInt64(&listener.totalDatapoints, 1)
			listener.DatapointStreamingAPI.DatapointsChannel() <- dp
		}

		if err == io.EOF {
			break
		}
	}
}

func (listener *carbonListener) startListening() {
	for atomic.LoadInt32(&listener.isClosed) == 0 {
		deadlineable, ok := listener.psocket.(*net.TCPListener)
		if ok {
			deadlineable.SetDeadline(time.Now().Add(1 * time.Second))
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

var defaultCarbonConfig = &config.ListenFrom{
	ListenAddr:                 workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:2003"),
	Name:                       workarounds.GolangDoesnotAllowPointerToStringLiteral("carbonlistener"),
	TimeoutDuration:            workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	MetricDeconstructor:        workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	MetricDeconstructorOptions: workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
}

// CarbonListenerLoader loads a listener for the carbon/graphite protocol from config
func CarbonListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultCarbonConfig)
	return startListeningCarbonOnPort(
		*listenFrom.ListenAddr, DatapointStreamingAPI, *listenFrom.TimeoutDuration,
		*listenFrom.MetricDeconstructor, *listenFrom.MetricDeconstructorOptions, *listenFrom.Name)
}

func startListeningCarbonOnPort(listenAddr string, DatapointStreamingAPI core.DatapointStreamingAPI,
	timeout time.Duration, metricDeconstructor string,
	metricDeconstructorOptions string, name string) (DatapointListener, error) {
	psocket, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	deconstructor, err := metricdeconstructor.Load(metricDeconstructor, metricDeconstructorOptions)
	if err != nil {
		return nil, err
	}
	receiver := carbonListener{
		totalPoints:           new(uint64),
		psocket:               psocket,
		DatapointStreamingAPI: DatapointStreamingAPI,
		connectionTimeout:     timeout,
		metricDeconstructor:   deconstructor,
		name:                  name,
	}
	go receiver.startListening()
	return &receiver, nil
}
