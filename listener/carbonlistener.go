package listener

import (
	"bufio"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
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
}

func (listener *carbonListener) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

func (listener *carbonListener) Close() {
	listener.psocket.Close()
	atomic.StoreInt32(&listener.isClosed, 1)
}

func (listener *carbonListener) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	defer conn.Close()
	for {
		conn.SetDeadline(time.Now().Add(listener.connectionTimeout))
		bytes, err := readerReadBytes(reader, (byte)('\n'))
		if err != nil && err != io.EOF {
			glog.Warningf("Listening for carbon data returned an error (Note: We timeout idle connections): %s", err)
			return
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp, err := protocoltypes.NewCarbonDatapoint(line, listener.metricDeconstructor)
			if err != nil {
				glog.Warningf("Received data on a carbon port, but it doesn't look like carbon data: %s => %s", line, err)
				return
			}
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
				glog.V(2).Infof("Timeout waiting for connection.  Expected, will continue")
				continue
			}
			glog.Warningf("Unable to accept a socket connection: %s", err)
			continue
		}
		go listener.handleConnection(conn)
	}
	glog.Infof("Carbon listener closed")
}

var defaultCarbonConfig = &config.ListenFrom{
	ListenAddr:                 workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:2003"),
	TimeoutDuration:            workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	MetricDeconstructor:        workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
	MetricDeconstructorOptions: workarounds.GolangDoesnotAllowPointerToStringLiteral(""),
}

// CarbonListenerLoader loads a listener for the carbon/graphite protocol from config
func CarbonListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultCarbonConfig)
	return startListeningCarbonOnPort(*listenFrom.ListenAddr, DatapointStreamingAPI, *listenFrom.TimeoutDuration, *listenFrom.MetricDeconstructor, *listenFrom.MetricDeconstructorOptions)
}

func startListeningCarbonOnPort(listenAddr string, DatapointStreamingAPI core.DatapointStreamingAPI, timeout time.Duration, metricDeconstructor string, metricDeconstructorOptions string) (DatapointListener, error) {
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
	}
	go receiver.startListening()
	return &receiver, nil
}
