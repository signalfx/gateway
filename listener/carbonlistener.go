package listener

import (
	"bufio"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

type carbonListener struct {
	totalPoints           *uint64
	psocket               net.Listener
	DatapointStreamingAPI core.DatapointStreamingAPI
	connectionTimeout     time.Duration
	isClosed              int32
}

func (listener *carbonListener) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

func (listener *carbonListener) Close() {
	listener.psocket.Close()
	atomic.StoreInt32(&listener.isClosed, 1)
}

var readerReadBytes = func(reader *bufio.Reader, delim byte) ([]byte, error) { return reader.ReadBytes(delim) }

func (listener *carbonListener) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	defer conn.Close()
	for {
		conn.SetDeadline(time.Now().Add(listener.connectionTimeout))
		bytes, err := readerReadBytes(reader, (byte)('\n'))
		if err != nil && err != io.EOF {
			glog.Warningf("Carbon listener pipe closed %s", err)
			return
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp, err := protocoltypes.NewCarbonDatapoint(line)

			if err != nil {
				glog.Warningf("Error parsing carbon line: %s", err)
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
				glog.V(1).Infof("Timeout waiting for connection.  Expected, will continue")
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
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:2003"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
}

// CarbonListenerLoader loads a listener for the carbon/graphite protocol from config
func CarbonListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultCarbonConfig)
	return startListeningCarbonOnPort(*listenFrom.ListenAddr, DatapointStreamingAPI, *listenFrom.TimeoutDuration)
}

func startListeningCarbonOnPort(listenAddr string, DatapointStreamingAPI core.DatapointStreamingAPI, timeout time.Duration) (DatapointListener, error) {
	psocket, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	receiver := carbonListener{
		totalPoints:           new(uint64),
		psocket:               psocket,
		DatapointStreamingAPI: DatapointStreamingAPI,
		connectionTimeout:     timeout,
	}
	go receiver.startListening()
	return &receiver, nil
}
