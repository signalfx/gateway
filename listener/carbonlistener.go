package listener

import (
	"bufio"
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"io"
	"net"
	"time"
)

type carbonListener struct {
	totalPoints           *uint64
	psocket               net.Listener
	DatapointStreamingAPI core.DatapointStreamingAPI
	connectionTimeout     time.Duration
}

func (listener *carbonListener) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

func (listener *carbonListener) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	defer conn.Close()
	for {
		conn.SetDeadline(time.Now().Add(listener.connectionTimeout))
		bytes, err := reader.ReadBytes((byte)('\n'))
		if err != nil && err != io.EOF {
			glog.Warningf("Carbon listener pipe closed %s", err)
			return
		}
		line := string(bytes)
		dp, err := protocoltypes.NewCarbonDatapoint(line)
		if err != nil {
			glog.Warningf("Error parsing carbon line: %s", err)
			return
		}
		listener.DatapointStreamingAPI.DatapointsChannel() <- dp
	}
}

func (listener *carbonListener) startListening() {
	for {
		conn, err := listener.psocket.Accept()
		if err != nil {
			glog.Warningf("Unable to accept a socket connection: %s", err)
			continue
		}
		go listener.handleConnection(conn)
	}
}

// CarbonListenerLoader loads a listener for the carbon/graphite protocol from config
func CarbonListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
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
