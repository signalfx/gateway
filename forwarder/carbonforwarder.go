package forwarder

import (
	"bytes"
	"fmt"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"net"
	"strconv"
	"sync"
	"time"
)

type reconectingGraphiteCarbonConnection struct {
	*basicBufferedForwarder
	openConnection    net.Conn
	connectionAddress string
	connectionTimeout time.Duration
	connectionLock    sync.Mutex
}

// NewTcpGraphiteCarbonForwarer creates a new forwarder for sending points to carbon
func newTcpGraphiteCarbonForwarer(host string, port uint16, timeout time.Duration, bufferSize uint32, name string) (*reconectingGraphiteCarbonConnection, error) {
	connectionAddress := net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
	var d net.Dialer
	d.Deadline = time.Now().Add(timeout)
	conn, err := d.Dial("tcp", connectionAddress)
	if err != nil {
		return nil, err
	}
	ret := &reconectingGraphiteCarbonConnection{
		basicBufferedForwarder: newBasicBufferedForwarder(bufferSize, 100, name, 1),
		openConnection:         conn,
		connectionTimeout:      timeout,
		connectionAddress:      connectionAddress}
	ret.start(ret.drainDatapointChannel)
	return ret, nil
}

func (carbonConnection *reconectingGraphiteCarbonConnection) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

var defaultCarbonConfig = &config.ForwardTo{
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	BufferSize:      workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(10000)),
	Port:            workarounds.GolangDoesnotAllowPointerToUint16Literal(2003),
	DrainingThreads: workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(5)),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("carbonforwarder"),
	MaxDrainSize:    workarounds.GolangDoesnotAllowPointerToUintLiteral(uint32(1000)),
}

// TcpGraphiteCarbonForwarerLoader loads a carbon forwarder
func TcpGraphiteCarbonForwarerLoader(forwardTo *config.ForwardTo) (core.StatKeepingStreamingAPI, error) {
	structdefaults.FillDefaultFrom(forwardTo, defaultCarbonConfig)
	return newTcpGraphiteCarbonForwarer(*forwardTo.Host, *forwardTo.Port, *forwardTo.TimeoutDuration, *forwardTo.BufferSize, *forwardTo.Name)
}

func (carbonConnection *reconectingGraphiteCarbonConnection) createClientIfNeeded() error {
	var err error
	if carbonConnection.openConnection == nil {
		carbonConnection.openConnection, err = net.Dial("tcp", carbonConnection.connectionAddress)
	}
	return err
}

func (carbonConnection *reconectingGraphiteCarbonConnection) drainDatapointChannel(datapoints []core.Datapoint) error {
	if err := carbonConnection.createClientIfNeeded(); err != nil {
		return err
	}
	err := carbonConnection.openConnection.SetDeadline(time.Now().Add(carbonConnection.connectionTimeout))
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, datapoint := range datapoints {
		carbonReadyDatapoint, ok := datapoint.(protocoltypes.CarbonReady)
		if ok {
			fmt.Fprintf(&buf, "%s\n", carbonReadyDatapoint.ToCarbonLine())
		} else {
			fmt.Fprintf(&buf, "%s %s %d\n", datapoint.Metric(), datapoint.Value().WireValue(), datapoint.Timestamp().UnixNano()/time.Second.Nanoseconds())
		}
	}
	glog.V(2).Infof("Will write: `%s`", buf.String())
	_, err = buf.WriteTo(carbonConnection.openConnection)
	if err != nil {
		return err
	}

	return nil
}
