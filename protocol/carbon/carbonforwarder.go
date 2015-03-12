package carbon

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/dimensions"
	"github.com/signalfx/metricproxy/stats"
)

type reconectingGraphiteCarbonConnection struct {
	datapoint.BufferedForwarder
	dimensionComparor dimensions.Ordering
	connectionAddress string
	connectionTimeout time.Duration
	connectionLock    sync.Mutex
	dialer            func(network, address string, timeout time.Duration) (net.Conn, error)
}

// NewTcpGraphiteCarbonForwarer creates a new forwarder for sending points to carbon
func newTCPGraphiteCarbonForwarer(host string, port uint16, timeout time.Duration, bufferSize uint32, name string, dimensionOrder []string) (*reconectingGraphiteCarbonConnection, error) {
	connectionAddress := net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
	var d net.Dialer
	d.Deadline = time.Now().Add(timeout)
	conn, err := d.Dial("tcp", connectionAddress)
	if err != nil {
		return nil, err
	}
	conn.Close()
	ret := &reconectingGraphiteCarbonConnection{
		dimensionComparor: dimensions.NewOrdering(dimensionOrder),
		BufferedForwarder: *datapoint.NewBufferedForwarder(bufferSize, 100, name, 1),
		connectionTimeout: timeout,
		connectionAddress: connectionAddress}
	ret.Start(ret.drainDatapointChannel)
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

// ForwarderLoader loads a carbon forwarder
func ForwarderLoader(forwardTo *config.ForwardTo) (stats.StatKeepingStreamer, error) {
	structdefaults.FillDefaultFrom(forwardTo, defaultForwarderConfig)
	if forwardTo.Host == nil {
		return nil, fmt.Errorf("Carbon forwarder requires host config")
	}
	return newTCPGraphiteCarbonForwarer(*forwardTo.Host, *forwardTo.Port, *forwardTo.TimeoutDuration, *forwardTo.BufferSize, *forwardTo.Name, forwardTo.DimensionsOrder)
}

func (carbonConnection *reconectingGraphiteCarbonConnection) Stats() []datapoint.Datapoint {
	return carbonConnection.BufferedForwarder.Stats()
}

func (carbonConnection *reconectingGraphiteCarbonConnection) datapointToGraphite(dp datapoint.Datapoint) string {
	dims := dp.Dimensions()
	sortedDims := carbonConnection.dimensionComparor.Sort(dims)
	ret := make([]string, 0, len(sortedDims)+1)
	for _, dim := range sortedDims {
		ret = append(ret, dims[dim])
	}
	ret = append(ret, dp.Metric())
	return strings.Join(ret, ".")
}

func (carbonConnection *reconectingGraphiteCarbonConnection) drainDatapointChannel(datapoints []datapoint.Datapoint) error {
	dialer := carbonConnection.dialer
	if dialer == nil {
		dialer = net.DialTimeout
	}
	openConnection, err := dialer("tcp", carbonConnection.connectionAddress, carbonConnection.connectionTimeout)
	if err != nil {
		return err
	}
	defer openConnection.Close()
	err = openConnection.SetDeadline(time.Now().Add(carbonConnection.connectionTimeout))
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, dp := range datapoints {
		carbonReadyDatapoint, ok := dp.(Native)
		if ok {
			fmt.Fprintf(&buf, "%s\n", carbonReadyDatapoint.ToCarbonLine())
		} else {
			fmt.Fprintf(&buf, "%s %s %d\n", carbonConnection.datapointToGraphite(dp),
				dp.Value(),
				dp.Timestamp().UnixNano()/time.Second.Nanoseconds())
		}
	}
	log.WithField("buf", buf).Debug("Will write to graphite")
	_, err = buf.WriteTo(openConnection)
	if err != nil {
		return err
	}

	return nil
}
