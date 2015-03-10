package carbon

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/nettest"
	"github.com/stretchr/testify/assert"
)

var testConfig1 = `
{
  "Type":"carbon",
  "Host": "127.0.0.1",
  "Port": 2013
}
`

type mockConn struct {
	a.Conn
	deadlineReturn   error
	setDeadlineBlock chan bool
	writeReturn      error
}

func (conn *mockConn) SetDeadline(t time.Time) error {
	r := conn.deadlineReturn
	conn.deadlineReturn = nil
	conn.setDeadlineBlock <- true
	return r
}

func (conn *mockConn) Write(bytes []byte) (int, error) {
	r := conn.writeReturn
	conn.writeReturn = nil
	return len(bytes), r
}

func TestConfig1(t *testing.T) {
	if true {
		return
	}
	listenFrom := config.ListenFrom{}
	// TODO: Enable :0 port and reading back the open port
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
	forwardTo := datapoint.NewBufferedForwarder(100, 1, "", 1)
	l, err := ListenerLoader(forwardTo, &listenFrom)
	assert.NoError(t, err)
	defer l.Close()

	var config config.ForwardTo
	assert.NoError(t, json.Unmarshal([]byte(testConfig1), &config))
	log.Info("%s", config)
	_, err = ForwarderLoader(&config)
	assert.NoError(t, err)
}

func TestNoHost(t *testing.T) {
	var config config.ForwardTo
	_, err := ForwarderLoader(&config)
	assert.Error(t, err)
}

func TestInvalidPort(t *testing.T) {

	ft := config.ForwardTo{
		Host: workarounds.GolangDoesnotAllowPointerToStringLiteral("invalid.port.address.should.not.bind"),
		Port: workarounds.GolangDoesnotAllowPointerToUint16Literal(1),
	}
	_, err := ForwarderLoader(&ft)
	assert.NotEqual(t, nil, err, "Expect an error")
}

type carbonDatapoint struct {
	datapoint.Datapoint
	line string
}

func (dp *carbonDatapoint) ToCarbonLine() string {
	return dp.line
}

func TestCreation(t *testing.T) {
	listenFrom := config.ListenFrom{}
	// TODO: Enable :0 port and reading back the open port
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
	forwardTo := datapoint.NewBufferedForwarder(100, 1, "", 1)
	l, err := ListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.Stats()), "Expect no stats")
	forwarder, err := newTcpGraphiteCarbonForwarer("127.0.0.1", nettest.TCPPort(l.(*carbonListener).psocket), time.Second, 10, "", []string{"zzfirst"})
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, "", forwarder.Name(), "Expect no name")
	assert.Equal(t, 7, len(forwarder.Stats()))
	forwarder.openConnection = nil // Connection should remake itself
	timeToSend := time.Now().Round(time.Second)
	dpSent := datapoint.NewAbsoluteTime("metric", map[string]string{"from": "bob", "host": "myhost", "zlast": "last", "zzfirst": "first"}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, timeToSend)
	log.Info("Sending a dp")
	forwarder.Channel() <- dpSent
	log.Info("Looking for DP back")
	dp := <-forwardTo.DatapointsChannel
	assert.Equal(t, "first.bob.myhost.last.metric", dp.Metric(), "Expect metric back")
	assert.Equal(t, dpSent.Timestamp(), dp.Timestamp(), "Expect metric back")
}

func TestDeadlineError(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")

	forwardTo := datapoint.NewBufferedForwarder(100, 1, "", 1)
	l, err := ListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	carbonForwarder, err := newTcpGraphiteCarbonForwarer("127.0.0.1", nettest.TCPPort(l.(*carbonListener).psocket), time.Second, 10, "", []string{})
	assert.Equal(t, nil, err, "Expect no error")

	mockConn := mockConn{
		setDeadlineBlock: make(chan bool),
	}
	mockConn.deadlineReturn = errors.New("deadline error")
	carbonForwarder.openConnection = &mockConn

	assert.Equal(t, 0, len(forwardTo.DatapointsChannel), "Expect drain from chan")
	assert.NotNil(t, mockConn.deadlineReturn)
	go carbonForwarder.drainDatapointChannel(nil)
	<-mockConn.setDeadlineBlock
	assert.Equal(t, 0, len(forwardTo.DatapointsChannel), "Expect no stats")
	assert.Nil(t, mockConn.deadlineReturn)
}

func TestWriteError(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")

	forwardTo := datapoint.NewBufferedForwarder(100, 1, "", 1)
	l, err := ListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	forwarder, err := newTcpGraphiteCarbonForwarer("127.0.0.1", nettest.TCPPort(l.(*carbonListener).psocket), time.Second, 10, "", []string{})
	assert.Equal(t, nil, err, "Expect no error")

	dpSent := datapoint.NewRelativeTime("metric", map[string]string{}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	mockConn := mockConn{
		setDeadlineBlock: make(chan bool),
	}
	mockConn.writeReturn = errors.New("write error")
	forwarder.openConnection = &mockConn
	forwarder.Channel() <- dpSent
	assert.Equal(t, 0, len(forwardTo.DatapointsChannel), "Expect drain from chan")
	_ = <-mockConn.setDeadlineBlock
	assert.Equal(t, 0, len(forwardTo.DatapointsChannel), "Expect no stats")
}

func TestCarbonWrite(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
	forwardTo := datapoint.NewBufferedForwarder(100, 1, "", 1)
	l, err := ListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.Stats()), "Expect no stats")
	forwarder, err := newTcpGraphiteCarbonForwarer("127.0.0.1", nettest.TCPPort(l.(*carbonListener).psocket), time.Second, 10, "", []string{})
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, "", forwarder.Name(), "Expect no name")
	forwarder.openConnection = nil // Connection should remake itself
	dpSent := datapoint.NewRelativeTime("metric", map[string]string{}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	log.Info("Sending a dp")
	carbonReadyDp := &carbonDatapoint{dpSent, "lineitem 3 4"}
	forwarder.Channel() <- carbonReadyDp
	log.Info("Looking for DP back")
	dp := <-forwardTo.DatapointsChannel
	assert.Equal(t, "lineitem", dp.Metric(), "Expect metric back")
	assert.Equal(t, "3", dp.Value().String(), "Expect value back")
}

func TestFailedConn(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
	forwardTo := datapoint.NewBufferedForwarder(100, 1, "", 1)
	l, err := ListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.Stats()), "Expect no stats")
	forwarder, err := newTcpGraphiteCarbonForwarer("127.0.0.1", nettest.TCPPort(l.(*carbonListener).psocket), time.Second, 10, "", []string{})
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, "", forwarder.Name(), "Expect no name")
	forwarder.openConnection = nil // Connection should remake itself
	forwarder.connectionAddress = "127.0.0.1:1"
	dpSent := datapoint.NewRelativeTime("metric", map[string]string{}, datapoint.NewIntValue(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	log.Info("Sending a dp")
	forwarder.Channel() <- dpSent
	log.Info("Looking for DP back")
	assert.Equal(t, 0, len(forwardTo.DatapointsChannel), "Expect no stats")
}
