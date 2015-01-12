package forwarder

import (
	"encoding/json"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/listener"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var testConfig1 = `
{
  "Type":"carbon",
  "Host": "0.0.0.0",
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
		return;
	}
	listenFrom := config.ListenFrom{}
	// TODO: Enable :0 port and reading back the open port
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:0")
	forwardTo := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.CarbonListenerLoader(forwardTo, &listenFrom)
	assert.NoError(t, err)
	defer l.Close()

	var config config.ForwardTo
	assert.NoError(t, json.Unmarshal([]byte(testConfig1), &config))
	log.Info("%s", config)
	_, err = TcpGraphiteCarbonForwarerLoader(&config)
	assert.NoError(t, err)
}

func TestInvalidPort(t *testing.T) {

	ft := config.ForwardTo{
		Host: workarounds.GolangDoesnotAllowPointerToStringLiteral("invalid.port.address.should.not.bind"),
		Port: workarounds.GolangDoesnotAllowPointerToUint16Literal(1),
	}
	_, err := TcpGraphiteCarbonForwarerLoader(&ft)
	assert.NotEqual(t, nil, err, "Expect an error")
}

type carbonDatapoint struct {
	core.Datapoint
	line string
}

func (dp *carbonDatapoint) ToCarbonLine() string {
	return dp.line
}

func TestCreation(t *testing.T) {
	listenFrom := config.ListenFrom{}
	// TODO: Enable :0 port and reading back the open port
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12345")
	forwardTo := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.CarbonListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.GetStats()), "Expect no stats")
	forwarder, err := newTcpGraphiteCarbonForwarer("0.0.0.0", 12345, time.Second, 10, "", 1, []string{"zzfirst"})
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, "", forwarder.Name(), "Expect no name")
	assert.Equal(t, 0, len(forwarder.GetStats()), "Expect no stats")
	forwarder.openConnection = nil // Connection should remake itself
	timeToSend := time.Now().Round(time.Second)
	dpSent := core.NewAbsoluteTimeDatapoint("metric", map[string]string{"from": "bob", "host": "myhost", "zlast": "last", "zzfirst": "first"}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, timeToSend)
	log.Info("Sending a dp")
	forwarder.DatapointsChannel() <- dpSent
	log.Info("Looking for DP back")
	dp := <-forwardTo.datapointsChannel
	assert.Equal(t, "first.bob.myhost.last.metric", dp.Metric(), "Expect metric back")
	assert.Equal(t, dpSent.Timestamp(), dp.Timestamp(), "Expect metric back")
}

func TestDeadlineError(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12246")

	forwardTo := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.CarbonListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	carbonForwarder, err := newTcpGraphiteCarbonForwarer("0.0.0.0", 12246, time.Second, 10, "", 1, []string{})
	assert.Equal(t, nil, err, "Expect no error")

	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	mockConn := mockConn{
		setDeadlineBlock: make(chan bool),
	}
	mockConn.deadlineReturn = errors.New("deadline error")
	carbonForwarder.openConnection = &mockConn
	carbonForwarder.DatapointsChannel() <- dpSent
	assert.Equal(t, 0, len(forwardTo.datapointsChannel), "Expect drain from chan")
	_ = <-mockConn.setDeadlineBlock
	assert.Equal(t, 0, len(forwardTo.datapointsChannel), "Expect no stats")
}

func TestWriteError(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12347")

	forwardTo := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.CarbonListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	forwarder, err := newTcpGraphiteCarbonForwarer("0.0.0.0", 12347, time.Second, 10, "", 1, []string{})
	assert.Equal(t, nil, err, "Expect no error")

	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	mockConn := mockConn{
		setDeadlineBlock: make(chan bool),
	}
	mockConn.writeReturn = errors.New("write error")
	forwarder.openConnection = &mockConn
	forwarder.DatapointsChannel() <- dpSent
	assert.Equal(t, 0, len(forwardTo.datapointsChannel), "Expect drain from chan")
	_ = <-mockConn.setDeadlineBlock
	assert.Equal(t, 0, len(forwardTo.datapointsChannel), "Expect no stats")
}

func TestCarbonWrite(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12348")
	forwardTo := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.CarbonListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.GetStats()), "Expect no stats")
	forwarder, err := newTcpGraphiteCarbonForwarer("0.0.0.0", 12348, time.Second, 10, "", 1, []string{})
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, "", forwarder.Name(), "Expect no name")
	assert.Equal(t, 0, len(forwarder.GetStats()), "Expect no stats")
	forwarder.openConnection = nil // Connection should remake itself
	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	log.Info("Sending a dp")
	carbonReadyDp := &carbonDatapoint{dpSent, "lineitem 3 4"}
	forwarder.DatapointsChannel() <- carbonReadyDp
	log.Info("Looking for DP back")
	dp := <-forwardTo.datapointsChannel
	assert.Equal(t, "lineitem", dp.Metric(), "Expect metric back")
	assert.Equal(t, "3", dp.Value().WireValue(), "Expect value back")
}

func TestFailedConn(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12349")
	forwardTo := newBasicBufferedForwarder(100, 1, "", 1)
	l, err := listener.CarbonListenerLoader(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.GetStats()), "Expect no stats")
	forwarder, err := newTcpGraphiteCarbonForwarer("0.0.0.0", 12349, time.Second, 10, "", 1, []string{})
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, "", forwarder.Name(), "Expect no name")
	assert.Equal(t, 0, len(forwarder.GetStats()), "Expect no stats")
	forwarder.openConnection = nil // Connection should remake itself
	forwarder.connectionAddress = "0.0.0.0:1"
	dpSent := core.NewRelativeTimeDatapoint("metric", map[string]string{}, value.NewIntWire(2), com_signalfuse_metrics_protobuf.MetricType_GAUGE, 0)
	log.Info("Sending a dp")
	forwarder.DatapointsChannel() <- dpSent
	log.Info("Looking for DP back")
	assert.Equal(t, 0, len(forwardTo.datapointsChannel), "Expect no stats")
}
