package carbon

import (
	"errors"
	"net"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dptest"
	"github.com/signalfx/metricproxy/nettest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

var testConfig1 = `
{
  "Type":"carbon",
  "Host": "127.0.0.1",
  "Port": 2013
}
`

type mockConn struct {
	net.Conn
	deadlineReturn error
	writeReturn    error
}

func (conn *mockConn) SetDeadline(t time.Time) error {
	r := conn.deadlineReturn
	conn.deadlineReturn = nil
	return r
}

func (conn *mockConn) Close() error {
	return nil
}

func (conn *mockConn) Write(bytes []byte) (int, error) {
	r := conn.writeReturn
	conn.writeReturn = nil
	return len(bytes), r
}

func TestNoHost(t *testing.T) {
	var config config.ForwardTo
	_, err := ForwarderLoader(context.Background(), &config)
	assert.Error(t, err)
}

func TestInvalidPort(t *testing.T) {

	ft := config.ForwardTo{
		Host: workarounds.GolangDoesnotAllowPointerToStringLiteral("invalid.port.address.should.not.bind"),
		Port: workarounds.GolangDoesnotAllowPointerToUint16Literal(1),
	}
	_, err := ForwarderLoader(context.Background(), &ft)
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
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := ListenerLoader(ctx, forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.Stats()), "Expect no stats")
	forwarder, err := NewForwarder("127.0.0.1", nettest.TCPPort(l.psocket), time.Second, []string{"zzfirst"}, 10)
	defer forwarder.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 1, len(forwarder.pool.conns))
	timeToSend := time.Now().Round(time.Second)
	dpSent := dptest.DP()
	dpSent.Timestamp = timeToSend
	log.Info("Sending a dp")
	forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent})
	log.Info("Looking for DP back")
	dpSeen := forwardTo.Next()

	assert.Equal(t, "randtest."+dpSent.Metric, dpSeen.Metric, "Expect metric back")
	assert.Equal(t, dpSent.Timestamp, dpSeen.Timestamp, "Expect metric back")

	// Test creating a new connection if pool is empty
	for forwarder.pool.Get() != nil {
	}

	forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent})
	dpSeen = forwardTo.Next()
	assert.Equal(t, "randtest."+dpSent.Metric, dpSeen.Metric, "Expect metric back")
	assert.Equal(t, dpSent.Timestamp, dpSeen.Timestamp, "Expect metric back")
	//
	// Test creation error if pool is empty
	for forwarder.pool.Get() != nil {
	}

	forwarder.dialer = func(network, address string, timeout time.Duration) (net.Conn, error) {
		return nil, errors.New("nope")
	}
	assert.Error(t, forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSeen}))

	forwarder.dialer = net.DialTimeout
	assert.NoError(t, forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSeen}), "Should get the conn back")

}

func TestDeadlineError(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")

	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := ListenerLoader(ctx, forwardTo, &listenFrom)
	defer l.Close()
	forwarder, err := NewForwarder("127.0.0.1", nettest.TCPPort(l.psocket), time.Second, []string{}, 10)
	assert.Equal(t, nil, err, "Expect no error")

	forwarder.dialer = func(network, address string, timeout time.Duration) (net.Conn, error) {
		mockConn := mockConn{}
		mockConn.deadlineReturn = errors.New("deadline error")
		return &mockConn, nil
	}

	for forwarder.pool.Get() != nil {
	}

	dpSent := dptest.DP()
	assert.Error(t, forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent}))
}

func TestWriteError(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")

	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := ListenerLoader(ctx, forwardTo, &listenFrom)
	defer l.Close()
	forwarder, err := NewForwarder("127.0.0.1", nettest.TCPPort(l.psocket), time.Second, []string{}, 10)
	assert.Equal(t, nil, err, "Expect no error")

	forwarder.dialer = func(network, address string, timeout time.Duration) (net.Conn, error) {
		mockConn := mockConn{}
		mockConn.writeReturn = errors.New("deadline error")
		return &mockConn, nil
	}

	for forwarder.pool.Get() != nil {
	}

	dpSent := dptest.DP()
	assert.Error(t, forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent}))
}

func TestCarbonWrite(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := ListenerLoader(ctx, forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 4, len(l.Stats()), "Expect no stats")
	forwarder, err := NewForwarder("127.0.0.1", nettest.TCPPort(l.psocket), time.Second, []string{"zzfirst"}, 10)
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 1, len(forwarder.pool.conns))
	timeToSend := time.Now().Round(time.Second)
	dpSent := dptest.DP()
	dpSent.Timestamp = timeToSend
	dpSent.Meta[carbonNative] = "abcd 123 123"
	log.Info("Sending a dp")
	forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent})
	log.Info("Looking for DP back")
	dpSeen := forwardTo.Next()

	assert.Equal(t, "abcd", dpSeen.Metric, "Expect metric back")
}

func TestLoader(t *testing.T) {
	listenFrom := config.ListenFrom{}
	listenFrom.ListenAddr = workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0")
	ctx := context.Background()
	forwardTo := dptest.NewBasicSink()
	l, err := ListenerLoader(ctx, forwardTo, &listenFrom)
	port := nettest.TCPPort(l.psocket)

	ft := config.ForwardTo{
		Host: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1"),
		Port: workarounds.GolangDoesnotAllowPointerToUint16Literal(port),
	}
	f, err := ForwarderLoader(context.Background(), &ft)
	assert.NoError(t, err)
	dpSent := dptest.DP()
	dpSent.Dimensions = map[string]string{}
	assert.NoError(t, f.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent}))
	dpSeen := forwardTo.Next()
	assert.Equal(t, dpSent.Metric, dpSeen.Metric)
	assert.Equal(t, 8, len(f.Stats()))
}

func TestNonNil(t *testing.T) {
	e1 := errors.New("nope")
	assert.Equal(t, e1, nonNil(e1, nil))
	assert.Equal(t, e1, nonNil(nil, e1))
	assert.Nil(t, nonNil(nil, nil))
}
