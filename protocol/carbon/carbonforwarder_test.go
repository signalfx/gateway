package carbon

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/nettest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"github.com/signalfx/golib/pointer"
)

const numStats = 10

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
	_, err := NewForwarder("", nil)
	assert.Error(t, err)
}

func TestInvalidPort(t *testing.T) {

	ft := ForwarderConfig{
		Port: pointer.Uint16(1),
	}
	_, err := NewForwarder("test", &ft)
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
	listenFrom := ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:0"),
	}
	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := NewListener(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 3, len(l.Datapoints()), "Expect no stats")

	forwarderConfig := ForwarderConfig{
		Port: pointer.Uint16(nettest.TCPPort(l.psocket)),
	}
	forwarder, err := NewForwarder("127.0.0.1", &forwarderConfig)
	defer forwarder.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 1, len(forwarder.pool.conns))
	timeToSend := time.Now().Round(time.Second)
	dpSent := dptest.DP()
	dpSent.Timestamp = timeToSend
	forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent})
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
	listenFrom := ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:0"),
	}

	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := NewListener(forwardTo, &listenFrom)
	defer l.Close()
	forwarderConfig := ForwarderConfig{
		Port: pointer.Uint16(nettest.TCPPort(l.psocket)),
	}
	forwarder, err := NewForwarder("127.0.0.1", &forwarderConfig)
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
	listenFrom := ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:0"),
	}

	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := NewListener(forwardTo, &listenFrom)
	defer l.Close()
	forwarderConfig := ForwarderConfig{
		Port: pointer.Uint16(nettest.TCPPort(l.psocket)),
	}
	forwarder, err := NewForwarder("127.0.0.1", &forwarderConfig)
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
	listenFrom := ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:0"),
	}
	forwardTo := dptest.NewBasicSink()
	ctx := context.Background()
	l, err := NewListener(forwardTo, &listenFrom)
	defer l.Close()
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 3, len(l.Datapoints()), "Expect no stats")
	forwarderConfig := ForwarderConfig{
		Port: pointer.Uint16(nettest.TCPPort(l.psocket)),
	}
	forwarder, err := NewForwarder("127.0.0.1", &forwarderConfig)
	assert.Equal(t, nil, err, "Expect no error")
	assert.Equal(t, 1, len(forwarder.pool.conns))
	timeToSend := time.Now().Round(time.Second)
	dpSent := dptest.DP()
	dpSent.Timestamp = timeToSend
	dpSent.Meta[carbonNative] = "abcd 123 123"
	forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent})
	dpSeen := forwardTo.Next()

	assert.Equal(t, "abcd", dpSeen.Metric, "Expect metric back")
}

func TestLoader(t *testing.T) {
	listenFrom := ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:0"),
	}
	ctx := context.Background()
	forwardTo := dptest.NewBasicSink()
	l, err := NewListener(forwardTo, &listenFrom)

	forwarderConfig := ForwarderConfig{
		Port: pointer.Uint16(nettest.TCPPort(l.psocket)),
	}
	f, err := NewForwarder("127.0.0.1", &forwarderConfig)
	assert.NoError(t, err)
	dpSent := dptest.DP()
	dpSent.Dimensions = map[string]string{}
	assert.NoError(t, f.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent}))
	dpSeen := forwardTo.Next()
	assert.Equal(t, dpSent.Metric, dpSeen.Metric)
}
