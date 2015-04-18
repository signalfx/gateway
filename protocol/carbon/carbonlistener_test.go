package carbon

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dptest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestCarbonInvalidListenerLoader(t *testing.T) {
	ctx := context.Background()
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:999999"),
	}
	sendTo := dptest.NewBasicSink()
	_, err := ListenerLoader(ctx, sendTo, listenFrom)
	assert.NotEqual(t, nil, err, "Should get an error making")
}

func TestCarbonInvalidCarbonDeconstructorListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr:          workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:12247"),
		MetricDeconstructor: workarounds.GolangDoesnotAllowPointerToStringLiteral("UNKNOWN"),
	}
	ctx := context.Background()
	forwardTo := dptest.NewBasicSink()
	_, err := ListenerLoader(ctx, forwardTo, listenFrom)
	assert.NotEqual(t, nil, err, "Should get an error making")
}

func TestCarbonHandleConnection(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0"),
	}

	ctx := context.Background()
	forwardTo := dptest.NewBasicSink()
	listener, err := ListenerLoader(ctx, forwardTo, listenFrom)
	defer listener.Close()

	listeningDialAddress := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener.psocket))

	conn, err := net.Dial("tcp", listeningDialAddress)
	assert.NoError(t, err)
	conn.Close()
	// Drain for the next read
	conn.Read(make([]byte, 100))
	assert.Error(t, listener.handleConnection(conn))

}

func TestListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr:           workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0"),
		ServerAcceptDeadline: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Millisecond),
	}
	ctx := context.Background()
	forwardTo := dptest.NewBasicSink()
	listener, err := ListenerLoader(ctx, forwardTo, listenFrom)
	defer listener.Close()
	assert.Equal(t, nil, err, "Should be ok to make")
	defer listener.Close()
	listeningDialAddress := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener.psocket))
	assert.Equal(t, 4, len(listener.Stats()), "Should have no stats")
	assert.NotEqual(t, listener, err, "Should be ok to make")

	// Wait for the connection to timeout
	time.Sleep(3 * time.Millisecond)

	conn, err := net.Dial("tcp", listeningDialAddress)
	assert.Equal(t, nil, err, "Should be ok to make")
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %d %d\n\nINVALIDLINE", "ametric", 2, 2)
	_, err = buf.WriteTo(conn)
	conn.Close()
	assert.Equal(t, nil, err, "Should be ok to write")
	dp := forwardTo.Next()
	assert.Equal(t, "ametric", dp.Metric, "Should be metric")
	i := dp.Value.(datapoint.IntValue).Int()
	assert.Equal(t, int64(2), i, "Should get 2")
}
