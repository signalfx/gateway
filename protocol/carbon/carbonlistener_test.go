package carbon

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/nettest"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestCarbonInvalidListenerLoader(t *testing.T) {
	listenFrom := &ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:999999999"),
	}
	sendTo := dptest.NewBasicSink()
	_, err := NewListener(sendTo, listenFrom)
	assert.NotEqual(t, nil, err, "Should get an error making")
}

func TestCarbonHandleConnection(t *testing.T) {
	listenFrom := &ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:0"),
	}
	ctx := context.Background()
	forwardTo := dptest.NewBasicSink()
	listener, err := NewListener(forwardTo, listenFrom)
	assert.NoError(t, err)
	defer listener.Close()

	listeningDialAddress := fmt.Sprintf("localhost:%d", nettest.TCPPort(listener.psocket))

	conn, err := net.Dial("tcp", listeningDialAddress)
	assert.NoError(t, err)
	conn.Close()
	assert.Error(t, listener.handleConnection(ctx, conn))

	conn, err = net.Dial("tcp", listeningDialAddress)
	assert.NoError(t, err)
	waitChan := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond * 10)
		assert.NoError(t, conn.Close())
		close(waitChan)
	}()
	<-waitChan

	for atomic.LoadInt64(&listener.stats.totalEOFCloses) == 0 {
		time.Sleep(time.Millisecond)
	}
}

func TestListenerLoader(t *testing.T) {
	listenFrom := &ListenerConfig{
		ListenAddr: pointer.String("127.0.0.1:0"),
		ServerAcceptDeadline: pointer.Duration(time.Millisecond),
	}
	forwardTo := dptest.NewBasicSink()
	listener, err := NewListener(forwardTo, listenFrom)
	defer listener.Close()
	assert.Equal(t, nil, err, "Should be ok to make")
	defer listener.Close()
	listeningDialAddress := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener.psocket))
	assert.NoError(t, err, "Should be ok to make")

	conn, err := net.Dial("tcp", listeningDialAddress)
	assert.NoError(t, err, "Should be ok to make")
	assert.Equal(t, int64(0), listener.stats.invalidDatapoints)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %d %d\n\nINVALIDLINE", "ametric", 2, 2)
	_, err = buf.WriteTo(conn)
	conn.Close()
	assert.Equal(t, nil, err, "Should be ok to write")
	dp := forwardTo.Next()
	assert.Equal(t, "ametric", dp.Metric, "Should be metric")
	i := dp.Value.(datapoint.IntValue).Int()
	assert.Equal(t, int64(2), i, "Should get 2")

	for atomic.LoadInt64(&listener.stats.retriedListenErrors) == 0 {
		time.Sleep(time.Millisecond)
	}
	assert.Equal(t, int64(1), atomic.LoadInt64(&listener.stats.invalidDatapoints))
}
