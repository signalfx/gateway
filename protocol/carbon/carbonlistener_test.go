package carbon

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/golib/datapoint/dptest"
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
	log.Info("START TestCarbonHandleConnection")
	defer log.Info("END   TestCarbonHandleConnection")
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("localhost:0"),
	}

	ctx := context.Background()
	forwardTo := dptest.NewBasicSink()
	listener, err := ListenerLoader(ctx, forwardTo, listenFrom)
	defer listener.Close()

	listeningDialAddress := fmt.Sprintf("localhost:%d", nettest.TCPPort(listener.psocket))

	conn, err := net.Dial("tcp", listeningDialAddress)
	assert.NoError(t, err)
	conn.Close()
	assert.Error(t, listener.handleConnection(conn))

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
	assert.Equal(t, numStats, len(listener.Stats()), "Should have no stats")
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
