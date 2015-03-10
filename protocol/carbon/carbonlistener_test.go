package carbon

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dptest"
	"github.com/signalfx/metricproxy/nettest"
	"github.com/stretchr/testify/assert"
)

func TestCarbonInvalidListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:999999"),
	}
	sendTo := &dptest.BasicStreamer{}
	_, err := ListenerLoader(sendTo, listenFrom)
	assert.NotEqual(t, nil, err, "Should get an error making")
}

func TestCarbonInvalidCarbonDeconstructorListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr:          workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:12247"),
		MetricDeconstructor: workarounds.GolangDoesnotAllowPointerToStringLiteral("UNKNOWN"),
	}
	sendTo := &dptest.BasicStreamer{}
	_, err := ListenerLoader(sendTo, listenFrom)
	assert.NotEqual(t, nil, err, "Should get an error making")
}

func TestCarbonHandleConnection(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0"),
	}
	sendTo := &dptest.BasicStreamer{
		Chan: make(chan datapoint.Datapoint),
	}
	listener, err := ListenerLoader(sendTo, listenFrom)
	defer listener.Close()

	listeningDialAddress := fmt.Sprintf("127.0.0.1:%d", nettest.TcpPort(listener.(*carbonListener).psocket))

	conn, err := net.Dial("tcp", listeningDialAddress)
	assert.NoError(t, err)
	conn.Close()
	assert.Error(t, listener.(*carbonListener).handleConnection(conn))

}

func TestListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr:           workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0"),
		ServerAcceptDeadline: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Millisecond),
	}
	sendTo := &dptest.BasicStreamer{
		Chan: make(chan datapoint.Datapoint),
	}
	listener, err := ListenerLoader(sendTo, listenFrom)
	assert.Equal(t, nil, err, "Should be ok to make")
	defer listener.Close()
	listeningDialAddress := fmt.Sprintf("127.0.0.1:%d", nettest.TcpPort(listener.(*carbonListener).psocket))
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
	dp := <-sendTo.Chan
	assert.Equal(t, "ametric", dp.Metric(), "Should be metric")
	i := dp.Value().(datapoint.IntValue).Int()
	assert.Equal(t, int64(2), i, "Should get 2")

	for len(sendTo.Chan) > 0 {
		_ = <-sendTo.Chan
	}
}

func BenchmarkCarbonListening(b *testing.B) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:0"),
	}
	bytesToSend := []byte("ametric 123 1234\n")
	sendTo := &dptest.BasicStreamer{
		Chan: make(chan datapoint.Datapoint, 10000),
	}
	listener, err := ListenerLoader(sendTo, listenFrom)
	if err != nil {
		b.Fatal(err)
	}
	carbonListener := listener.(*carbonListener)
	defer listener.Close()

	conn, err := net.Dial(carbonListener.psocket.Addr().Network(), carbonListener.psocket.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	doneReadingPoints := make(chan bool)

	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			dp := <-sendTo.Chan
			if dp.Metric() != "ametric" {
				b.Fatalf("Invalid metric %s", dp.Metric())
			}
		}
		doneReadingPoints <- true
	}()

	n := int64(0)
	for i := 0; i < b.N; i++ {
		n += int64(len(bytesToSend))
		_, err = bytes.NewBuffer(bytesToSend).WriteTo(conn)
	}
	_ = <-doneReadingPoints
	b.SetBytes(n)
}
