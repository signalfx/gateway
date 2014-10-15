package listener

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"io"
	"net"
	"testing"
	"time"
)

type basicDatapointStreamingAPI struct {
	channel chan core.Datapoint
}

func (api *basicDatapointStreamingAPI) DatapointsChannel() chan<- core.Datapoint {
	return api.channel
}

func (api *basicDatapointStreamingAPI) Name() string {
	return ""
}

func TestInvalidCarbonListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:1"),
	}
	sendTo := &basicDatapointStreamingAPI{}
	_, err := CarbonListenerLoader(sendTo, listenFrom)
	a.ExpectNotEquals(t, nil, err, "Should get an error making")
}

func TestCarbonListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12346"),
	}
	sendTo := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listener, err := CarbonListenerLoader(sendTo, listenFrom)
	defer listener.Close()
	a.ExpectEquals(t, nil, err, "Should be ok to make")
	a.ExpectEquals(t, 0, len(listener.GetStats()), "Should have no stats")
	a.ExpectNotEquals(t, listener, err, "Should be ok to make")

	// Wait for the connection to timeout
	time.Sleep(2 * time.Second)

	conn, err := net.Dial("tcp", *listenFrom.ListenAddr)
	a.ExpectEquals(t, nil, err, "Should be ok to make")
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %d %d\n\nINVALIDLINE", "ametric", 2, 2)
	_, err = buf.WriteTo(conn)
	conn.Close()
	a.ExpectEquals(t, nil, err, "Should be ok to write")
	datapoint := <-sendTo.channel
	a.ExpectEquals(t, "ametric", datapoint.Metric(), "Should be metric")
	i, _ := datapoint.Value().IntValue()
	a.ExpectEquals(t, int64(2), i, "Should get 2")

	for len(sendTo.channel) > 0 {
		_ = <-sendTo.channel
	}

	prev := readerReadBytes
	readerReadBytes = func(reader *bufio.Reader, delim byte) ([]byte, error) {
		return nil, errors.New("error reading from reader")
	}
	conn, err = net.Dial("tcp", *listenFrom.ListenAddr)
	a.ExpectEquals(t, nil, err, "Should be ok to make")
	var buf2 bytes.Buffer
	fmt.Fprintf(&buf2, "ametric 2 2\n")
	_, err = buf2.WriteTo(conn)
	conn.Close()

	for len(sendTo.channel) > 0 {
		_ = <-sendTo.channel
	}

	time.Sleep(time.Millisecond)

	readerReadBytes = func(reader *bufio.Reader, delim byte) ([]byte, error) { return []byte("ametric 3 2\n"), io.EOF }
	conn, err = net.Dial("tcp", *listenFrom.ListenAddr)
	a.ExpectEquals(t, nil, err, "Should be ok to make")
	var buf3 bytes.Buffer
	fmt.Fprintf(&buf3, "ametric 3 2\n")
	_, err = buf3.WriteTo(conn)
	conn.Close()
	readerReadBytes = prev
	datapoint = <-sendTo.channel
	i, _ = datapoint.Value().IntValue()
	a.ExpectEquals(t, int64(3), i, "Should get 3")

	listener.Close()
	// Wait for the other thread to die
	time.Sleep(2 * time.Second)
}
