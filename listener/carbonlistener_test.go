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
	"github.com/signalfuse/signalfxproxy/listener/metricdeconstructor"
	"io"
	"net"
	"testing"
	"time"
)

var readerReadBytesObj a.ReaderReadBytesObj

func init() {
	readerReadBytes = readerReadBytesObj.Execute
}

type basicDatapointStreamingAPI struct {
	channel chan core.Datapoint
}

func (api *basicDatapointStreamingAPI) DatapointsChannel() chan<- core.Datapoint {
	return api.channel
}

func (api *basicDatapointStreamingAPI) Name() string {
	return ""
}

func TestCarbonCoverOriginalReaderReadBytes(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader([]byte("test*test")))
	b, err := originalReaderReadBytes(r, '*')
	a.ExpectNil(t, err)
	a.ExpectEquals(t, "test*", string(b), "Did not get test string back")
}

func TestCarbonInvalidCarbonListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:999999"),
	}
	sendTo := &basicDatapointStreamingAPI{}
	_, err := CarbonListenerLoader(sendTo, listenFrom)
	a.ExpectNotEquals(t, nil, err, "Should get an error making")
}

func TestCarbonInvalidCarbonDeconstructorListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr:          workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12247"),
		MetricDeconstructor: workarounds.GolangDoesnotAllowPointerToStringLiteral("UNKNOWN"),
	}
	sendTo := &basicDatapointStreamingAPI{}
	_, err := CarbonListenerLoader(sendTo, listenFrom)
	a.ExpectNotEquals(t, nil, err, "Should get an error making")
}

func TestCarbonListenerLoader(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12245"),
	}
	sendTo := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listener, err := CarbonListenerLoader(sendTo, listenFrom)
	a.ExpectEquals(t, nil, err, "Should be ok to make")
	defer listener.Close()
	a.ExpectEquals(t, 4, len(listener.GetStats()), "Should have no stats")
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
}
func TestCarbonListenerLoader2(t *testing.T) {
	listenFrom := &config.ListenFrom{
		ListenAddr: workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12248"),
	}
	sendTo := &basicDatapointStreamingAPI{
		channel: make(chan core.Datapoint),
	}
	listener, err := CarbonListenerLoader(sendTo, listenFrom)
	a.ExpectEquals(t, nil, err, "Should be ok to make")
	defer listener.Close()
	carbonlistener, _ := listener.(*carbonListener)
	carbonlistener.metricDeconstructor, err = metricdeconstructor.Load("commakeys", "")
	a.ExpectNil(t, err)
	conn, err := net.Dial("tcp", *listenFrom.ListenAddr)
	a.ExpectEquals(t, nil, err, "Should be ok to make")
	buf := bytes.Buffer{}
	fmt.Fprintf(&buf, "a.metric.name[host:bob,type:dev] 3 3")
	_, err = buf.WriteTo(conn)
	conn.Close()
	a.ExpectEquals(t, nil, err, "Should be ok to write")
	datapoint := <-sendTo.channel
	a.ExpectEquals(t, "a.metric.name", datapoint.Metric(), "Should be metric")
	a.ExpectEquals(t, map[string]string{"host": "bob", "type": "dev"}, datapoint.Dimensions(), "Did not parse dimensions")
	i, _ := datapoint.Value().IntValue()
	a.ExpectEquals(t, int64(3), i, "Should get 3")

	carbonlistener.metricDeconstructor, _ = metricdeconstructor.Load("", "")

	func() {
		readerErrorSignal := make(chan bool)
		readerReadBytesObj.UseFunction(func(reader *bufio.Reader, delim byte) ([]byte, error) {
			readerErrorSignal <- true
			return nil, errors.New("error reading from reader")
		})
		defer readerReadBytesObj.Reset()
		conn, err = net.Dial("tcp", *listenFrom.ListenAddr)
		a.ExpectEquals(t, nil, err, "Should be ok to make")
		var buf2 bytes.Buffer
		fmt.Fprintf(&buf2, "ametric 2 2\n")
		_, err = buf2.WriteTo(conn)
		conn.Close()
		_ = <-readerErrorSignal
	}()

	time.Sleep(time.Millisecond)

	func() {
		readerErrorSignal := make(chan bool)
		readerReadBytesObj.UseFunction(func(reader *bufio.Reader, delim byte) ([]byte, error) {
			readerErrorSignal <- true
			return []byte("ametric 3 2\n"), io.EOF
		})
		defer readerReadBytesObj.Reset()
		conn, err = net.Dial("tcp", *listenFrom.ListenAddr)
		a.ExpectEquals(t, nil, err, "Should be ok to make")
		var buf3 bytes.Buffer
		fmt.Fprintf(&buf3, "ametric 3 2\n")
		_, err = buf3.WriteTo(conn)
		conn.Close()
		_ = <-readerErrorSignal
		datapoint = <-sendTo.channel
		i, _ = datapoint.Value().IntValue()
		a.ExpectEquals(t, int64(3), i, "Should get 3")
	}()
}
