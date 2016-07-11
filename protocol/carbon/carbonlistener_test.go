package carbon

import (
	"fmt"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/metricproxy/protocol/filtering"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

var errDeadline = errors.New("nope")

type undeadlineable struct {
	net.Conn
}

func (u *undeadlineable) SetDeadline(t time.Time) error {
	return errDeadline
}

func TestCarbonListenerBadAddr(t *testing.T) {
	Convey("bad listener ports shouldn't be able to accept", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:90090999r"),
		}
		sendTo := dptest.NewBasicSink()
		_, err := NewListener(sendTo, listenFrom)
		So(err, ShouldNotBeNil)
	})
	Convey("bad udp listener ports shouldn't be able to accept", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:90090999r"),
			Protocol:   pointer.String("udp"),
		}
		sendTo := dptest.NewBasicSink()
		_, err := NewListener(sendTo, listenFrom)
		So(err, ShouldNotBeNil)
	})
	Convey("non tcp or udp connections prohibited", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:90090999r"),
			Protocol:   pointer.String("jack"),
		}
		sendTo := dptest.NewBasicSink()
		_, err := NewListener(sendTo, listenFrom)
		So(err, ShouldNotBeNil)
	})
}

func TestCarbonForwarderBadAddr(t *testing.T) {
	Convey("bad forwarder ports shouldn't be able to accept", t, func() {
		listenFrom := &ForwarderConfig{
			Port: pointer.Uint16(123),
		}
		_, err := NewForwarder("", listenFrom)
		So(err, ShouldNotBeNil)
	})
}

func TestCarbonForwarderNormal(t *testing.T) {
	Convey("A do nothing port", t, func() {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		So(err, ShouldBeNil)

		Convey("Invalid regexes should cause an error", func() {
			forwardConfig := &ForwarderConfig{
				Port:    pointer.Uint16(nettest.TCPPort(l)),
				Timeout: pointer.Duration(time.Millisecond * 100),
				Filters: &filtering.FilterObj{
					Allow: []string{"["},
				},
			}
			forwarder, err := NewForwarder("127.0.0.1", forwardConfig)
			So(err, ShouldNotBeNil)
			So(forwarder, ShouldBeNil)
		})

		Convey("With a forwarder", func() {
			forwarderConfig := ForwarderConfig{
				Port:    pointer.Uint16(nettest.TCPPort(l)),
				Timeout: pointer.Duration(time.Millisecond * 100),
				Filters: &filtering.FilterObj{
					Deny: []string{"blarg"},
				},
			}
			forwarder, err := NewForwarder("127.0.0.1", &forwarderConfig)
			So(err, ShouldBeNil)
			ctx := context.Background()
			eventuallyTimesOut := func(ctx context.Context) {
				var err error
				for err == nil {
					dps := []*datapoint.Datapoint{}
					// Make this really big so the OS doesn't try to buffer it in the socket
					for i := 0; i < 10000; i++ {
						dps = append(dps, dptest.DP())
					}
					// Eventually this should timeout b/c of the above context
					err = forwarder.AddDatapoints(ctx, dps)
					time.Sleep(time.Millisecond)
				}

				tailErr := errors.Tail(err).(net.Error)
				So(tailErr.Timeout(), ShouldBeTrue)
			}
			Convey("Should respect ctx cancel", func() {
				ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
				eventuallyTimesOut(ctx)
				cancel()
			})
			Convey("Should respect internal timeout", func() {
				ctx, cancel := context.WithTimeout(ctx, time.Hour*100)
				eventuallyTimesOut(ctx)
				cancel()
			})
			Convey("Should respect deadline fails", func() {
				badConn := &undeadlineable{forwarder.pool.Get()}
				forwarder.pool.Return(badConn)
				So(errors.Tail(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dptest.DP()})), ShouldEqual, errDeadline)
			})
		})

		Reset(func() {
			So(l.Close(), ShouldBeNil)
		})
	})
}

func TestCarbonListenerNormalTCP(t *testing.T) {
	Convey("A normally setup listener", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:0"),
		}
		sendTo := dptest.NewBasicSink()
		listener, err := NewListener(sendTo, listenFrom)
		So(err, ShouldBeNil)
		Convey("should eventually time out idle connections", func() {
			listenFrom.ConnectionTimeout = pointer.Duration(time.Millisecond)
			listenFrom.ServerAcceptDeadline = pointer.Duration(time.Millisecond)
			So(listener.Close(), ShouldBeNil)
			listener, err = NewListener(sendTo, listenFrom)
			So(err, ShouldBeNil)

			connAddr := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener))
			s, err := net.Dial("tcp", connAddr)
			So(err, ShouldBeNil)
			// Wait for the idle timeout
			for atomic.LoadInt64(&listener.stats.idleTimeouts) == 0 {
				time.Sleep(time.Millisecond)
			}
			So(s.Close(), ShouldBeNil)
			dps := listener.Datapoints()
			So(dptest.ExactlyOne(dps, "idle_timeouts").Value.String(), ShouldEqual, "1")

			Convey("and idle in its its own listen loop", func() {
				for atomic.LoadInt64(&listener.stats.retriedListenErrors) == 0 {
					time.Sleep(time.Millisecond)
				}
				dps := listener.Datapoints()
				So(dptest.ExactlyOne(dps, "retry_listen_errors").Value.String(), ShouldNotEqual, "0")
			})
		})
		Convey("should error invalid lines", func() {
			dps := listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_datapoints").Value.String(), ShouldEqual, "0")

			connAddr := fmt.Sprintf("127.0.0.1:%d", nettest.TCPPort(listener))
			s, err := net.Dial("tcp", connAddr)
			So(err, ShouldBeNil)
			_, err = io.WriteString(s, "hello world bob\n")
			So(err, ShouldBeNil)
			So(s.Close(), ShouldBeNil)

			// Wait for the idle timeout
			for atomic.LoadInt64(&listener.stats.invalidDatapoints) == 0 {
				time.Sleep(time.Millisecond)
			}

			dps = listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_datapoints").Value.String(), ShouldEqual, "1")
		})
		Convey("with a forwarder", func() {
			forwarderConfig := ForwarderConfig{
				Port: pointer.Uint16(nettest.TCPPort(listener)),
				Filters: &filtering.FilterObj{
					Deny: []string{"blarg"},
				},
			}
			forwarder, err := NewForwarder("127.0.0.1", &forwarderConfig)
			So(err, ShouldBeNil)
			So(dptest.ExactlyOne(forwarder.Datapoints(), "returned_connections").Value.String(), ShouldEqual, "1")
			ctx := context.Background()

			Convey("all filtered metrics work", func() {
				dp := dptest.DP()
				dp.Metric = "blarg"
				So(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
				So(forwarder.FilteredDatapoints, ShouldEqual, 1)
			})

			trySendingDatpoints := func() {
				dp := dptest.DP()
				dp.Dimensions = nil
				dp.Timestamp = dp.Timestamp.Round(time.Second)
				So(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
				So(sendTo.Next().String(), ShouldEqual, dp.String())
			}

			Convey("and empty connection pool", func() {
				Convey("connections should be remakeable", func() {
					So(forwarder.pool.Close(), ShouldBeNil)
					trySendingDatpoints()
					dps := forwarder.Datapoints()
					So(dptest.ExactlyOne(dps, "reused_connections").Value.String(), ShouldEqual, "0")
					So(dptest.ExactlyOne(dps, "returned_connections").Value.String(), ShouldEqual, "2")
				})
				Convey("connection remake attempt failures should be caught", func() {
					So(forwarder.pool.Close(), ShouldBeNil)
					forwarder.connectionAddress = ""
					So(errors.Details(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dptest.DP()})), ShouldContainSubstring, "cannot dial ")
				})
			})

			Convey("should be able to directly send carbon points", func() {
				dp, err := NewCarbonDatapoint("dice.roll 3 3", listener.metricDeconstructor)
				So(err, ShouldBeNil)
				So(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dp}), ShouldBeNil)
			})

			Convey("should be able to accept datapoints", func() {
				trySendingDatpoints()
				dps := forwarder.Datapoints()
				So(dptest.ExactlyOne(dps, "reused_connections").Value.String(), ShouldEqual, "1")
				So(dptest.ExactlyOne(dps, "returned_connections").Value.String(), ShouldEqual, "2")
			})

			Reset(func() {
				So(forwarder.Close(), ShouldBeNil)
			})
		})
		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
}

func TestCarbonListenerNormalUDP(t *testing.T) {
	Convey("A normally setup listener", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:0"),
			Protocol:   pointer.String("udp"),
		}
		sendTo := dptest.NewBasicSink()
		listener, err := NewListener(sendTo, listenFrom)
		So(err, ShouldBeNil)

		Convey("should error invalid lines", func() {
			dps := listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_datapoints").Value.String(), ShouldEqual, "0")

			connAddr := fmt.Sprintf("127.0.0.1:%d", (uint16)(listener.Addr().(*net.UDPAddr).Port))
			s, err := net.Dial("udp", connAddr)
			So(err, ShouldBeNil)
			_, err = io.WriteString(s, "hello world bob\n")
			So(err, ShouldBeNil)
			So(s.Close(), ShouldBeNil)

			// Wait for the idle timeout
			for atomic.LoadInt64(&listener.stats.invalidDatapoints) == 0 {
				time.Sleep(time.Millisecond)
			}

			dps = listener.Datapoints()
			So(dptest.ExactlyOne(dps, "invalid_datapoints").Value.String(), ShouldEqual, "1")
		})
		Convey("should eventually time out idle connections", func() {
			listenFrom.ConnectionTimeout = pointer.Duration(time.Millisecond)
			listenFrom.ServerAcceptDeadline = pointer.Duration(time.Millisecond)
			So(listener.Close(), ShouldBeNil)
			listener, err = NewListener(sendTo, listenFrom)
			So(err, ShouldBeNil)

			s, err := net.DialUDP("udp", nil, listener.Addr().(*net.UDPAddr))
			So(err, ShouldBeNil)
			// Wait for the idle timeout
			for atomic.LoadInt64(&listener.stats.idleTimeouts) == 0 {
				time.Sleep(time.Millisecond)
			}
			So(s.Close(), ShouldBeNil)
			dps := listener.Datapoints()
			dptest.ExactlyOne(dps, "idle_timeouts")
		})
		Convey("try sending valid datapoints", func() {
			s, err := net.DialUDP("udp", nil, listener.Addr().(*net.UDPAddr))
			metric := "dice.roll 3 3\n"
			n, err := io.WriteString(s, metric)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, len(metric))
			m := sendTo.Next()
			So(m.Metric, ShouldEqual, "dice.roll")
			So(m.Value.String(), ShouldEqual, "3")
		})
		Convey("try sending datapoint without new line", func() {
			s, err := net.DialUDP("udp", nil, listener.Addr().(*net.UDPAddr))
			metric := "dice.roll 3 3"
			n, err := io.WriteString(s, metric)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, len(metric))
			m := sendTo.Next()
			So(m.Metric, ShouldEqual, "dice.roll")
			So(m.Value.String(), ShouldEqual, "3")
		})
		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
}

func TestInvalidConnection(t *testing.T) {
	Convey("try opening invalid connection", t, func() {
		listenFrom := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:10001"),
			Protocol:   pointer.String("udp"),
		}
		sendTo := dptest.NewBasicSink()
		listener, err := NewListener(sendTo, listenFrom)
		So(err, ShouldBeNil)
		addr := listener.Addr().(*net.UDPAddr)
		So(listener.Close(), ShouldBeNil)
		_, err = net.ListenUDP("udp", addr)
		So(err, ShouldBeNil)
		listener, err = NewListener(sendTo, listenFrom)
		So(err, ShouldNotBeNil)
	})
}
