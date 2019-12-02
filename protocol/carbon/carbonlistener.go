package carbon

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"sync"

	"bytes"
	"context"
	"fmt"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/gateway/protocol"
	"github.com/signalfx/gateway/protocol/carbon/metricdeconstructor"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
)

// Listener once setup will listen for carbon protocol points to forward on
type Listener struct {
	protocol.CloseableHealthCheck
	psocket              net.Listener
	udpsocket            *net.UDPConn
	sink                 dpsink.Sink
	metricDeconstructor  metricdeconstructor.MetricDeconstructor
	serverAcceptDeadline time.Duration
	connectionTimeout    time.Duration
	listenfunc           func()
	logger               log.Logger
	stats                listenerStats
	wg                   sync.WaitGroup
}

var _ protocol.Listener = &Listener{}

type listenerStats struct {
	totalDatapoints     int64
	idleTimeouts        int64
	retriedListenErrors int64
	totalEOFCloses      int64
	invalidDatapoints   int64
	totalConnections    int64
	activeConnections   int64
}

// DebugDatapoints returns datapoints that are used for debugging the listener
func (listener *Listener) DebugDatapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("invalid_datapoints", nil, atomic.LoadInt64(&listener.stats.invalidDatapoints)),
		sfxclient.Cumulative("total_connections", nil, atomic.LoadInt64(&listener.stats.totalConnections)),
		sfxclient.Gauge("active_connections", nil, atomic.LoadInt64(&listener.stats.activeConnections)),
		sfxclient.Cumulative("idle_timeouts", nil, atomic.LoadInt64(&listener.stats.idleTimeouts)),
		sfxclient.Cumulative("retry_listen_errors", nil, atomic.LoadInt64(&listener.stats.retriedListenErrors)),
	}
}

// DefaultDatapoints returns datapoints that should always be reported from the listener
func (listener *Listener) DefaultDatapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{}
}

// Datapoints reports information about the total points seen by carbon
func (listener *Listener) Datapoints() []*datapoint.Datapoint {
	return append(listener.DebugDatapoints(), listener.DefaultDatapoints()...)
}

// Close the exposed carbon port
func (listener *Listener) Close() error {
	var err error
	if listener.psocket != nil {
		err = listener.psocket.Close()
	}
	if listener.udpsocket != nil {
		err = listener.udpsocket.Close()
	}

	listener.wg.Wait()
	return err
}

type carbonListenConn interface {
	io.Reader
	Close() error
	SetDeadline(t time.Time) error
	RemoteAddr() net.Addr
}

func (listener *Listener) handleUDPConnection(ctx context.Context, addr *net.UDPAddr, data []byte) error {
	connLogger := log.NewContext(listener.logger).With(logkey.RemoteAddr, addr)
	atomic.AddInt64(&listener.stats.totalConnections, 1)
	atomic.AddInt64(&listener.stats.activeConnections, 1)
	defer atomic.AddInt64(&listener.stats.activeConnections, -1)
	for {
		buf := bytes.NewBuffer(data)
		for {
			bytes, err := buf.ReadBytes((byte)('\n'))
			if err == io.EOF {
				atomic.AddInt64(&listener.stats.totalEOFCloses, 1)
				if len(bytes) == 0 {
					return nil
				}
			}
			line := strings.TrimSpace(string(bytes))
			if line != "" {
				dp, err := NewCarbonDatapoint(line, listener.metricDeconstructor)

				if err != nil {
					atomic.AddInt64(&listener.stats.invalidDatapoints, 1)
					connLogger.Log(logkey.CarbonLine, line, log.Err, err, "Received data on a carbon udp port, but it doesn't look like carbon data")
					continue
				}

				if dp == nil {
					continue
				}

				log.IfErr(connLogger, listener.sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp}))
				atomic.AddInt64(&listener.stats.totalDatapoints, 1)
			}
		}

	}
}

func (listener *Listener) handleTCPConnection(ctx context.Context, conn carbonListenConn) error {
	connLogger := log.NewContext(listener.logger).With(logkey.RemoteAddr, conn.RemoteAddr())
	defer func() {
		log.IfErr(connLogger, conn.Close())
	}()
	reader := bufio.NewReader(conn)
	atomic.AddInt64(&listener.stats.totalConnections, 1)
	atomic.AddInt64(&listener.stats.activeConnections, 1)
	defer atomic.AddInt64(&listener.stats.activeConnections, -1)
	for {
		log.IfErr(connLogger, conn.SetDeadline(time.Now().Add(listener.connectionTimeout)))
		bytes, err := reader.ReadBytes((byte)('\n'))
		if err != nil && err != io.EOF {
			atomic.AddInt64(&listener.stats.idleTimeouts, 1)
			connLogger.Log(log.Err, err, "Listening for carbon data returned an error (Note: We timeout idle connections)")
			return err
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp, err := NewCarbonDatapoint(line, listener.metricDeconstructor)
			if err != nil {
				atomic.AddInt64(&listener.stats.invalidDatapoints, 1)
				connLogger.Log(logkey.CarbonLine, line, log.Err, err, "Received data on a carbon port, but it doesn't look like carbon data")
				continue
			}
			if dp == nil {
				continue
			}
			log.IfErr(connLogger, listener.sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp}))
			atomic.AddInt64(&listener.stats.totalDatapoints, 1)
		}

		if err == io.EOF {
			atomic.AddInt64(&listener.stats.totalEOFCloses, 1)
			return nil
		}
	}
}

func (listener *Listener) startListeningUDP() {
	defer listener.wg.Done()
	defer listener.logger.Log("Stop listening carbon UDP")
	buf := make([]byte, 65507) // max size for udp packet body
	for {
		log.IfErr(listener.logger, listener.udpsocket.SetDeadline(time.Now().Add(listener.connectionTimeout)))
		n, addr, err := listener.udpsocket.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() {
					atomic.AddInt64(&listener.stats.idleTimeouts, 1)
					continue
				}
			}
			listener.logger.Log(log.Err, err, "Unable to accept a udp socket connection")
			return
		}
		if n != 0 {
			go func() {
				log.IfErr(listener.logger, listener.handleUDPConnection(context.Background(), addr, buf[:n]))
			}()
		}
	}
}

func (listener *Listener) startListeningTCP() {
	defer listener.wg.Done()
	defer listener.logger.Log("Stop listening carbon TCP")
	for {
		deadlineable, ok := listener.psocket.(*net.TCPListener)
		if ok {
			log.IfErr(listener.logger, deadlineable.SetDeadline(time.Now().Add(listener.serverAcceptDeadline)))
		}
		conn, err := listener.psocket.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				if netErr.Timeout() || netErr.Temporary() {
					atomic.AddInt64(&listener.stats.retriedListenErrors, 1)
					continue
				}
			}
			listener.logger.Log(log.Err, err, "Unable to accept a socket connection")
			return
		}
		go func() {
			log.IfErr(listener.logger, listener.handleTCPConnection(context.Background(), conn))
		}()
	}
}

// Constants for udp and tcp config
const (
	TCP = "tcp"
	UDP = "udp"
)

// ListenerConfig controls optional parameters for carbon listeners
type ListenerConfig struct {
	ServerAcceptDeadline *time.Duration
	ConnectionTimeout    *time.Duration
	ListenAddr           *string
	MetricDeconstructor  metricdeconstructor.MetricDeconstructor
	Logger               log.Logger
	Protocol             *string
}

var defaultListenerConfig = &ListenerConfig{
	ServerAcceptDeadline: pointer.Duration(time.Second),
	ConnectionTimeout:    pointer.Duration(time.Second * 30),
	ListenAddr:           pointer.String("127.0.0.1:2003"),
	MetricDeconstructor:  &metricdeconstructor.IdentityMetricDeconstructor{},
	Protocol:             pointer.String(TCP),
}

// Addr returns the listening address of this carbon listener
func (listener *Listener) Addr() net.Addr {
	if listener.psocket != nil {
		return listener.psocket.Addr()
	}
	return listener.udpsocket.LocalAddr()
}

func (listener *Listener) getServer(conf *ListenerConfig) error {
	loweredProtocol := strings.ToLower(*conf.Protocol)
	if loweredProtocol == UDP {
		serverAddr, err := net.ResolveUDPAddr(UDP, *conf.ListenAddr)
		if err != nil {
			return errors.Annotatef(err, "cannot listen to addr %s", *conf.ListenAddr)
		}
		server, err := net.ListenUDP(UDP, serverAddr)
		if err != nil {
			return errors.Annotatef(err, "cannot listen to addr %s", *conf.ListenAddr)
		}
		listener.udpsocket = server
		listener.listenfunc = listener.startListeningUDP
	} else if loweredProtocol == TCP {
		server, err := net.Listen(TCP, *conf.ListenAddr)
		if err != nil {
			return errors.Annotatef(err, "cannot listen to addr %s", *conf.ListenAddr)
		}
		listener.psocket = server
		listener.listenfunc = listener.startListeningTCP
	} else {
		return fmt.Errorf("specified protocol '%s' not recognized. '%s' or '%s' only please", *conf.Protocol, UDP, TCP)
	}
	return nil
}

// NewListener creates a new listener for carbon datapoints
func NewListener(sendTo dpsink.Sink, passedConf *ListenerConfig) (*Listener, error) {
	conf := pointer.FillDefaultFrom(passedConf, defaultListenerConfig).(*ListenerConfig)
	receiver := Listener{
		sink:                 sendTo,
		metricDeconstructor:  conf.MetricDeconstructor,
		serverAcceptDeadline: *conf.ServerAcceptDeadline,
		connectionTimeout:    *conf.ConnectionTimeout,
		logger:               log.NewContext(conf.Logger).With(logkey.Protocol, "carbon", logkey.Direction, "listener"),
	}
	err := receiver.getServer(conf)
	if err != nil {
		return nil, err
	}
	receiver.wg.Add(1)
	go receiver.listenfunc()
	return &receiver, nil
}
