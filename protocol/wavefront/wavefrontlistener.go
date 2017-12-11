package wavefront

import (
	"bufio"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/collectd"
	"golang.org/x/net/context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

// Listener once setup will listen for wavefront protocol points to forward on
type Listener struct {
	protocol.CloseableHealthCheck
	psocket                   net.Listener
	sink                      dpsink.Sink
	serverAcceptDeadline      time.Duration
	connectionTimeout         time.Duration
	listenfunc                func()
	logger                    log.Logger
	stats                     listenerStats
	wg                        sync.WaitGroup
	extractCollectdDimensions bool
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

// Datapoints reports information about the total points seen by wavefront
func (listener *Listener) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("invalid_datapoints", nil, atomic.LoadInt64(&listener.stats.invalidDatapoints)),
		sfxclient.Cumulative("total_connections", nil, atomic.LoadInt64(&listener.stats.totalConnections)),
		sfxclient.Gauge("active_connections", nil, atomic.LoadInt64(&listener.stats.activeConnections)),
		sfxclient.Cumulative("idle_timeouts", nil, atomic.LoadInt64(&listener.stats.idleTimeouts)),
		sfxclient.Cumulative("retry_listen_errors", nil, atomic.LoadInt64(&listener.stats.retriedListenErrors)),
	}
}

// Close the exposed wavefront port
func (listener *Listener) Close() error {
	var err error
	if listener.psocket != nil {
		err = listener.psocket.Close()
	}

	listener.wg.Wait()
	return err
}

type wavefrontListenConn interface {
	io.Reader
	Close() error
	SetDeadline(t time.Time) error
	RemoteAddr() net.Addr
}

// TODO need to run this and prometheus through some benchmarks for allocations
func extractCollectdDimensions(doit bool, metricName string) (string, map[string]string) {
	dimensions := make(map[string]string)
	if !doit {
		return metricName, dimensions
	}
	index := strings.Index(metricName, "..")
	var toAddDims map[string]string
	for {
		metricName, toAddDims = collectd.GetDimensionsFromName(&metricName)
		if len(toAddDims) == 0 {
			// we may have put two dots next to each other extracting this stuff, see
			// if there were two beforehand and if not, replace them if they occur with
			// a single dot
			// TODO could do better here if wavefront users prefixed all metrics with
			// collectd, we could know how they're constructed and could make them be
			// almost as good as coming from collectd.
			// could even be crazy and put the types.db file in here and provide
			// accurate metric types... overkill?
			if index == -1 {
				metricName = strings.Replace(metricName, "..", ".", -1)
			}

			return metricName, dimensions
		}
		for k, v := range toAddDims {
			dimensions[k] = v
		}
	}
}

// i always wonder about passing strings around and if it's worth it to use thier address
// TODO write a benchmark to test performance and garbage generation here
func stripQuotes(s string) string {
	if s[0] == '"' {
		s = s[1:]
	}
	if s[len(s)-1] == '"' {
		s = s[:len(s)-1]
	}
	return s
}

func getMetricPieces(line string) []string {
	lastQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
			return false
		default:
			return unicode.IsSpace(c)

		}
	}

	return strings.FieldsFunc(line, f)
}

// https://docs.wavefront.com/wavefront_data_format.html
//<metricName> <metricValue> [<timestamp>] source=<source> [pointTags]
func (listener *Listener) fromWavefrontDatapoint(line string) *datapoint.Datapoint {
	pieces := getMetricPieces(line)
	if len(pieces) < 3 {
		return nil
	}
	metricName, dimensions := extractCollectdDimensions(listener.extractCollectdDimensions, stripQuotes(pieces[0]))

	valueString := pieces[1]

	var value datapoint.Value
	if i, err := strconv.ParseInt(valueString, 10, 64); err == nil {
		value = datapoint.NewIntValue(i)
	} else if f, err := strconv.ParseFloat(valueString, 64); err == nil {
		value = datapoint.NewFloatValue(f)
	} else {
		return nil
	}
	var timestamp time.Time
	epoch, err := strconv.ParseInt(pieces[2], 10, 64)
	if err != nil {
		// probably not the timestamp, probably a dimension
		pieces = pieces[2:]
		timestamp = time.Now()
	} else {
		timestamp = time.Unix(epoch, 0)
		pieces = pieces[3:]
	}
	for _, p := range pieces {
		dimPieces := strings.SplitN(p, "=", 2)
		if len(dimPieces) == 2 {
			key := dimPieces[0]
			value := stripQuotes(dimPieces[1])
			dimensions[key] = value
		}
		// ignore malformed dimensions
	}
	return datapoint.New(metricName, dimensions, value, datapoint.Gauge, timestamp)
}

var errInvalidDatapoint = errors.New("invalid wavefront datapoint")

func (listener *Listener) handleTCPConnection(ctx context.Context, conn wavefrontListenConn) error {
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
			connLogger.Log(log.Err, err, "Listening for wavefront data returned an error (Note: We timeout idle connections)")
			return err
		}
		line := strings.TrimSpace(string(bytes))
		if line != "" {
			dp := listener.fromWavefrontDatapoint(line)
			if dp == nil {
				atomic.AddInt64(&listener.stats.invalidDatapoints, 1)
				connLogger.Log(logkey.WavefrontLine, line, log.Err, err, "Received data on a wavefront port, but it doesn't look like wavefront data")
				return errInvalidDatapoint
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

func (listener *Listener) startListeningTCP() {
	defer listener.wg.Done()
	defer listener.logger.Log("Stop listening wavefront TCP")
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

// ListenerConfig controls optional parameters for wavefront listeners
type ListenerConfig struct {
	ServerAcceptDeadline      *time.Duration
	ConnectionTimeout         *time.Duration
	ListenAddr                *string
	Logger                    log.Logger
	ExtractCollectdDimensions *bool
}

var defaultListenerConfig = &ListenerConfig{
	ServerAcceptDeadline:      pointer.Duration(time.Second),
	ConnectionTimeout:         pointer.Duration(time.Second * 30),
	ListenAddr:                pointer.String("127.0.0.1:2003"),
	ExtractCollectdDimensions: pointer.Bool(true),
}

// Addr returns the listening address of this wavefront listener
func (listener *Listener) Addr() net.Addr {
	return listener.psocket.Addr()
}

func (listener *Listener) getServer(conf *ListenerConfig) error {
	server, err := net.Listen("tcp", *conf.ListenAddr)
	if err != nil {
		return errors.Annotatef(err, "cannot listen to addr %s", *conf.ListenAddr)
	}
	listener.psocket = server
	listener.listenfunc = listener.startListeningTCP
	return nil
}

// NewListener creates a new listener for wavefront datapoints
func NewListener(sendTo dpsink.Sink, passedConf *ListenerConfig) (*Listener, error) {
	conf := pointer.FillDefaultFrom(passedConf, defaultListenerConfig).(*ListenerConfig)
	receiver := Listener{
		sink:                 sendTo,
		serverAcceptDeadline: *conf.ServerAcceptDeadline,
		connectionTimeout:    *conf.ConnectionTimeout,
		logger:               log.NewContext(conf.Logger).With(logkey.Protocol, "wavefront", logkey.Direction, "listener"),
		extractCollectdDimensions: *conf.ExtractCollectdDimensions,
	}
	err := receiver.getServer(conf)
	if err != nil {
		return nil, err
	}
	receiver.wg.Add(1)
	go receiver.listenfunc()
	return &receiver, nil
}
