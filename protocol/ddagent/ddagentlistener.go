package ddagent

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/dp/dpsink"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/stats"
	"golang.org/x/net/context"
)

var defaultListenerConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:17123"),
	ReforwardHost:   workarounds.GolangDoesnotAllowPointerToStringLiteral("https://app.datadoghq.com"),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("ddagent-listener"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
}

// Listener will listen on a HTTP port pretending to be ddagent's endpoint, forwarding requests to
// some URL and responding with that URL's response while also parsing any metrics and sending them
// to forwarders
type Listener struct {
	reforwardHost    string
	reforwardTimeout time.Duration
	name             string
	ctx              context.Context
	listener         net.Listener
	st               stats.Keeper
	sink             dpsink.Sink
}

// Stats reports internal stats about the various pieces of the listener, such as total points sent
// and outstanding HTTP connections
func (l *Listener) Stats() []*datapoint.Datapoint {
	return l.st.Stats()
}

// Close the listening HTTP socket
func (l *Listener) Close() error {
	return l.listener.Close()
}

// ListenerLoader is the generic loader interface that creates a ddagent listener
func ListenerLoader(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (*Listener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultListenerConfig)
	return NewListener(ctx, sink, *listenFrom.ReforwardHost, *listenFrom.TimeoutDuration, *listenFrom.TimeoutDuration, *listenFrom.Name, *listenFrom.ListenAddr, url.Parse)
}

// Forwarder understands how to deconstruct a ddagent POST and extract datapoints to send to
// a Sink
type Forwarder struct {
	Sink                   dpsink.Sink
	Handler                ForwarderResponseHandler
	errCopyingBuffer       int64
	errDecodingJSON        int64
	errDecodingCompression int64
}

// Stats reports error counts
func (d *Forwarder) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	now := time.Now()
	return []*datapoint.Datapoint{
		datapoint.New("error_copying_buffer", dimensions, datapoint.NewIntValue(atomic.LoadInt64(&d.errCopyingBuffer)), datapoint.Counter, now),
		datapoint.New("error_decoding_json", dimensions, datapoint.NewIntValue(atomic.LoadInt64(&d.errDecodingJSON)), datapoint.Counter, now),
		datapoint.New("error_decoding_compression", dimensions, datapoint.NewIntValue(atomic.LoadInt64(&d.errDecodingCompression)), datapoint.Counter, now),
	}
}

// ForwarderResponseHandler is any type that can forward a response (or ignore it)
type ForwarderResponseHandler interface {
	HandleForwardedResponse(reforwardError error, bodyBuffer io.Reader, rw http.ResponseWriter, req *http.Request)
}

// OkReforwarder dumbly replies 200-OK every time
type OkReforwarder struct{}

// HandleForwardedResponse writes to rw http.StatusOK
func (o *OkReforwarder) HandleForwardedResponse(reforwardError error, bodyBuffer io.Reader, rw http.ResponseWriter, req *http.Request) {
	rw.WriteHeader(http.StatusOK)
}

func decompressBody(body io.Reader, headers http.Header) (io.Reader, error) {
	if headers.Get("Content-Encoding") == "deflate" {
		return zlib.NewReader(body)

	}
	return body, nil
}

// ServeHTTPC ignores the context (currently) and will decompress a ddagent payload from req, send
// metrics to the sink, and ask a Handler to reforward or reply.
func (d *Forwarder) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	bodyBuffer := bytes.Buffer{}
	_, err := io.Copy(&bodyBuffer, req.Body)
	if err != nil {
		atomic.AddInt64(&d.errCopyingBuffer, 1)
		d.Handler.HandleForwardedResponse(err, &bodyBuffer, rw, req)
		return
	}

	payload := ddAgentIntakePayload{}
	var reforwardError error
	decompressReader, err := decompressBody(&bodyBuffer, req.Header)
	if err == nil {
		if err := json.NewDecoder(decompressReader).Decode(&payload); err == nil {
			datapoints := make([]*datapoint.Datapoint, 0, len(payload.Metrics))
			for _, m := range payload.Metrics {
				if dp, err := m.Datapoint(); err == nil {
					datapoints = append(datapoints, dp)
				}
			}
			if len(datapoints) > 0 {
				reforwardError = d.Sink.AddDatapoints(ctx, datapoints)
			}
		} else {
			atomic.AddInt64(&d.errDecodingJSON, 1)
		}
	} else {
		atomic.AddInt64(&d.errDecodingCompression, 1)
	}
	bodyBuffer.Reset()
	d.Handler.HandleForwardedResponse(reforwardError, &bodyBuffer, rw, req)
}

// ResendForwarder resends requests to another host and replies with that host's reply
type ResendForwarder struct {
	client           http.Client
	resendHost       string
	parsedResendHost url.URL
}

type respError struct {
	error
	status int
}

type httpNewRequest func(method, urlStr string, body io.Reader) (*http.Request, error)

func (r *ResendForwarder) forwardReq(bodyBuffer io.Reader, req *http.Request, newReq httpNewRequest) (*http.Response, *respError) {
	forwardURL := *req.URL
	forwardURL.Scheme = r.parsedResendHost.Scheme
	forwardURL.Host = r.parsedResendHost.Host
	forwardReq, err := newReq(req.Method, forwardURL.String(), bodyBuffer)
	if err != nil {
		return nil, &respError{
			error:  err,
			status: http.StatusInternalServerError,
		}
	}
	for k, vals := range map[string][]string(req.Header) {
		for _, val := range vals {
			forwardReq.Header.Add(k, val)
		}
	}
	resp, err := r.client.Do(forwardReq)
	if err == nil {
		return resp, nil
	}
	return nil, &respError{
		error:  err,
		status: http.StatusInternalServerError,
	}
}

// HandleForwardedResponse forwards a req to a new host
func (r *ResendForwarder) HandleForwardedResponse(reforwardError error, bodyBuffer io.Reader, rw http.ResponseWriter, req *http.Request) {
	forwardResp, statusErr := r.forwardReq(bodyBuffer, req, http.NewRequest)
	if statusErr != nil {
		rw.WriteHeader(statusErr.status)
		return
	}
	for k, vals := range map[string][]string(forwardResp.Header) {
		for _, val := range vals {
			rw.Header().Add(k, val)
		}
	}
	rw.WriteHeader(forwardResp.StatusCode)
	io.Copy(rw, forwardResp.Body)
}

type urlParseFunc func(string) (*url.URL, error)

func getReforwarder(reforwardHost string, reforwardTimeout time.Duration, urlParse urlParseFunc) (ForwarderResponseHandler, error) {
	if reforwardHost == "" {
		return &OkReforwarder{}, nil
	}
	client := http.Client{
		Timeout: reforwardTimeout,
	}
	r := ResendForwarder{
		client:     client,
		resendHost: reforwardHost,
	}
	var resendURL *url.URL
	resendURL, err := urlParse(reforwardHost)
	if err != nil {
		return nil, err
	}
	r.parsedResendHost = *resendURL
	return &r, nil
}

// NewListener creates a new ddagent listener
func NewListener(ctx context.Context, sink dpsink.Sink, reforwardHost string, reforwardTimeout time.Duration, clientTimeout time.Duration, name string, listenAddr string, urlParse urlParseFunc) (*Listener, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	r, err := getReforwarder(reforwardHost, reforwardTimeout, urlParse)
	if err != nil {
		return nil, err
	}

	counter := dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, dpsink.NextWrap(&counter))

	agentForwarder := Forwarder{
		Sink:    finalSink,
		Handler: r,
	}

	metricTracking := web.RequestCounter{}
	handler := web.NewHandler(ctx, &agentForwarder).Add(web.NextHTTP(metricTracking.ServeHTTP))

	server := http.Server{
		Handler:      handler,
		Addr:         listenAddr,
		ReadTimeout:  clientTimeout,
		WriteTimeout: clientTimeout,
	}
	l := Listener{
		reforwardHost:    reforwardHost,
		reforwardTimeout: reforwardTimeout,
		name:             name,
		listener:         listener,
		st:               stats.ToKeeperMany(protocol.ListenerDims(name, "ddagent"), &counter, &metricTracking, &agentForwarder),
	}
	go server.Serve(listener)
	return &l, nil
}
