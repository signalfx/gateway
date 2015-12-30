package signalfx

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/protocol/collectd"
	"golang.org/x/net/context"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/sfxclient"
)

// ListenerServer controls listening on a socket for SignalFx connections
type ListenerServer struct {
	listener net.Listener
	logger   log.Logger

	internalCollectors sfxclient.Collector
	metricHandler metricHandler
}

// Close the exposed socket listening for new connections
func (streamer *ListenerServer) Close() error {
	return streamer.listener.Close()
}

func (streamer *ListenerServer) Datapoints() []*datapoint.Datapoint {
	return streamer.internalCollectors.Datapoints()
}

// MericTypeGetter is an old metric interface that returns the type of a metric name
type MericTypeGetter interface {
	GetMetricTypeFromMap(metricName string) com_signalfx_metrics_protobuf.MetricType
}

// ErrorReader are datapoint streamers that read from a HTTP request and return errors if
// the stream is invalid
type ErrorReader interface {
	Read(ctx context.Context, req *http.Request) error
}

// ErrorTrackerHandler behaves like a http handler, but tracks error returns from a ErrorReader
type ErrorTrackerHandler struct {
	TotalErrors int64
	reader      ErrorReader
}

// Datapoints gets TotalErrors stats
func (e *ErrorTrackerHandler) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Cumulative("total_errors", nil, atomic.LoadInt64(&e.TotalErrors)),
	}
}


// ServeHTTPC will serve the wrapped ErrorReader and return the error (if any) to rw if ErrorReader
// fails
func (e *ErrorTrackerHandler) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, req *http.Request) {
	if err := e.reader.Read(ctx, req); err != nil {
		atomic.AddInt64(&e.TotalErrors, 1)
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}
	rw.Write([]byte(`"OK"`))
}

// ProtobufDecoderV1 creates datapoints out of the V1 protobuf definition
type ProtobufDecoderV1 struct {
	Sink       dpsink.DSink
	TypeGetter MericTypeGetter
	Logger     log.Logger
}

var errInvalidProtobuf = errors.New("invalid protocol buffer sent")
var errProtobufTooLarge = errors.New("protobuf structure too large")
var errInvalidProtobufVarint = errors.New("invalid protobuf varint")

func (decoder *ProtobufDecoderV1) Read(ctx context.Context, req *http.Request) error {
	body := req.Body
	bufferedBody := bufio.NewReaderSize(body, 32768)
	for {
		buf, err := bufferedBody.Peek(1)
		if err == io.EOF {
			return nil
		}
		buf, err = bufferedBody.Peek(4) // should be big enough for any varint

		if err != nil {
			decoder.Logger.Log(log.Err, err, "peek error")
			return err
		}
		num, bytesRead := proto.DecodeVarint(buf)
		if bytesRead == 0 {
			// Invalid varint?
			return errInvalidProtobufVarint
		}
		if num > 32768 {
			// Sanity check
			return errProtobufTooLarge
		}
		// Get the varint out
		buf = make([]byte, bytesRead)
		io.ReadFull(bufferedBody, buf)

		// Get the structure out
		buf = make([]byte, num)
		_, err = io.ReadFull(bufferedBody, buf)
		if err != nil {
			return fmt.Errorf("unable to fully read protobuf message: %s", err)
		}
		var msg com_signalfx_metrics_protobuf.DataPoint
		err = proto.Unmarshal(buf, &msg)
		if err != nil {
			return err
		}
		if datapointProtobufIsInvalidForV1(&msg) {
			return errInvalidProtobuf
		}
		mt := decoder.TypeGetter.GetMetricTypeFromMap(msg.GetMetric())
		if dp, err := NewProtobufDataPointWithType(&msg, mt); err == nil {
			decoder.Sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp})
		}
	}
}

func datapointProtobufIsInvalidForV1(msg *com_signalfx_metrics_protobuf.DataPoint) bool {
	return msg.Metric == nil || msg.Value == nil
}

// JSONDecoderV1 creates datapoints out of the v1 JSON definition
type JSONDecoderV1 struct {
	TypeGetter MericTypeGetter
	Sink       dpsink.DSink
	Logger     log.Logger
}

func (decoder *JSONDecoderV1) Read(ctx context.Context, req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	for {
		var d JSONDatapointV1
		if err := dec.Decode(&d); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			if d.Metric == "" {
				continue
			}
			mt := fromMT(decoder.TypeGetter.GetMetricTypeFromMap(d.Metric))
			dp := datapoint.New(d.Metric, map[string]string{"sf_source": d.Source}, datapoint.NewFloatValue(d.Value), mt, time.Now())
			decoder.Sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp})
		}
	}
	return nil
}

// ProtobufDecoderV2 decodes protocol buffers in signalfx's v2 format and sends them to Sink
type ProtobufDecoderV2 struct {
	Sink   dpsink.Sink
	Logger log.Logger
}

var errInvalidContentLength = errors.New("invalid Content-Length")

func (decoder *ProtobufDecoderV2) Read(ctx context.Context, req *http.Request) error {
	if req.ContentLength == -1 {
		return errInvalidContentLength
	}

	// TODO: Source of memory creation.  Maybe pass buf in?
	buf := make([]byte, req.ContentLength)
	readLen, err := io.ReadFull(req.Body, buf)
	if err != nil {
		decoder.Logger.Log(log.Err, err, logkey.ReadLen, readLen, logkey.ContentLength, req.ContentLength, "Unable to fully read from buffer")
		return err
	}
	var msg com_signalfx_metrics_protobuf.DataPointUploadMessage
	err = proto.Unmarshal(buf, &msg)
	if err != nil {
		return err
	}
	dps := make([]*datapoint.Datapoint, 0, len(msg.GetDatapoints()))
	for _, protoDb := range msg.GetDatapoints() {
		if dp, err := NewProtobufDataPointWithType(protoDb, com_signalfx_metrics_protobuf.MetricType_GAUGE); err == nil {
			dps = append(dps, dp)
		}
	}
	return decoder.Sink.AddDatapoints(ctx, dps)
}

// ProtobufEventDecoderV2 decodes protocol buffers in signalfx's v2 format and sends them to Sink
type ProtobufEventDecoderV2 struct {
	Sink   dpsink.ESink
	Logger log.Logger
}

func (decoder *ProtobufEventDecoderV2) Read(ctx context.Context, req *http.Request) error {
	if req.ContentLength == -1 {
		return errInvalidContentLength
	}

	// TODO: Source of memory creation.  Maybe pass buf in?
	buf := make([]byte, req.ContentLength)
	readLen, err := io.ReadFull(req.Body, buf)
	if err != nil {
		decoder.Logger.Log(log.Err, err, logkey.ReadLen, readLen, logkey.ContentLength, req.ContentLength, "Unable to fully read from buffer")
		return err
	}
	var msg com_signalfx_metrics_protobuf.EventUploadMessage
	err = proto.Unmarshal(buf, &msg)
	if err != nil {
		return err
	}
	evts := make([]*event.Event, 0, len(msg.GetEvents()))
	for _, protoDb := range msg.GetEvents() {
		if e, err := NewProtobufEvent(protoDb); err == nil {
			evts = append(evts, e)
		}
	}
	return decoder.Sink.AddEvents(ctx, evts)
}

// JSONDecoderV2 decodes v2 json data for signalfx and sends it to Sink
type JSONDecoderV2 struct {
	Sink   dpsink.Sink
	Logger log.Logger
}

func (decoder *JSONDecoderV2) Read(ctx context.Context, req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	var d JSONDatapointV2
	if err := dec.Decode(&d); err != nil {
		return err
	}
	dps := make([]*datapoint.Datapoint, 0, len(d))
	for metricType, datapoints := range d {
		mt, ok := com_signalfx_metrics_protobuf.MetricType_value[strings.ToUpper(metricType)]
		if !ok {
			decoder.Logger.Log(logkey.MetricType, metricType, "Uknown metric type")
			continue
		}
		for _, jsonDatapoint := range datapoints {
			v, err := ValueToValue(jsonDatapoint.Value)
			if err != nil {
				decoder.Logger.Log(log.Err, err, "Unable to get value for datapoint")
			} else {
				dp := datapoint.New(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, fromMT(com_signalfx_metrics_protobuf.MetricType(mt)), fromTs(jsonDatapoint.Timestamp))
				dps = append(dps, dp)
			}
		}
	}
	if len(dps) == 0 {
		return nil
	}
	return decoder.Sink.AddDatapoints(ctx, dps)
}

// JSONEventDecoderV2 decodes v2 json data for signalfx events and sends it to Sink
type JSONEventDecoderV2 struct {
	Sink   dpsink.ESink
	Logger log.Logger
}

func (decoder *JSONEventDecoderV2) Read(ctx context.Context, req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	var e JSONEventV2
	if err := dec.Decode(&e); err != nil {
		return err
	}
	evts := make([]*event.Event, 0, len(e))
	for _, jsonEvent := range e {
		if jsonEvent.Category == nil {
			jsonEvent.Category = pointer.String("USER_DEFINED")
		}
		if jsonEvent.Timestamp == nil {
			jsonEvent.Timestamp = pointer.Int64(0)
		}
		evt := event.NewWithMeta(jsonEvent.EventType, *jsonEvent.Category, jsonEvent.Dimensions, jsonEvent.Properties, fromTs(*jsonEvent.Timestamp))
		evts = append(evts, evt)
	}
	return decoder.Sink.AddEvents(ctx, evts)
}

type ListenerConfig struct {
	ListenAddr *string
	Timeout *time.Duration
	Logger log.Logger
	RootContext context.Context
	JsonMarshal func(v interface{}) ([]byte, error)
}

var defaultListenerConfig  = &ListenerConfig {
	ListenAddr: pointer.String("127.0.0.1:12345"),
	Timeout: pointer.Duration(time.Second),
	Logger: log.Discard,
	RootContext: context.Background(),
	JsonMarshal: json.Marshal,
}

type metricHandler struct {
	metricCreationsMapMutex sync.Mutex
	metricCreationsMap      map[string]com_signalfx_metrics_protobuf.MetricType
	jsonMarshal             func(v interface{}) ([]byte, error)
	logger                  log.Logger
}

func (handler *metricHandler) ServeHTTP(writter http.ResponseWriter, req *http.Request) {
	dec := json.NewDecoder(req.Body)
	var d []MetricCreationStruct
	if err := dec.Decode(&d); err != nil {
		handler.logger.Log(log.Err, err, "Invalid metric creation request")
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(`{msg:"Invalid creation request"}`))
		return
	}
	handler.metricCreationsMapMutex.Lock()
	defer handler.metricCreationsMapMutex.Unlock()
	ret := []MetricCreationResponse{}
	for _, m := range d {
		metricType, ok := com_signalfx_metrics_protobuf.MetricType_value[m.MetricType]
		if !ok {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Invalid metric type"}`))
			return
		}
		handler.metricCreationsMap[m.MetricName] = com_signalfx_metrics_protobuf.MetricType(metricType)
		ret = append(ret, MetricCreationResponse{Code: 409})
	}
	toWrite, err := handler.jsonMarshal(ret)
	if err != nil {
		handler.logger.Log(log.Err, err, "Unable to marshal json")
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(`{msg:"Unable to marshal json!"}`))
		return
	}
	writter.WriteHeader(http.StatusOK)
	writter.Write([]byte(toWrite))
}

func (handler *metricHandler) GetMetricTypeFromMap(metricName string) com_signalfx_metrics_protobuf.MetricType {
	handler.metricCreationsMapMutex.Lock()
	defer handler.metricCreationsMapMutex.Unlock()
	mt, ok := handler.metricCreationsMap[metricName]
	if !ok {
		return com_signalfx_metrics_protobuf.MetricType_GAUGE
	}
	return mt
}

// StartServingHTTPOnPort servers http requests for Signalfx datapoints
func NewListener(sink dpsink.Sink, conf *ListenerConfig) (*ListenerServer, error) {
	conf = pointer.FillDefaultFrom(conf, defaultListenerConfig).(*ListenerConfig)

	listener, err := net.Listen("tcp", *conf.ListenAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "cannot open listening address %s", *conf.ListenAddr)
	}
	r := mux.NewRouter()

	server := http.Server{
		Handler:      r,
		Addr:         *conf.ListenAddr,
		ReadTimeout:  *conf.Timeout,
		WriteTimeout: *conf.Timeout,
	}
	listenServer := ListenerServer{
		listener: listener,
		logger:   conf.Logger,
		metricHandler : metricHandler{
			metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
			logger:             log.NewContext(conf.Logger).With(logkey.Struct, "metricHandler"),
			jsonMarshal: conf.JsonMarshal,
		},
	}

	r.Handle("/v1/metric", &listenServer.metricHandler)
	r.Handle("/metric", &listenServer.metricHandler)

	listenServer.internalCollectors = sfxclient.NewMultiCollector(
		setupNotFoundHandler(r, conf.RootContext),
		setupProtobufV1(r, conf.RootContext, sink, &listenServer.metricHandler, conf.Logger),
		setupJSONV1(r, conf.RootContext, sink, &listenServer.metricHandler, conf.Logger),
		setupProtobufV2(r, conf.RootContext, sink, conf.Logger),
		setupProtobufEventV2(r, conf.RootContext, sink, conf.Logger),
		setupJSONV2(r, conf.RootContext, sink, conf.Logger),
		setupJSONEventV2(r, conf.RootContext, sink, conf.Logger),
		setupCollectd(r, conf.RootContext, sink),
	)

	go server.Serve(listener)
	return &listenServer, err
}

func setupNotFoundHandler(r *mux.Router, ctx context.Context) sfxclient.Collector {
	metricTracking := web.RequestCounter{}
	r.NotFoundHandler = web.NewHandler(ctx, web.FromHTTP(http.NotFoundHandler())).Add(web.NextHTTP(metricTracking.ServeHTTP))
	return &sfxclient.WithDimensions{
		Dimensions: map[string]string{"type": "http404"},
		Collector: &metricTracking,
	}
}

func setupChain(ctx context.Context, sink dpsink.Sink, chainType string, getReader func(dpsink.Sink) ErrorReader) (*web.Handler, sfxclient.Collector) {
	counter := &dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, dpsink.NextWrap(counter))
	errReader := getReader(finalSink)
	errorTracker := ErrorTrackerHandler{
		reader: errReader,
	}
	metricTracking := web.RequestCounter{}
	handler := web.NewHandler(ctx, &errorTracker).Add(web.NextHTTP(metricTracking.ServeHTTP))
	st := &sfxclient.WithDimensions{
		Collector: sfxclient.NewMultiCollector(
			&metricTracking,
			&errorTracker,
			counter,
		),
		Dimensions: map[string]string {
			"type": "sfx_" + chainType,
		},
	}
	return handler, st
}

func setupProtobufV1(r *mux.Router, ctx context.Context, sink dpsink.Sink, typeGetter MericTypeGetter, logger log.Logger) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "protobuf_v1", func(s dpsink.Sink) ErrorReader {
		return &ProtobufDecoderV1{Sink: s, TypeGetter: typeGetter, Logger: logger}
	})

	SetupProtobufV1Paths(r, handler)
	return st
}

// SetupProtobufV1Paths routes to R paths that should handle V1 Protobuf datapoints
func SetupProtobufV1Paths(r *mux.Router, handler http.Handler) {
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
}

func setupJSONV1(r *mux.Router, ctx context.Context, sink dpsink.Sink, typeGetter MericTypeGetter, logger log.Logger) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "json_v1", func(s dpsink.Sink) ErrorReader {
		return &JSONDecoderV1{Sink: s, TypeGetter: typeGetter, Logger: logger}
	})
	SetupJSONV1Paths(r, handler)

	return st
}

// SetupJSONV1Paths routes to R paths that should handle V1 JSON datapoints
func SetupJSONV1Paths(r *mux.Router, handler http.Handler) {
	SetupJSONByPaths(r, handler, "/datapoint")
	SetupJSONByPaths(r, handler, "/v1/datapoint")
}

func setupProtobufV2(r *mux.Router, ctx context.Context, sink dpsink.Sink, logger log.Logger) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "protobuf_v2", func(s dpsink.Sink) ErrorReader {
		return &ProtobufDecoderV2{Sink: s, Logger: logger}
	})
	SetupProtobufV2DatapointPaths(r, handler)

	return st
}

func setupProtobufEventV2(r *mux.Router, ctx context.Context, sink dpsink.Sink, logger log.Logger) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "protobuf_event_v2", func(s dpsink.Sink) ErrorReader {
		return &ProtobufEventDecoderV2{Sink: s, Logger: logger}
	})
	SetupProtobufV2EventPaths(r, handler)

	return st
}

// SetupProtobufV2DatapointPaths tells the router which paths the given handler (which should handle v2 protobufs)
func SetupProtobufV2DatapointPaths(r *mux.Router, handler http.Handler) {
	SetupProtobufV2ByPaths(r, handler, "/v2/datapoint")
}

// SetupProtobufV2EventPaths tells the router which paths the given handler (which should handle v2 protobufs)
func SetupProtobufV2EventPaths(r *mux.Router, handler http.Handler) {
	SetupProtobufV2ByPaths(r, handler, "/v2/event")
}

// SetupProtobufV2ByPaths tells the router which paths the given handler (which should handle v2 protobufs)
func SetupProtobufV2ByPaths(r *mux.Router, handler http.Handler, path string) {
	r.Path(path).Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
}

func setupJSONV2(r *mux.Router, ctx context.Context, sink dpsink.Sink, logger log.Logger) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "json_v2", func(s dpsink.Sink) ErrorReader {
		return &JSONDecoderV2{Sink: s, Logger: logger}
	})
	SetupJSONV2DatapointPaths(r, handler)
	return st
}

func setupJSONEventV2(r *mux.Router, ctx context.Context, sink dpsink.Sink, logger log.Logger) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "json_event_v2", func(s dpsink.Sink) ErrorReader {
		return &JSONEventDecoderV2{Sink: s, Logger: logger}
	})
	SetupJSONV2EventPaths(r, handler)
	return st
}

// SetupJSONV2DatapointPaths tells the router which paths the given handler (which should handle v2 protobufs)
func SetupJSONV2DatapointPaths(r *mux.Router, handler http.Handler) {
	SetupJSONByPaths(r, handler, "/v2/datapoint")
}

// SetupJSONV2EventPaths tells the router which paths the given handler (which should handle v2 protobufs)
func SetupJSONV2EventPaths(r *mux.Router, handler http.Handler) {
	SetupJSONByPaths(r, handler, "/v2/event")
}

// SetupJSONByPaths tells the router which paths the given handler (which should handle the given
// endpoint) should see
func SetupJSONByPaths(r *mux.Router, handler http.Handler, endpoint string) {
	r.Path(endpoint).Methods("POST").Headers("Content-Type", "application/json").Handler(handler)
	r.Path(endpoint).Methods("POST").Headers("Content-Type", "").HandlerFunc(web.InvalidContentType)
	r.Path(endpoint).Methods("POST").Handler(handler)
}

func setupCollectd(r *mux.Router, ctx context.Context, sink dpsink.Sink) sfxclient.Collector {
	counter := &dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, dpsink.NextWrap(counter))
	decoder := collectd.JSONDecoder {
		SendTo:      finalSink,
	}
	metricTracking := &web.RequestCounter{}
	httpHandler := web.NewHandler(ctx, &decoder).Add(web.NextHTTP(metricTracking.ServeHTTP))
	collectd.SetupCollectdPaths(r, httpHandler, "/v1/collectd")
	return &sfxclient.WithDimensions{
		Collector: sfxclient.NewMultiCollector(
			metricTracking,
			counter,
			&decoder,
		),
		Dimensions: map[string]string {
			"type": "collectd",
		},
	}
}
