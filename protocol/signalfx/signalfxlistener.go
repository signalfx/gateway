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

	"bytes"
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/logkey"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/collectd"
	"github.com/signalfx/metricproxy/protocol/zipper"
	"net/http/httputil"
)

// ListenerServer controls listening on a socket for SignalFx connections
type ListenerServer struct {
	protocol.CloseableHealthCheck
	listener net.Listener
	logger   log.Logger

	internalCollectors sfxclient.Collector
	metricHandler      metricHandler
}

// Close the exposed socket listening for new connections
func (streamer *ListenerServer) Close() error {
	return streamer.listener.Close()
}

// Addr returns the currently listening address
func (streamer *ListenerServer) Addr() net.Addr {
	return streamer.listener.Addr()
}

// Datapoints returns the datapoints about various internal endpoints
func (streamer *ListenerServer) Datapoints() []*datapoint.Datapoint {
	return append(streamer.internalCollectors.Datapoints(), streamer.HealthDatapoints()...)
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
	Logger      log.Logger
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
		_, err = rw.Write([]byte(err.Error()))
		log.IfErr(e.Logger, err)
		return
	}
	_, err := rw.Write([]byte(`"OK"`))
	log.IfErr(e.Logger, err)
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
		_, err := bufferedBody.Peek(1)
		if err == io.EOF {
			return nil
		}
		buf, err := bufferedBody.Peek(4) // should be big enough for any varint

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
		_, err = io.ReadFull(bufferedBody, buf)
		log.IfErr(decoder.Logger, err)

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
			log.IfErr(decoder.Logger, decoder.Sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp}))
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
			log.IfErr(decoder.Logger, decoder.Sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp}))
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

var buffs = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func readFromRequest(jeff *bytes.Buffer, req *http.Request, logger log.Logger) error {
	var r io.Reader
	r = req.Body
	chunked := false
	for _, k := range req.TransferEncoding {
		if k == "chunked" {
			r = httputil.NewChunkedReader(req.Body)
			chunked = true
		}
	}

	if !chunked && req.ContentLength == -1 {
		return errInvalidContentLength
	}

	// for compressed transactions, contentLength isn't trustworthy
	readLen, err := jeff.ReadFrom(r)
	if err != nil {
		logger.Log(log.Err, err, logkey.ReadLen, readLen, logkey.ContentLength, req.ContentLength, "Unable to fully read from buffer")
		return err
	}
	return nil
}

func (decoder *ProtobufDecoderV2) Read(ctx context.Context, req *http.Request) (err error) {
	jeff := buffs.Get().(*bytes.Buffer)
	defer buffs.Put(jeff)
	jeff.Reset()
	if err = readFromRequest(jeff, req, decoder.Logger); err != nil {
		return err
	}
	var msg com_signalfx_metrics_protobuf.DataPointUploadMessage
	if err = proto.Unmarshal(jeff.Bytes(), &msg); err != nil {
		return err
	}
	dps := make([]*datapoint.Datapoint, 0, len(msg.GetDatapoints()))
	for _, protoDb := range msg.GetDatapoints() {
		if dp, err := NewProtobufDataPointWithType(protoDb, com_signalfx_metrics_protobuf.MetricType_GAUGE); err == nil {
			dps = append(dps, dp)
		}
	}
	if len(dps) > 0 {
		err = decoder.Sink.AddDatapoints(ctx, dps)
	}
	return err
}

// ProtobufEventDecoderV2 decodes protocol buffers in signalfx's v2 format and sends them to Sink
type ProtobufEventDecoderV2 struct {
	Sink   dpsink.ESink
	Logger log.Logger
}

func (decoder *ProtobufEventDecoderV2) Read(ctx context.Context, req *http.Request) (err error) {
	jeff := buffs.Get().(*bytes.Buffer)
	defer buffs.Put(jeff)
	jeff.Reset()
	if err = readFromRequest(jeff, req, decoder.Logger); err != nil {
		return err
	}
	var msg com_signalfx_metrics_protobuf.EventUploadMessage
	if err = proto.Unmarshal(jeff.Bytes(), &msg); err != nil {
		return err
	}
	evts := make([]*event.Event, 0, len(msg.GetEvents()))
	for _, protoDb := range msg.GetEvents() {
		if e, err := NewProtobufEvent(protoDb); err == nil {
			evts = append(evts, e)
		}
	}
	if len(evts) > 0 {
		err = decoder.Sink.AddEvents(ctx, evts)
	}
	return err
}

// JSONDecoderV2 decodes v2 json data for signalfx and sends it to Sink
type JSONDecoderV2 struct {
	Sink              dpsink.Sink
	Logger            log.Logger
	unknownMetricType int64
	invalidValue      int64
}

func appendProperties(dp *datapoint.Datapoint, Properties map[string]ValueToSend) {
	for name, p := range Properties {
		v := valueToRaw(p)
		if v == nil {
			continue
		}
		dp.SetProperty(name, p)
	}
}

var errInvalidJSONFormat = errors.New("invalid JSON format; please see correct format at https://developers.signalfx.com/docs/datapoint")

// Datapoints returns datapoints for json decoder v2
func (decoder *JSONDecoderV2) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Counter("dropped_points", map[string]string{"protocol": "sfx_json_v2", "reason": "unknown_metric_type"}, atomic.LoadInt64(&decoder.unknownMetricType)),
		sfxclient.Counter("dropped_points", map[string]string{"protocol": "sfx_json_v2", "reason": "invalid_value"}, atomic.LoadInt64(&decoder.invalidValue)),
	}
}

func (decoder *JSONDecoderV2) Read(ctx context.Context, req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	var d JSONDatapointV2
	if err := dec.Decode(&d); err != nil {
		return errInvalidJSONFormat
	}
	dps := make([]*datapoint.Datapoint, 0, len(d))
	for metricType, datapoints := range d {
		mt, ok := com_signalfx_metrics_protobuf.MetricType_value[strings.ToUpper(metricType)]
		if !ok {
			decoder.Logger.Log(logkey.MetricType, metricType, "Unknown metric type")
			atomic.AddInt64(&decoder.unknownMetricType, int64(len(datapoints)))
			continue
		}
		for _, jsonDatapoint := range datapoints {
			v, err := ValueToValue(jsonDatapoint.Value)
			if err != nil {
				decoder.Logger.Log(log.Err, err, logkey.Struct, jsonDatapoint, "Unable to get value for datapoint")
				atomic.AddInt64(&decoder.invalidValue, 1)
				continue
			}
			dp := datapoint.New(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, fromMT(com_signalfx_metrics_protobuf.MetricType(mt)), fromTs(jsonDatapoint.Timestamp))
			appendProperties(dp, jsonDatapoint.Properties)
			dps = append(dps, dp)
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
		cat := event.USERDEFINED
		if pbcat, ok := com_signalfx_metrics_protobuf.EventCategory_value[*jsonEvent.Category]; ok {
			cat = event.Category(pbcat)
		}
		evt := event.NewWithProperties(jsonEvent.EventType, cat, jsonEvent.Dimensions, jsonEvent.Properties, fromTs(*jsonEvent.Timestamp))
		evts = append(evts, evt)
	}
	return decoder.Sink.AddEvents(ctx, evts)
}

// ListenerConfig controls optional parameters for the listener
type ListenerConfig struct {
	ListenAddr   *string
	HealthCheck  *string
	Timeout      *time.Duration
	Logger       log.Logger
	RootContext  context.Context
	JSONMarshal  func(v interface{}) ([]byte, error)
	DebugContext *web.HeaderCtxFlag
	HTTPChain    web.NextConstructor
}

var defaultListenerConfig = &ListenerConfig{
	ListenAddr:  pointer.String("127.0.0.1:12345"),
	HealthCheck: pointer.String("/healthz"),
	Timeout:     pointer.Duration(time.Second * 30),
	Logger:      log.Discard,
	RootContext: context.Background(),
	JSONMarshal: json.Marshal,
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
		_, err = writter.Write([]byte(`{msg:"Invalid creation request"}`))
		log.IfErr(handler.logger, err)
		return
	}
	handler.metricCreationsMapMutex.Lock()
	defer handler.metricCreationsMapMutex.Unlock()
	ret := []MetricCreationResponse{}
	for _, m := range d {
		metricType, ok := com_signalfx_metrics_protobuf.MetricType_value[m.MetricType]
		if !ok {
			writter.WriteHeader(http.StatusBadRequest)
			_, err := writter.Write([]byte(`{msg:"Invalid metric type"}`))
			log.IfErr(handler.logger, err)
			return
		}
		handler.metricCreationsMap[m.MetricName] = com_signalfx_metrics_protobuf.MetricType(metricType)
		ret = append(ret, MetricCreationResponse{Code: 409})
	}
	toWrite, err := handler.jsonMarshal(ret)
	if err != nil {
		handler.logger.Log(log.Err, err, "Unable to marshal json")
		writter.WriteHeader(http.StatusBadRequest)
		_, err = writter.Write([]byte(`{msg:"Unable to marshal json!"}`))
		log.IfErr(handler.logger, err)
		return
	}
	writter.WriteHeader(http.StatusOK)
	_, err = writter.Write(toWrite)
	log.IfErr(handler.logger, err)
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

// NewListener servers http requests for Signalfx datapoints
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
		metricHandler: metricHandler{
			metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
			logger:             log.NewContext(conf.Logger).With(logkey.Struct, "metricHandler"),
			jsonMarshal:        conf.JSONMarshal,
		},
	}
	listenServer.SetupHealthCheck(conf.HealthCheck, r, conf.Logger)

	r.Handle("/v1/metric", &listenServer.metricHandler)
	r.Handle("/metric", &listenServer.metricHandler)

	listenServer.internalCollectors = sfxclient.NewMultiCollector(
		setupNotFoundHandler(conf.RootContext, r),
		setupProtobufV1(conf.RootContext, r, sink, &listenServer.metricHandler, conf.Logger, conf.HTTPChain),
		setupJSONV1(conf.RootContext, r, sink, &listenServer.metricHandler, conf.Logger, conf.HTTPChain),
		setupProtobufV2(conf.RootContext, r, sink, conf.Logger, conf.DebugContext, conf.HTTPChain),
		setupProtobufEventV2(conf.RootContext, r, sink, conf.Logger, conf.DebugContext, conf.HTTPChain),
		setupJSONV2(conf.RootContext, r, sink, conf.Logger, conf.DebugContext, conf.HTTPChain),
		setupJSONEventV2(conf.RootContext, r, sink, conf.Logger, conf.DebugContext, conf.HTTPChain),
		setupCollectd(conf.RootContext, r, sink, conf.DebugContext, conf.HTTPChain, conf.Logger),
	)

	go func() {
		log.IfErr(conf.Logger, server.Serve(listener))
	}()
	return &listenServer, err
}

func setupNotFoundHandler(ctx context.Context, r *mux.Router) sfxclient.Collector {
	metricTracking := web.RequestCounter{}
	r.NotFoundHandler = web.NewHandler(ctx, web.FromHTTP(http.NotFoundHandler())).Add(web.NextHTTP(metricTracking.ServeHTTP))
	return &sfxclient.WithDimensions{
		Dimensions: map[string]string{"http_endpoint": "http404"},
		Collector:  &metricTracking,
	}
}

func setupChain(ctx context.Context, sink dpsink.Sink, chainType string, getReader func(dpsink.Sink) ErrorReader, httpChain web.NextConstructor, logger log.Logger, moreConstructors ...web.Constructor) (http.Handler, sfxclient.Collector) {
	zippers := zipper.NewZipper()
	counter := &dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, dpsink.NextWrap(counter))
	errReader := getReader(finalSink)
	errorTracker := ErrorTrackerHandler{
		reader: errReader,
		Logger: logger,
	}
	metricTracking := web.RequestCounter{}
	handler := web.NewHandler(ctx, &errorTracker).Add(web.NextHTTP(metricTracking.ServeHTTP)).Add(httpChain)
	for _, c := range moreConstructors {
		handler.Add(c)
	}
	st := &sfxclient.WithDimensions{
		Collector: sfxclient.NewMultiCollector(
			&metricTracking,
			&errorTracker,
			counter,
			zippers,
		),
		Dimensions: map[string]string{
			"http_endpoint": "sfx_" + chainType,
		},
	}
	return zippers.GzipHandler(handler), st
}

func setupProtobufV1(ctx context.Context, r *mux.Router, sink dpsink.Sink, typeGetter MericTypeGetter, logger log.Logger, httpChain web.NextConstructor) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "protobuf_v1", func(s dpsink.Sink) ErrorReader {
		return &ProtobufDecoderV1{Sink: s, TypeGetter: typeGetter, Logger: logger}
	}, httpChain, logger)

	SetupProtobufV1Paths(r, handler)
	return st
}

// SetupProtobufV1Paths routes to R paths that should handle V1 Protobuf datapoints
func SetupProtobufV1Paths(r *mux.Router, handler http.Handler) {
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
}

func setupJSONV1(ctx context.Context, r *mux.Router, sink dpsink.Sink, typeGetter MericTypeGetter, logger log.Logger, httpChain web.NextConstructor) sfxclient.Collector {
	handler, st := setupChain(ctx, sink, "json_v1", func(s dpsink.Sink) ErrorReader {
		return &JSONDecoderV1{Sink: s, TypeGetter: typeGetter, Logger: logger}
	}, httpChain, logger)
	SetupJSONV1Paths(r, handler)

	return st
}

// SetupJSONV1Paths routes to R paths that should handle V1 JSON datapoints
func SetupJSONV1Paths(r *mux.Router, handler http.Handler) {
	SetupJSONByPaths(r, handler, "/datapoint")
	SetupJSONByPaths(r, handler, "/v1/datapoint")
}

func setupProtobufV2(ctx context.Context, r *mux.Router, sink dpsink.Sink, logger log.Logger, debugContext *web.HeaderCtxFlag, httpChain web.NextConstructor) sfxclient.Collector {
	additionalConstructors := []web.Constructor{}
	if debugContext != nil {
		additionalConstructors = append(additionalConstructors, debugContext)
	}
	handler, st := setupChain(ctx, sink, "protobuf_v2", func(s dpsink.Sink) ErrorReader {
		return &ProtobufDecoderV2{Sink: s, Logger: logger}
	}, httpChain, logger, additionalConstructors...)
	SetupProtobufV2DatapointPaths(r, handler)

	return st
}

func setupProtobufEventV2(ctx context.Context, r *mux.Router, sink dpsink.Sink, logger log.Logger, debugContext *web.HeaderCtxFlag, httpChain web.NextConstructor) sfxclient.Collector {
	additionalConstructors := []web.Constructor{}
	if debugContext != nil {
		additionalConstructors = append(additionalConstructors, debugContext)
	}
	handler, st := setupChain(ctx, sink, "protobuf_event_v2", func(s dpsink.Sink) ErrorReader {
		return &ProtobufEventDecoderV2{Sink: s, Logger: logger}
	}, httpChain, logger, additionalConstructors...)
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

func setupJSONV2(ctx context.Context, r *mux.Router, sink dpsink.Sink, logger log.Logger, debugContext *web.HeaderCtxFlag, httpChain web.NextConstructor) sfxclient.Collector {
	additionalConstructors := []web.Constructor{}
	if debugContext != nil {
		additionalConstructors = append(additionalConstructors, debugContext)
	}
	var j2 *JSONDecoderV2
	handler, st := setupChain(ctx, sink, "json_v2", func(s dpsink.Sink) ErrorReader {
		j2 = &JSONDecoderV2{Sink: s, Logger: logger}
		return j2
	}, httpChain, logger, additionalConstructors...)
	multi := sfxclient.NewMultiCollector(st, j2)
	SetupJSONV2DatapointPaths(r, handler)
	return multi
}

func setupJSONEventV2(ctx context.Context, r *mux.Router, sink dpsink.Sink, logger log.Logger, debugContext *web.HeaderCtxFlag, httpChain web.NextConstructor) sfxclient.Collector {
	additionalConstructors := []web.Constructor{}
	if debugContext != nil {
		additionalConstructors = append(additionalConstructors, debugContext)
	}
	handler, st := setupChain(ctx, sink, "json_event_v2", func(s dpsink.Sink) ErrorReader {
		return &JSONEventDecoderV2{Sink: s, Logger: logger}
	}, httpChain, logger, additionalConstructors...)
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
	r.Path(endpoint).Methods("POST").Headers("Content-Type", "application/json; charset=UTF-8").Handler(handler)
	r.Path(endpoint).Methods("POST").Headers("Content-Type", "").HandlerFunc(web.InvalidContentType)
	r.Path(endpoint).Methods("POST").Handler(handler)
}

func setupCollectd(ctx context.Context, r *mux.Router, sink dpsink.Sink, debugContext *web.HeaderCtxFlag, httpChain web.NextConstructor, logger log.Logger) sfxclient.Collector {
	counter := &dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, dpsink.NextWrap(counter))
	decoder := collectd.JSONDecoder{
		Logger: logger,
		SendTo: finalSink,
	}
	metricTracking := &web.RequestCounter{}
	httpHandler := web.NewHandler(ctx, &decoder).Add(web.NextHTTP(metricTracking.ServeHTTP), debugContext, httpChain)
	collectd.SetupCollectdPaths(r, httpHandler, "/v1/collectd")
	return &sfxclient.WithDimensions{
		Collector: sfxclient.NewMultiCollector(
			metricTracking,
			counter,
			&decoder,
		),
		Dimensions: map[string]string{
			"type": "collectd",
		},
	}
}
