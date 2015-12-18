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

	"errors"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dplocal"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/collectd"
	"github.com/signalfx/metricproxy/stats"
	"golang.org/x/net/context"
)

// ListenerServer controls listening on a socket for SignalFx connections
type ListenerServer struct {
	stats.Keeper
	name     string
	listener net.Listener
}

var _ protocol.Listener = &ListenerServer{}

// Close the exposed socket listening for new connections
func (streamer *ListenerServer) Close() error {
	return streamer.listener.Close()
}

// MericTypeGetter is an old metric interface that returns the type of a metric name
type MericTypeGetter interface {
	GetMetricTypeFromMap(metricName string) com_signalfx_metrics_protobuf.MetricType
}

// ConstTypeGetter always returns the wrapped metric type as a MericTypeGetter
type ConstTypeGetter com_signalfx_metrics_protobuf.MetricType

// GetMetricTypeFromMap returns the wrapped metric type object
func (c ConstTypeGetter) GetMetricTypeFromMap(metricName string) com_signalfx_metrics_protobuf.MetricType {
	return com_signalfx_metrics_protobuf.MetricType(c)
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

// Stats returns the number of calls to AddDatapoint
func (e *ErrorTrackerHandler) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		dplocal.NewOnHostDatapointDimensions(
			"total_errors",
			datapoint.NewIntValue(e.TotalErrors),
			datapoint.Counter,
			dimensions),
	}

}

// ServeHTTPC will serve the wrapped ErrorReader and return the error (if any) to rw if ErrorReader
// fails
func (e *ErrorTrackerHandler) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, req *http.Request) {
	if err := e.reader.Read(ctx, req); err != nil {
		log.WithField("err", err).Debug("Bad request")
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
}

var errInvalidProtobuf = errors.New("invalid protocol buffer sent")
var errProtobufTooLarge = errors.New("protobuf structure too large")
var errInvalidProtobufVarint = errors.New("invalid protobuf varint")

func (decoder *ProtobufDecoderV1) Read(ctx context.Context, req *http.Request) error {
	body := req.Body
	bufferedBody := bufio.NewReaderSize(body, 32768)
	for {
		log.WithField("body", body).Debug("Starting protobuf loop")
		buf, err := bufferedBody.Peek(1)
		if err == io.EOF {
			log.Debug("EOF")
			return nil
		}
		buf, err = bufferedBody.Peek(4) // should be big enough for any varint

		if err != nil {
			log.WithField("err", err).Info("peek error")
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
			log.WithField("dp", d).Debug("Got a new point")
			if d.Metric == "" {
				log.WithField("dp", d).Debug("Invalid Datapoint")
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
	Sink dpsink.Sink
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
		log.WithField("err", err).WithField("len", readLen).WithField("content-len", req.ContentLength).Warn("Unable to fully read from buffer")
		return err
	}
	var msg com_signalfx_metrics_protobuf.DataPointUploadMessage
	err = proto.Unmarshal(buf, &msg)
	if err != nil {
		log.Debug("Unable to unmarshal")
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
	Sink dpsink.ESink
}

func (decoder *ProtobufEventDecoderV2) Read(ctx context.Context, req *http.Request) error {
	if req.ContentLength == -1 {
		return errInvalidContentLength
	}

	// TODO: Source of memory creation.  Maybe pass buf in?
	buf := make([]byte, req.ContentLength)
	readLen, err := io.ReadFull(req.Body, buf)
	if err != nil {
		log.WithField("err", err).WithField("len", readLen).WithField("content-len", req.ContentLength).Warn("Unable to fully read from buffer")
		return err
	}
	var msg com_signalfx_metrics_protobuf.EventUploadMessage
	err = proto.Unmarshal(buf, &msg)
	if err != nil {
		log.Debug("Unable to unmarshal")
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
	Sink dpsink.Sink
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
			log.WithField("metricType", metricType).Warn("Unknown metric type")
			continue
		}
		for _, jsonDatapoint := range datapoints {
			v, err := ValueToValue(jsonDatapoint.Value)
			if err != nil {
				log.WithField("err", err).Warn("Unable to get value for datapoint")
			} else {
				dp := datapoint.New(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, fromMT(com_signalfx_metrics_protobuf.MetricType(mt)), fromTs(jsonDatapoint.Timestamp))
				dps = append(dps, dp)
			}
		}
	}
	return decoder.Sink.AddDatapoints(ctx, dps)
}

// JSONEventDecoderV2 decodes v2 json data for signalfx events and sends it to Sink
type JSONEventDecoderV2 struct {
	Sink dpsink.ESink
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
			jsonEvent.Category = workarounds.GolangDoesnotAllowPointerToStringLiteral("USER_DEFINED")
		}
		if jsonEvent.Timestamp == nil {
			jsonEvent.Timestamp = workarounds.GolangDoesnotAllowPointerToIntLiteral(0)
		}
		evt := event.NewWithMeta(jsonEvent.EventType, *jsonEvent.Category, jsonEvent.Dimensions, jsonEvent.Properties, fromTs(*jsonEvent.Timestamp))
		evts = append(evts, evt)
	}
	return decoder.Sink.AddEvents(ctx, evts)
}

var defaultConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:12345"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("signalfxlistener"),
	JSONEngine:      workarounds.GolangDoesnotAllowPointerToStringLiteral("native"),
}

// ListenerLoader loads a listener for signalfx protocol from config
func ListenerLoader(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (*ListenerServer, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultConfig)

	log.WithField("listenFrom", listenFrom).Info("Creating signalfx listener using final config")
	return StartServingHTTPOnPort(ctx, sink, *listenFrom.ListenAddr, *listenFrom.TimeoutDuration, *listenFrom.Name)
}

type jsonMarshalStub func(v interface{}) ([]byte, error)

type metricHandler struct {
	metricCreationsMapMutex sync.Mutex
	metricCreationsMap      map[string]com_signalfx_metrics_protobuf.MetricType
	jsonMarshal             jsonMarshalStub
}

func (handler *metricHandler) ServeHTTP(writter http.ResponseWriter, req *http.Request) {
	dec := json.NewDecoder(req.Body)
	var d []MetricCreationStruct
	if err := dec.Decode(&d); err != nil {
		log.WithField("err", err).Info("Invalid metric creation request")
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(`{msg:"Invalid creation request"}`))
		return
	}
	log.WithField("d", d).Debug("Got metric types")
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
	unmarshal := handler.jsonMarshal
	if unmarshal == nil {
		unmarshal = json.Marshal
	}
	toWrite, err := unmarshal(ret)
	if err != nil {
		log.WithField("err", err).Warn("Unable to marshal json")
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
func StartServingHTTPOnPort(ctx context.Context, sink dpsink.Sink, listenAddr string,
	clientTimeout time.Duration, name string) (*ListenerServer, error) {
	r := mux.NewRouter()

	server := http.Server{
		Handler:      r,
		Addr:         listenAddr,
		ReadTimeout:  clientTimeout,
		WriteTimeout: clientTimeout,
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	listenServer := ListenerServer{
		name:     name,
		listener: listener,
	}

	metricHandler := metricHandler{
		metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
	}
	r.Handle("/v1/metric", &metricHandler)
	r.Handle("/metric", &metricHandler)

	listenServer.Keeper = stats.Combine(
		setupNotFoundHandler(r, ctx, name),
		setupProtobufV1(r, ctx, name, sink, &metricHandler),
		setupJSONV1(r, ctx, name, sink, &metricHandler),
		setupProtobufV2(r, ctx, name, sink),
		setupProtobufEventV2(r, ctx, name, sink),
		setupJSONV2(r, ctx, name, sink),
		setupJSONEventV2(r, ctx, name, sink),
		setupCollectd(r, ctx, name, sink))

	go server.Serve(listener)
	return &listenServer, err
}

func setupNotFoundHandler(r *mux.Router, ctx context.Context, name string) stats.Keeper {
	metricTracking := web.RequestCounter{}
	r.NotFoundHandler = web.NewHandler(ctx, web.FromHTTP(http.NotFoundHandler())).Add(web.NextHTTP(metricTracking.ServeHTTP))
	return stats.ToKeeperMany(map[string]string{"location": "listener", "name": name, "type": "http404"}, &metricTracking)
}

func setupChain(ctx context.Context, sink dpsink.Sink, name string, chainType string, getReader func(dpsink.Sink) ErrorReader) (*web.Handler, stats.Keeper) {
	counter := &dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, dpsink.NextWrap(counter))
	errReader := getReader(finalSink)
	errorTracker := ErrorTrackerHandler{
		reader: errReader,
	}
	metricTracking := web.RequestCounter{}
	handler := web.NewHandler(ctx, &errorTracker).Add(web.NextHTTP(metricTracking.ServeHTTP))
	st := stats.ToKeeperMany(protocol.ListenerDims(name, "sfx_"+chainType), &metricTracking, &errorTracker, counter)
	return handler, st
}

func invalidContentType(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Invalid content type:"+r.Header.Get("Content-Type"), http.StatusBadRequest)
}

func setupProtobufV1(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink, typeGetter MericTypeGetter) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "protobuf_v1", func(s dpsink.Sink) ErrorReader {
		return &ProtobufDecoderV1{Sink: s, TypeGetter: typeGetter}
	})

	SetupProtobufV1Paths(r, handler)
	return st
}

// SetupProtobufV1Paths routes to R paths that should handle V1 Protobuf datapoints
func SetupProtobufV1Paths(r *mux.Router, handler http.Handler) {
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
}

func setupJSONV1(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink, typeGetter MericTypeGetter) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "json_v1", func(s dpsink.Sink) ErrorReader {
		return &JSONDecoderV1{Sink: s, TypeGetter: typeGetter}
	})
	SetupJSONV1Paths(r, handler)

	return st
}

// SetupJSONV1Paths routes to R paths that should handle V1 JSON datapoints
func SetupJSONV1Paths(r *mux.Router, handler http.Handler) {
	SetupJSONByPaths(r, handler, "/datapoint")
	SetupJSONByPaths(r, handler, "/v1/datapoint")
}

func setupProtobufV2(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "protobuf_v2", func(s dpsink.Sink) ErrorReader {
		return &ProtobufDecoderV2{Sink: s}
	})
	SetupProtobufV2DatapointPaths(r, handler)

	return st
}

func setupProtobufEventV2(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "protobuf_v2", func(s dpsink.Sink) ErrorReader {
		return &ProtobufEventDecoderV2{Sink: s}
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

func setupJSONV2(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "json_v2", func(s dpsink.Sink) ErrorReader {
		return &JSONDecoderV2{Sink: s}
	})
	SetupJSONV2DatapointPaths(r, handler)
	return st
}

func setupJSONEventV2(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "json_event_v2", func(s dpsink.Sink) ErrorReader {
		return &JSONEventDecoderV2{Sink: s}
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
	r.Path(endpoint).Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path(endpoint).Methods("POST").Handler(handler)
}

func setupCollectd(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	h, st := collectd.SetupHandler(ctx, name, sink, nil)
	SetupCollectdPaths(r, h)
	return st
}

// SetupCollectdPaths tells the router which paths the given handler (which should handle collectd json)
// should see
func SetupCollectdPaths(r *mux.Router, handler http.Handler) {
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "application/json").Handler(handler)
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
}
