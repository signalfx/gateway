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

	"code.google.com/p/goprotobuf/proto"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/gorilla/mux"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/datapoint/dpsink"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/protocol/collectd"
	"github.com/signalfx/metricproxy/reqcounter"
	"github.com/signalfx/metricproxy/stats"
	"github.com/signalfx/metricproxy/web"
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
	GetMetricTypeFromMap(metricName string) com_signalfuse_metrics_protobuf.MetricType
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
		datapoint.NewOnHostDatapointDimensions(
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

type protobufDecoderV1 struct {
	sink       dpsink.Sink
	typeGetter MericTypeGetter
}

var errInvalidProtobuf = errors.New("invalid protocol buffer sent")
var errProtobufTooLarge = errors.New("protobuf structure too large")
var errInvalidProtobufVarint = errors.New("invalid protobuf varint")

func (decoder *protobufDecoderV1) Read(ctx context.Context, req *http.Request) error {
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
		log.Debug("Decoding varint")
		num, bytesRead := proto.DecodeVarint(buf)
		log.WithField("num", num).WithField("bytesRead", bytesRead).Debug("Decode results")
		log.WithFields(log.Fields{"num": num, "bytesRead": bytesRead}).Debug("Decoding result")
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
		var msg com_signalfuse_metrics_protobuf.DataPoint
		err = proto.Unmarshal(buf, &msg)
		if err != nil {
			return err
		}
		if msg.Metric == nil || msg.Value == nil {
			return errInvalidProtobuf
		}
		mt := decoder.typeGetter.GetMetricTypeFromMap(msg.GetMetric())
		dp := NewProtobufDataPointWithType(&msg, mt)
		log.WithField("dp", dp).Debug("Adding a point")
		decoder.sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp})
	}
}

type jsonDecoderV1 struct {
	typeGetter MericTypeGetter
	sink       dpsink.Sink
}

func (decoder *jsonDecoderV1) Read(ctx context.Context, req *http.Request) error {
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
			mt := fromMT(decoder.typeGetter.GetMetricTypeFromMap(d.Metric))
			dp := datapoint.New(d.Metric, map[string]string{"sf_source": d.Source}, datapoint.NewFloatValue(d.Value), mt, time.Now())
			decoder.sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp})
		}
	}
	return nil
}

type protobufDecoderV2 struct {
	sink dpsink.Sink
}

var errInvalidContentLength = errors.New("invalid Content Length")

func (decoder *protobufDecoderV2) Read(ctx context.Context, req *http.Request) error {
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
	var msg com_signalfuse_metrics_protobuf.DataPointUploadMessage
	err = proto.Unmarshal(buf, &msg)
	if err != nil {
		log.Debug("Unable to unmarshal")
		return err
	}
	dps := make([]*datapoint.Datapoint, 0, len(msg.GetDatapoints()))
	for _, protoDb := range msg.GetDatapoints() {
		dps = append(dps, NewProtobufDataPointWithType(protoDb, com_signalfuse_metrics_protobuf.MetricType_GAUGE))
	}
	log.Debug("Ready to forward")
	return decoder.sink.AddDatapoints(ctx, dps)
}

type jsonDecoderV2 struct {
	sink dpsink.Sink
}

func (decoder *jsonDecoderV2) Read(ctx context.Context, req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	var d JSONDatapointV2
	if err := dec.Decode(&d); err != nil {
		return err
	}
	log.WithField("jsonpoint_v2", d).Debug("Got a new point")
	dps := make([]*datapoint.Datapoint, 0, 5)
	for metricType, datapoints := range d {
		mt, ok := com_signalfuse_metrics_protobuf.MetricType_value[strings.ToUpper(metricType)]
		if !ok {
			log.WithField("metricType", metricType).Warn("Unknown metric type")
			continue
		}
		for _, jsonDatapoint := range datapoints {
			v, err := ValueToValue(jsonDatapoint.Value)
			if err != nil {
				log.WithField("err", err).Warn("Unable to get value for datapoint")
			} else {
				dp := datapoint.New(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, fromMT(com_signalfuse_metrics_protobuf.MetricType(mt)), fromTs(jsonDatapoint.Timestamp))
				dps = append(dps, dp)
			}
		}
	}
	return decoder.sink.AddDatapoints(ctx, dps)
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
	metricCreationsMap      map[string]com_signalfuse_metrics_protobuf.MetricType
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
		metricType, ok := com_signalfuse_metrics_protobuf.MetricType_value[m.MetricType]
		if !ok {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Invalid metric type"}`))
			return
		}
		handler.metricCreationsMap[m.MetricName] = com_signalfuse_metrics_protobuf.MetricType(metricType)
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

func (handler *metricHandler) GetMetricTypeFromMap(metricName string) com_signalfuse_metrics_protobuf.MetricType {
	handler.metricCreationsMapMutex.Lock()
	defer handler.metricCreationsMapMutex.Unlock()
	mt, ok := handler.metricCreationsMap[metricName]
	if !ok {
		return com_signalfuse_metrics_protobuf.MetricType_GAUGE
	}
	return mt
}

type decoderFunc func() func(*http.Request) error

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
		metricCreationsMap: make(map[string]com_signalfuse_metrics_protobuf.MetricType),
	}
	r.Handle("/v1/metric", &metricHandler)
	r.Handle("/metric", &metricHandler)

	listenServer.Keeper = stats.Combine(
		setupNotFoundHandler(r, ctx, name),
		setupProtobufV1(r, ctx, name, sink, &metricHandler),
		setupJSONV1(r, ctx, name, sink, &metricHandler),
		setupProtobufV2(r, ctx, name, sink),
		setupJSONV2(r, ctx, name, sink),
		setupCollectd(r, ctx, name, sink))

	go server.Serve(listener)
	return &listenServer, err
}

func setupNotFoundHandler(r *mux.Router, ctx context.Context, name string) stats.Keeper {
	metricTracking := reqcounter.RequestCounter{}
	r.NotFoundHandler = web.NewHandler(ctx, web.FromHTTP(http.NotFoundHandler())).Add(web.NextHTTP(metricTracking.ServeHTTP))
	return stats.ToKeeperMany(map[string]string{"listener": name, "type": "http404"}, &metricTracking)
}

func setupChain(ctx context.Context, sink dpsink.Sink, name string, chainType string, getReader func(dpsink.Sink) ErrorReader) (*web.Handler, stats.Keeper) {
	counter := &dpsink.Counter{}
	finalSink := dpsink.FromChain(sink, counter.SinkMiddleware)
	errReader := getReader(finalSink)
	errorTracker := ErrorTrackerHandler{
		reader: errReader,
	}
	metricTracking := reqcounter.RequestCounter{}
	handler := web.NewHandler(ctx, &errorTracker).Add(web.NextHTTP(metricTracking.ServeHTTP))
	st := stats.ToKeeperMany(map[string]string{"listener": name, "type": chainType}, &metricTracking, &errorTracker, counter)
	return handler, st
}

func setupProtobufV1(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink, typeGetter MericTypeGetter) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "protobuf_v1", func(s dpsink.Sink) ErrorReader {
		return &protobufDecoderV1{sink: s, typeGetter: typeGetter}
	})

	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
	return st
}

func invalidContentType(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Invalid content type:"+r.Header.Get("Content-Type"), http.StatusBadRequest)
}

func setupJSONV1(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink, typeGetter MericTypeGetter) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "json_v1", func(s dpsink.Sink) ErrorReader {
		return &jsonDecoderV1{sink: s, typeGetter: typeGetter}
	})

	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(handler)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(handler)
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/datapoint").Methods("POST").Handler(handler)
	r.Path("/v1/datapoint").Methods("POST").Handler(handler)
	return st
}

func setupProtobufV2(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "protobuf_v2", func(s dpsink.Sink) ErrorReader {
		return &protobufDecoderV2{sink: s}
	})
	log.Debug("Setting up proto v2")
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)

	return st
}

func setupJSONV2(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	handler, st := setupChain(ctx, sink, name, "json_v2", func(s dpsink.Sink) ErrorReader {
		return &jsonDecoderV2{sink: s}
	})

	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(handler)
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/v2/datapoint").Methods("POST").Handler(handler)
	return st
}

func setupCollectd(r *mux.Router, ctx context.Context, name string, sink dpsink.Sink) stats.Keeper {
	h, st := collectd.SetupHandler(ctx, name, sink)
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "application/json").Handler(h)
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	return st
}
