package listener

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

	"code.google.com/p/goprotobuf/proto"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/jsonengines"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
)

type listenerServer struct {
	name     string
	listener net.Listener

	statKeepers []core.StatKeeper
}

func (streamer *listenerServer) GetAddr() net.Addr {
	return streamer.listener.Addr()
}

func (streamer *listenerServer) GetStats() []core.Datapoint {
	return core.CombineStats(streamer.statKeepers)
}

func (streamer *listenerServer) Close() {
	streamer.listener.Close()
}

// MericTypeGetter is an old metric interface that returns the type of a metric name
type MericTypeGetter interface {
	GetMetricTypeFromMap(metricName string) com_signalfuse_metrics_protobuf.MetricType
}

// ErrorReader are datapoint streamers that read from a HTTP request and return errors if
// the stream is invalid
type ErrorReader interface {
	Read(req *http.Request) error
}

// ErrorTrackerHandler behaves like a http handler, but tracks error returns from a ErrorReader
type ErrorTrackerHandler struct {
	TotalErrors int64
	reader      ErrorReader
}

// GetStats returns the number of calls to AddDatapoint
func (e *ErrorTrackerHandler) GetStats(dimensions map[string]string) []core.Datapoint {
	ret := []core.Datapoint{}
	ret = append(
		ret,
		protocoltypes.NewOnHostDatapointDimensions(
			"total_errors",
			value.NewIntWire(e.TotalErrors),
			com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
			dimensions))
	return ret
}

func (e *ErrorTrackerHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if err := e.reader.Read(req); err != nil {
		atomic.AddInt64(&e.TotalErrors, 1)
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(err.Error()))
		return
	}
	rw.Write([]byte(`"OK"`))
}

type protobufDecoderV1 struct {
	typeGetter       MericTypeGetter
	datapointTracker DatapointTracker
}

var errInvalidProtobuf = fmt.Errorf("Invalid protocol buffer sent")
var errProtobufTooLarge = fmt.Errorf("Protobuf structure too large")
var errInvalidProtobufVarint = fmt.Errorf("Invalid protobuf varint")

func (decoder *protobufDecoderV1) Read(req *http.Request) error {
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
		fullyReadFromBuffer(bufferedBody, uint64(bytesRead))
		//buf = make([]byte, num)
		buf, err = fullyReadFromBuffer(bufferedBody, num)
		if int(num) != len(buf) {
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
		dp := protocoltypes.NewProtobufDataPointWithType(&msg, mt)
		log.WithField("dp", dp).Debug("Adding a point")
		decoder.datapointTracker.AddDatapoint(dp)
	}
}

type jsonDecoderV1 struct {
	typeGetter       MericTypeGetter
	datapointTracker DatapointTracker
}

func (decoder *jsonDecoderV1) Read(req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	for {
		var d protocoltypes.SignalfxJSONDatapointV1
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
			mt := decoder.typeGetter.GetMetricTypeFromMap(d.Metric)
			dp := core.NewRelativeTimeDatapoint(d.Metric, map[string]string{"sf_source": d.Source}, value.NewFloatWire(d.Value), mt, 0)
			decoder.datapointTracker.AddDatapoint(dp)
		}
	}
	return nil
}

type protobufDecoderV2 struct {
	datapointTracker DatapointTracker
}

var errInvalidContentLength = fmt.Errorf("Invalid Content Length")

func (decoder *protobufDecoderV2) Read(req *http.Request) error {
	if req.ContentLength == -1 {
		return errInvalidContentLength
	}
	var msg com_signalfuse_metrics_protobuf.DataPointUploadMessage
	bufferedBody := bufio.NewReaderSize(req.Body, 32768)
	buf, err := fullyReadFromBuffer(bufferedBody, uint64(req.ContentLength))
	if err != nil {
		log.WithField("err", err).WithField("len", len(buf)).Warn("Unable to fully read from buffer")
		return err
	}
	err = proto.Unmarshal(buf, &msg)
	if err != nil {
		return err
	}
	for _, protoDb := range msg.GetDatapoints() {
		dp := protocoltypes.NewProtobufDataPointWithType(protoDb, com_signalfuse_metrics_protobuf.MetricType_GAUGE)
		decoder.datapointTracker.AddDatapoint(dp)
	}
	return nil
}

type jsonDecoderV2 struct {
	datapointTracker DatapointTracker
}

func (decoder *jsonDecoderV2) Read(req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	var d protocoltypes.SignalfxJSONDatapointV2
	if err := dec.Decode(&d); err != nil {
		return err
	}
	log.WithField("jsonpoint_v2", d).Debug("Got a new point")
	for metricType, datapoints := range d {
		mt, ok := com_signalfuse_metrics_protobuf.MetricType_value[strings.ToUpper(metricType)]
		if !ok {
			log.WithField("metricType", metricType).Warn("Unknown metric type")
			continue
		}
		for _, jsonDatapoint := range datapoints {
			v, err := protocoltypes.ValueToDatapointValue(jsonDatapoint.Value)
			if err != nil {
				log.WithField("err", err).Warn("Unable to get value for datapoint")
			} else {
				dp := core.NewRelativeTimeDatapoint(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, com_signalfuse_metrics_protobuf.MetricType(mt), jsonDatapoint.Timestamp)
				decoder.datapointTracker.AddDatapoint(dp)
			}
		}
	}
	return nil
}

func fullyReadFromBuffer(buffer *bufio.Reader, numBytes uint64) ([]byte, error) {
	totalBytesRead := uint64(0)
	buf := make([]byte, numBytes)
	for numBytes > totalBytesRead {
		n, err := buffer.Read(buf[totalBytesRead:numBytes])
		totalBytesRead += uint64(n)
		if err != nil || n == 0 {
			return buf[0:totalBytesRead], err
		}

	}
	return buf, nil
}

var defaultConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:12345"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("signalfxlistener"),
	JSONEngine:      workarounds.GolangDoesnotAllowPointerToStringLiteral("native"),
}

// SignalFxListenerLoader loads a listener for signalfx protocol from config
func SignalFxListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultConfig)
	log.WithField("listenFrom", listenFrom).Info("Creating signalfx listener using final config")
	engine, err := jsonengines.Load(*listenFrom.JSONEngine)
	if err != nil {
		return nil, err
	}
	return StartServingHTTPOnPort(*listenFrom.ListenAddr, DatapointStreamingAPI,
		*listenFrom.TimeoutDuration, *listenFrom.Name, engine)
}

type jsonMarshalStub func(v interface{}) ([]byte, error)

type metricHandler struct {
	metricCreationsMapMutex sync.Mutex
	metricCreationsMap      map[string]com_signalfuse_metrics_protobuf.MetricType
	jsonMarshal             jsonMarshalStub
}

func (handler *metricHandler) ServeHTTP(writter http.ResponseWriter, req *http.Request) {
	dec := json.NewDecoder(req.Body)
	var d []protocoltypes.SignalfxMetricCreationStruct
	if err := dec.Decode(&d); err != nil {
		log.WithField("err", err).Info("Invalid metric creation request")
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(`{msg:"Invalid creation request"}`))
		return
	}
	log.WithField("d", d).Debug("Got metric types")
	handler.metricCreationsMapMutex.Lock()
	defer handler.metricCreationsMapMutex.Unlock()
	ret := []protocoltypes.SignalfxMetricCreationResponse{}
	for _, m := range d {
		metricType, ok := com_signalfuse_metrics_protobuf.MetricType_value[m.MetricType]
		if !ok {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Invalid metric type"}`))
			return
		}
		handler.metricCreationsMap[m.MetricName] = com_signalfuse_metrics_protobuf.MetricType(metricType)
		ret = append(ret, protocoltypes.SignalfxMetricCreationResponse{Code: 409})
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
func StartServingHTTPOnPort(listenAddr string, DatapointStreamingAPI core.DatapointStreamingAPI,
	clientTimeout time.Duration, name string, decodingEngine jsonengines.JSONDecodingEngine) (DatapointListener, error) {

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
	listenServer := listenerServer{
		name:     name,
		listener: listener,
	}

	metricHandler := metricHandler{
		metricCreationsMap: make(map[string]com_signalfuse_metrics_protobuf.MetricType),
	}
	r.Handle("/v1/metric", &metricHandler)
	r.Handle("/metric", &metricHandler)

	listenServer.statKeepers = append(listenServer.statKeepers, setupNotFoundHandler(r, name))
	listenServer.statKeepers = append(listenServer.statKeepers, setupProtobufV1(DatapointStreamingAPI, r, name, &metricHandler))
	listenServer.statKeepers = append(listenServer.statKeepers, setupJSONV1(DatapointStreamingAPI, r, name, &metricHandler))
	listenServer.statKeepers = append(listenServer.statKeepers, setupProtobufV2(DatapointStreamingAPI, r, name))
	listenServer.statKeepers = append(listenServer.statKeepers, setupJSONV2(DatapointStreamingAPI, r, name))
	listenServer.statKeepers = append(listenServer.statKeepers, setupCollectd(decodingEngine, DatapointStreamingAPI, r, name))

	go server.Serve(listener)
	return &listenServer, err
}

func setupNotFoundHandler(r *mux.Router, name string) core.StatKeeper {
	metricTracking := MetricTrackingMiddleware{}
	r.NotFoundHandler = negroni.New(&metricTracking, negroni.Wrap(http.NotFoundHandler()))
	return &core.StatKeeperWrap{
		Base:       []core.DimensionStatKeeper{&metricTracking},
		Dimensions: map[string]string{"listener": name, "type": "unknown"},
	}
}

func setupProtobufV1(DatapointStreamingAPI core.DatapointStreamingAPI, r *mux.Router, name string, typeGetter MericTypeGetter) core.StatKeeper {
	decoder := protobufDecoderV1{
		datapointTracker: DatapointTracker{
			DatapointStreamingAPI: DatapointStreamingAPI,
		},
		typeGetter: typeGetter,
	}

	errorTracker := ErrorTrackerHandler{
		reader: &decoder,
	}

	metricTracking := MetricTrackingMiddleware{}

	n := negroni.New(&metricTracking, negroni.Wrap(&errorTracker))
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(n)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(n)
	return &core.StatKeeperWrap{
		Base:       []core.DimensionStatKeeper{&metricTracking, &errorTracker, &decoder.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "protobuf_v1"},
	}
}

func invalidContentType(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Invalid content type:"+r.Header.Get("Content-Type"), http.StatusBadRequest)
}

func setupJSONV1(DatapointStreamingAPI core.DatapointStreamingAPI, r *mux.Router, name string, typeGetter MericTypeGetter) core.StatKeeper {
	decoder := jsonDecoderV1{
		datapointTracker: DatapointTracker{
			DatapointStreamingAPI: DatapointStreamingAPI,
		},
		typeGetter: typeGetter,
	}

	errorTracker := ErrorTrackerHandler{
		reader: &decoder,
	}

	metricTracking := MetricTrackingMiddleware{}

	n := negroni.New(&metricTracking, negroni.Wrap(&errorTracker))
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/datapoint").Methods("POST").Handler(n)
	r.Path("/v1/datapoint").Methods("POST").Handler(n)
	return &core.StatKeeperWrap{
		Base:       []core.DimensionStatKeeper{&metricTracking, &errorTracker, &decoder.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "json_v1"},
	}
}

func setupProtobufV2(DatapointStreamingAPI core.DatapointStreamingAPI, r *mux.Router, name string) core.StatKeeper {

	protobufDecoderV2 := protobufDecoderV2{
		datapointTracker: DatapointTracker{
			DatapointStreamingAPI: DatapointStreamingAPI,
		},
	}

	protobufDecoderV2ErrorTracker := ErrorTrackerHandler{
		reader: &protobufDecoderV2,
	}

	metricTracking := MetricTrackingMiddleware{}

	n := negroni.New(&metricTracking, negroni.Wrap(&protobufDecoderV2ErrorTracker))
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(n)

	return &core.StatKeeperWrap{
		Base:       []core.DimensionStatKeeper{&metricTracking, &protobufDecoderV2ErrorTracker, &protobufDecoderV2.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "protobuf_v2"},
	}
}

func setupJSONV2(DatapointStreamingAPI core.DatapointStreamingAPI, r *mux.Router, name string) core.StatKeeper {
	jsonDecoderV2 := jsonDecoderV2{
		datapointTracker: DatapointTracker{
			DatapointStreamingAPI: DatapointStreamingAPI,
		},
	}

	jsonDecoderV2ErrorTracker := ErrorTrackerHandler{
		reader: &jsonDecoderV2,
	}

	metricTracking := MetricTrackingMiddleware{}

	n := negroni.New(&metricTracking, negroni.Wrap(&jsonDecoderV2ErrorTracker))
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/v2/datapoint").Methods("POST").Handler(n)
	return &core.StatKeeperWrap{
		Base:       []core.DimensionStatKeeper{&metricTracking, &jsonDecoderV2ErrorTracker, &jsonDecoderV2.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "json_v2"},
	}
}

func setupCollectd(decodingEngine jsonengines.JSONDecodingEngine, DatapointStreamingAPI core.DatapointStreamingAPI, r *mux.Router, name string) core.StatKeeper {
	collectdHandler := collectdJSONDecoder{
		decodingEngine: decodingEngine,
		datapointTracker: DatapointTracker{
			DatapointStreamingAPI: DatapointStreamingAPI,
		},
	}

	metricTracking := MetricTrackingMiddleware{}

	n := negroni.New(&metricTracking, negroni.Wrap(&collectdHandler))
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	return &core.StatKeeperWrap{
		Base:       []core.DimensionStatKeeper{&metricTracking, &collectdHandler},
		Dimensions: map[string]string{"listener": name, "type": "collectd"},
	}
}
