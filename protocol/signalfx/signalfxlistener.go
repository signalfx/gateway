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

	"code.google.com/p/goprotobuf/proto"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/datapoint"
	"github.com/signalfuse/signalfxproxy/protocol/collectd"
	"github.com/signalfuse/signalfxproxy/reqcounter"
	"github.com/signalfuse/signalfxproxy/stats"
)

type listenerServer struct {
	name     string
	listener net.Listener

	statKeepers []stats.Keeper
}

func (streamer *listenerServer) Stats() []datapoint.Datapoint {
	return stats.CombineStats(streamer.statKeepers)
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

// Stats returns the number of calls to AddDatapoint
func (e *ErrorTrackerHandler) Stats(dimensions map[string]string) []datapoint.Datapoint {
	ret := []datapoint.Datapoint{}
	ret = append(
		ret,
		datapoint.NewOnHostDatapointDimensions(
			"total_errors",
			datapoint.NewIntValue(e.TotalErrors),
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
	datapointTracker datapoint.Tracker
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
		decoder.datapointTracker.AddDatapoint(dp)
	}
}

type jsonDecoderV1 struct {
	typeGetter       MericTypeGetter
	datapointTracker datapoint.Tracker
}

func (decoder *jsonDecoderV1) Read(req *http.Request) error {
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
			mt := decoder.typeGetter.GetMetricTypeFromMap(d.Metric)
			dp := datapoint.NewRelativeTime(d.Metric, map[string]string{"sf_source": d.Source}, datapoint.NewFloatValue(d.Value), mt, 0)
			decoder.datapointTracker.AddDatapoint(dp)
		}
	}
	return nil
}

type protobufDecoderV2 struct {
	datapointTracker datapoint.Tracker
}

var errInvalidContentLength = fmt.Errorf("Invalid Content Length")

func (decoder *protobufDecoderV2) Read(req *http.Request) error {
	return DecodeProtobufV2(req, &decoder.datapointTracker)
}

// DecodeProtobufV2 will take a request of signalfx's V2 protocol buffers and forward them to datapointTracker
func DecodeProtobufV2(req *http.Request, datapointTracker datapoint.Adder) error {
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
		return err
	}
	for _, protoDb := range msg.GetDatapoints() {
		dp := NewProtobufDataPointWithType(protoDb, com_signalfuse_metrics_protobuf.MetricType_GAUGE)
		datapointTracker.AddDatapoint(dp)
	}
	return nil
}

type jsonDecoderV2 struct {
	datapointTracker datapoint.Tracker
}

func (decoder *jsonDecoderV2) Read(req *http.Request) error {
	return DecodeJSONV2(req, &decoder.datapointTracker)
}

// DecodeJSONV2 accepts datapoints in signalfx's v2 JSON format and forwards them to datapointTracker
func DecodeJSONV2(req *http.Request, datapointTracker datapoint.Adder) error {
	dec := json.NewDecoder(req.Body)
	var d JSONDatapointV2
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
			v, err := ValueToValue(jsonDatapoint.Value)
			if err != nil {
				log.WithField("err", err).Warn("Unable to get value for datapoint")
			} else {
				dp := datapoint.NewRelativeTime(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, com_signalfuse_metrics_protobuf.MetricType(mt), jsonDatapoint.Timestamp)
				datapointTracker.AddDatapoint(dp)
			}
		}
	}
	return nil
}

var defaultConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:12345"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("signalfxlistener"),
	JSONEngine:      workarounds.GolangDoesnotAllowPointerToStringLiteral("native"),
}

// ListenerLoader loads a listener for signalfx protocol from config
func ListenerLoader(Streamer datapoint.Streamer, listenFrom *config.ListenFrom) (stats.ClosableKeeper, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultConfig)

	engine, err := collectd.LoadEngine(*listenFrom.JSONEngine)
	if err != nil {
		return nil, err
	}

	log.WithField("listenFrom", listenFrom).Info("Creating signalfx listener using final config")
	return StartServingHTTPOnPort(*listenFrom.ListenAddr, Streamer,
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
func StartServingHTTPOnPort(listenAddr string, Streamer datapoint.Streamer,
	clientTimeout time.Duration, name string, decodingEngine collectd.JSONEngine) (stats.ClosableKeeper, error) {
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
	listenServer.statKeepers = append(listenServer.statKeepers, setupProtobufV1(Streamer, r, name, &metricHandler))
	listenServer.statKeepers = append(listenServer.statKeepers, setupJSONV1(Streamer, r, name, &metricHandler))
	listenServer.statKeepers = append(listenServer.statKeepers, setupProtobufV2(Streamer, r, name))
	listenServer.statKeepers = append(listenServer.statKeepers, setupJSONV2(Streamer, r, name))
	listenServer.statKeepers = append(listenServer.statKeepers, setupCollectd(decodingEngine, Streamer, r, name))

	go server.Serve(listener)
	return &listenServer, err
}

func setupNotFoundHandler(r *mux.Router, name string) stats.Keeper {
	metricTracking := reqcounter.RequestCounter{}
	r.NotFoundHandler = negroni.New(&metricTracking, negroni.Wrap(http.NotFoundHandler()))
	return &stats.KeeperWrap{
		Base:       []stats.DimensionKeeper{&metricTracking},
		Dimensions: map[string]string{"listener": name, "type": "unknown"},
	}
}

func setupProtobufV1(Streamer datapoint.Streamer, r *mux.Router, name string, typeGetter MericTypeGetter) stats.Keeper {
	decoder := protobufDecoderV1{
		datapointTracker: datapoint.Tracker{
			Streamer: Streamer,
		},
		typeGetter: typeGetter,
	}

	errorTracker := ErrorTrackerHandler{
		reader: &decoder,
	}

	metricTracking := reqcounter.RequestCounter{}

	n := negroni.New(&metricTracking, negroni.Wrap(&errorTracker))
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(n)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(n)
	return &stats.KeeperWrap{
		Base:       []stats.DimensionKeeper{&metricTracking, &errorTracker, &decoder.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "protobuf_v1"},
	}
}

func invalidContentType(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Invalid content type:"+r.Header.Get("Content-Type"), http.StatusBadRequest)
}

func setupJSONV1(Streamer datapoint.Streamer, r *mux.Router, name string, typeGetter MericTypeGetter) stats.Keeper {
	decoder := jsonDecoderV1{
		datapointTracker: datapoint.Tracker{
			Streamer: Streamer,
		},
		typeGetter: typeGetter,
	}

	errorTracker := ErrorTrackerHandler{
		reader: &decoder,
	}

	metricTracking := reqcounter.RequestCounter{}

	n := negroni.New(&metricTracking, negroni.Wrap(&errorTracker))
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/v1/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/datapoint").Methods("POST").Handler(n)
	r.Path("/v1/datapoint").Methods("POST").Handler(n)
	return &stats.KeeperWrap{
		Base:       []stats.DimensionKeeper{&metricTracking, &errorTracker, &decoder.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "json_v1"},
	}
}

func setupProtobufV2(Streamer datapoint.Streamer, r *mux.Router, name string) stats.Keeper {
	protobufDecoderV2 := protobufDecoderV2{
		datapointTracker: datapoint.Tracker{
			Streamer: Streamer,
		},
	}

	protobufDecoderV2ErrorTracker := ErrorTrackerHandler{
		reader: &protobufDecoderV2,
	}

	metricTracking := reqcounter.RequestCounter{}

	n := negroni.New(&metricTracking, negroni.Wrap(&protobufDecoderV2ErrorTracker))
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(n)

	return &stats.KeeperWrap{
		Base:       []stats.DimensionKeeper{&metricTracking, &protobufDecoderV2ErrorTracker, &protobufDecoderV2.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "protobuf_v2"},
	}
}

func setupJSONV2(Streamer datapoint.Streamer, r *mux.Router, name string) stats.Keeper {
	jsonDecoderV2 := jsonDecoderV2{
		datapointTracker: datapoint.Tracker{
			Streamer: Streamer,
		},
	}

	jsonDecoderV2ErrorTracker := ErrorTrackerHandler{
		reader: &jsonDecoderV2,
	}

	metricTracking := reqcounter.RequestCounter{}

	n := negroni.New(&metricTracking, negroni.Wrap(&jsonDecoderV2ErrorTracker))
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/v2/datapoint").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	r.Path("/v2/datapoint").Methods("POST").Handler(n)
	return &stats.KeeperWrap{
		Base:       []stats.DimensionKeeper{&metricTracking, &jsonDecoderV2ErrorTracker, &jsonDecoderV2.datapointTracker},
		Dimensions: map[string]string{"listener": name, "type": "json_v2"},
	}
}

func setupCollectd(decodingEngine collectd.JSONEngine, Streamer datapoint.Streamer, r *mux.Router, name string) stats.Keeper {
	collectdHandler := collectd.JSONDecoder{
		DecodingEngine: decodingEngine,
		DatapointTracker: datapoint.Tracker{
			Streamer: Streamer,
		},
	}

	metricTracking := reqcounter.RequestCounter{}

	n := negroni.New(&metricTracking, negroni.Wrap(&collectdHandler))
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "application/json").Handler(n)
	r.Path("/v1/collectd").Methods("POST").Headers("Content-Type", "").HandlerFunc(invalidContentType)
	return &stats.KeeperWrap{
		Base:       []stats.DimensionKeeper{&metricTracking, &collectdHandler},
		Dimensions: map[string]string{"listener": name, "type": "collectd"},
	}
}
