package listener

import (
	"bufio"
	"encoding/json"
	"errors"
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
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/jsonengines"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
)

type listenerServer struct {
	name                    string
	listener                net.Listener
	metricCreationsMap      map[string]com_signalfuse_metrics_protobuf.MetricType
	metricCreationsMapMutex sync.Mutex

	collectdHandler       *collectdListenerServer
	datapointStreamingAPI core.DatapointStreamingAPI

	protobufPoints   int64
	jsonPoints       int64
	protobufPointsV2 int64
	jsonPointsV2     int64

	totalJSONDecoderCalls      int64
	totalProtobufDecoderCalls  int64
	activeJSONDecoderCalls     int64
	activeProtobufDecoderCalls int64
	errorJSONDecoderCalls      int64
	errorProtobufDecoderCalls  int64

	totalJSONDecoderCallsv2      int64
	totalProtobufDecoderCallsv2  int64
	activeJSONDecoderCallsv2     int64
	activeProtobufDecoderCallsv2 int64
	errorJSONDecoderCallsv2      int64
	errorProtobufDecoderCallsv2  int64
}

func (streamer *listenerServer) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	stats := map[string]int64{
		"total_json_calls":  atomic.LoadInt64(&streamer.totalJSONDecoderCalls),
		"active_json_calls": atomic.LoadInt64(&streamer.activeJSONDecoderCalls),
		"error_json_calls":  atomic.LoadInt64(&streamer.errorJSONDecoderCalls),

		"total_json_v2_calls":  atomic.LoadInt64(&streamer.totalJSONDecoderCallsv2),
		"active_json_v2_calls": atomic.LoadInt64(&streamer.activeJSONDecoderCallsv2),
		"error_json_v2_calls":  atomic.LoadInt64(&streamer.errorJSONDecoderCallsv2),

		"total_protobuf_calls":  atomic.LoadInt64(&streamer.totalProtobufDecoderCalls),
		"active_protobuf_calls": atomic.LoadInt64(&streamer.activeProtobufDecoderCalls),
		"error_protobuf_calls":  atomic.LoadInt64(&streamer.errorProtobufDecoderCalls),

		"total_protobuf_v2_calls":  atomic.LoadInt64(&streamer.totalProtobufDecoderCallsv2),
		"active_protobuf_v2_calls": atomic.LoadInt64(&streamer.activeProtobufDecoderCallsv2),
		"error_protobuf_v2_calls":  atomic.LoadInt64(&streamer.errorProtobufDecoderCallsv2),
	}
	for k, v := range stats {
		keyParts := strings.SplitN(k, "_", 2)

		keyType := keyParts[0]
		metric := keyParts[1]
		var t com_signalfuse_metrics_protobuf.MetricType
		if keyType == "active" {
			t = com_signalfuse_metrics_protobuf.MetricType_GAUGE
		} else {
			t = com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER
		}
		ret = append(
			ret,
			protocoltypes.NewOnHostDatapointDimensions(
				metric,
				value.NewIntWire(v),
				t,
				map[string]string{"listener": streamer.name, "type": keyType}))
	}
	ret = append(ret, streamer.collectdHandler.GetStats()...)
	return ret
}

func (streamer *listenerServer) Close() {
	streamer.listener.Close()
}

func (streamer *listenerServer) getMetricTypeFromMap(metricName string) com_signalfuse_metrics_protobuf.MetricType {
	streamer.metricCreationsMapMutex.Lock()
	defer streamer.metricCreationsMapMutex.Unlock()
	mt, ok := streamer.metricCreationsMap[metricName]
	if !ok {
		return com_signalfuse_metrics_protobuf.MetricType_GAUGE
	}
	return mt
}

var protoXXXDecodeVarint = proto.DecodeVarint

func (streamer *listenerServer) protobufDecoding(body io.Reader) error {
	bufferedBody := bufio.NewReaderSize(body, 32768)
	for {
		log.WithField("body", body).Debug("Starting protobuf loop")
		buf, err := bufferedBody.Peek(1) // should be big enough for any varint
		if err == io.EOF {
			log.Debug("EOF")
			return nil
		}

		if err != nil {
			log.WithField("err", err).Info("peek error")
			return err
		}
		log.Debug("Decoding varint")
		num, bytesRead := protoXXXDecodeVarint(buf)
		log.WithFields(log.Fields{"num": num, "bytesRead": bytesRead}).Debug("Decoding result")
		if bytesRead == 0 {
			// Invalid varint?
			return errors.New("invalid varint decode from protobuf stream")
		}
		if num > 32768 {
			// Sanity check
			return fmt.Errorf("invalid varint decode from protobuf stream.  Value too large %d", num)
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
		mt := streamer.getMetricTypeFromMap(msg.GetMetric())
		dp := protocoltypes.NewProtobufDataPointWithType(&msg, mt)
		log.WithField("dp", dp).Debug("Adding a point")
		streamer.datapointStreamingAPI.DatapointsChannel() <- dp
		atomic.AddInt64(&streamer.protobufPoints, 1)
	}
}

func (streamer *listenerServer) jsonDecoderFunction() func(*http.Request) error {
	return func(req *http.Request) error {
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
				mt := streamer.getMetricTypeFromMap(d.Metric)
				atomic.AddInt64(&streamer.jsonPoints, 1)
				streamer.datapointStreamingAPI.DatapointsChannel() <- core.NewRelativeTimeDatapoint(d.Metric, map[string]string{"sf_source": d.Source}, value.NewFloatWire(d.Value), mt, 0)
			}
		}
		return nil
	}
}

func (streamer *listenerServer) protobufDecoderFunctionV2() func(*http.Request) error {
	return func(req *http.Request) error {
		if req.ContentLength == -1 {
			return errors.New("Invalid content length")
		}
		var msg com_signalfuse_metrics_protobuf.DataPointUploadMessage
		bufferedBody := bufio.NewReaderSize(req.Body, 32768)
		buf, err := fullyReadFromBuffer(bufferedBody, uint64(req.ContentLength))
		if err != nil {
			return err
		}
		err = proto.Unmarshal(buf, &msg)
		if err != nil {
			return err
		}
		for _, protoDb := range msg.GetDatapoints() {
			dp := protocoltypes.NewProtobufDataPointWithType(protoDb, com_signalfuse_metrics_protobuf.MetricType_GAUGE)
			atomic.AddInt64(&streamer.protobufPointsV2, 1)
			streamer.datapointStreamingAPI.DatapointsChannel() <- dp
		}
		return nil
	}
}

func (streamer *listenerServer) jsonDecoderFunctionV2() func(*http.Request) error {
	return func(req *http.Request) error {
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
					atomic.AddInt64(&streamer.jsonPointsV2, 1)
					streamer.datapointStreamingAPI.DatapointsChannel() <- dp
				}
			}
		}
		return nil
	}
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

func (streamer *listenerServer) protobufDecoderFunction() func(*http.Request) error {
	return func(req *http.Request) error {
		log.WithField("req", req).Debug("Got a request")
		return streamer.protobufDecoding(req.Body)
	}
}

var defaultConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12345"),
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

func (streamer *listenerServer) metricHandler(writter http.ResponseWriter, req *http.Request) {
	dec := json.NewDecoder(req.Body)
	var d []protocoltypes.SignalfxMetricCreationStruct
	if err := dec.Decode(&d); err != nil {
		log.WithField("err", err).Info("Invalid metric creation request")
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(`{msg:"Invalid creation request"}`))
		return
	}
	log.WithField("d", d).Debug("Got metric types")
	streamer.metricCreationsMapMutex.Lock()
	defer streamer.metricCreationsMapMutex.Unlock()
	ret := []protocoltypes.SignalfxMetricCreationResponse{}
	for _, m := range d {
		metricType, ok := com_signalfuse_metrics_protobuf.MetricType_value[m.MetricType]
		if !ok {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Invalid metric type"}`))
			return
		}
		streamer.metricCreationsMap[m.MetricName] = com_signalfuse_metrics_protobuf.MetricType(metricType)
		ret = append(ret, protocoltypes.SignalfxMetricCreationResponse{Code: 409})
	}
	toWrite, err := jsonXXXMarshal(ret)
	if err != nil {
		log.WithField("err", err).Warn("Unable to marshal json")
		writter.WriteHeader(http.StatusBadRequest)
		writter.Write([]byte(`{msg:"Unable to marshal json!"}`))
		return
	}
	writter.WriteHeader(http.StatusOK)
	writter.Write([]byte(toWrite))
}

var jsonXXXMarshal = json.Marshal

type decoderFunc func() func(*http.Request) error

// StartServingHTTPOnPort servers http requests for Signalfx datapoints
func StartServingHTTPOnPort(listenAddr string, DatapointStreamingAPI core.DatapointStreamingAPI,
	clientTimeout time.Duration, name string, decodingEngine jsonengines.JSONDecodingEngine) (DatapointListener, error) {
	mux := http.NewServeMux()

	datapointHandler := func(
		writter http.ResponseWriter,
		req *http.Request,
		knownTypes map[string]decoderFunc,
		totalCallsMap map[string]*int64,
		activeCallsMap map[string]*int64,
		errorCallsMap map[string]*int64) {

		contentType := req.Header.Get("Content-type")
		decoderFunc, ok := knownTypes[contentType]

		if !ok {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Unknown content type"}`))
			return
		}
		totalCalls := totalCallsMap[contentType]
		activeCalls := activeCallsMap[contentType]
		errorCalls := errorCallsMap[contentType]
		atomic.AddInt64(totalCalls, 1)
		atomic.AddInt64(activeCalls, 1)
		defer atomic.AddInt64(activeCalls, -1)
		err := decoderFunc()(req)
		if err != nil {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(err.Error()))
			atomic.AddInt64(errorCalls, 1)
		} else {
			writter.WriteHeader(http.StatusOK)
			writter.Write([]byte(`"OK"`))
		}
	}

	server := http.Server{
		Handler:      mux,
		Addr:         listenAddr,
		ReadTimeout:  clientTimeout,
		WriteTimeout: clientTimeout,
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	listenServer := listenerServer{
		listener:                listener,
		metricCreationsMap:      make(map[string]com_signalfuse_metrics_protobuf.MetricType),
		metricCreationsMapMutex: sync.Mutex{},
		collectdHandler: &collectdListenerServer{
			name:                  name + "_collectd",
			listener:              nil,
			datapointStreamingAPI: DatapointStreamingAPI,
			decodingEngine:        decodingEngine,
		},
		datapointStreamingAPI: DatapointStreamingAPI,
	}

	collectdHandlerV1 := func(writter http.ResponseWriter, req *http.Request) {
		listenServer.collectdHandler.handleCollectd(writter, req)
	}

	datapointHandlerV1 := func(writter http.ResponseWriter, req *http.Request) {
		total := map[string]*int64{
			"":                       &listenServer.totalJSONDecoderCalls,
			"application/json":       &listenServer.totalJSONDecoderCalls,
			"application/x-protobuf": &listenServer.totalProtobufDecoderCalls,
		}
		active := map[string]*int64{
			"":                       &listenServer.activeJSONDecoderCalls,
			"application/json":       &listenServer.activeJSONDecoderCalls,
			"application/x-protobuf": &listenServer.activeProtobufDecoderCalls,
		}
		error := map[string]*int64{
			"":                       &listenServer.errorJSONDecoderCalls,
			"application/json":       &listenServer.errorJSONDecoderCalls,
			"application/x-protobuf": &listenServer.errorProtobufDecoderCalls,
		}
		knownTypes := map[string]decoderFunc{
			"":                       listenServer.jsonDecoderFunction,
			"application/json":       listenServer.jsonDecoderFunction,
			"application/x-protobuf": listenServer.protobufDecoderFunction,
		}
		datapointHandler(writter, req, knownTypes, total, active, error)
	}

	datapointHandlerV2 := func(writter http.ResponseWriter, req *http.Request) {
		total := map[string]*int64{
			"":                       &listenServer.totalJSONDecoderCallsv2,
			"application/json":       &listenServer.totalJSONDecoderCallsv2,
			"application/x-protobuf": &listenServer.totalProtobufDecoderCallsv2,
		}
		active := map[string]*int64{
			"":                       &listenServer.activeJSONDecoderCallsv2,
			"application/json":       &listenServer.activeJSONDecoderCallsv2,
			"application/x-protobuf": &listenServer.activeProtobufDecoderCallsv2,
		}
		error := map[string]*int64{
			"":                       &listenServer.errorJSONDecoderCallsv2,
			"application/json":       &listenServer.errorJSONDecoderCallsv2,
			"application/x-protobuf": &listenServer.errorProtobufDecoderCallsv2,
		}
		knownTypes := map[string]decoderFunc{
			"":                       listenServer.jsonDecoderFunctionV2,
			"application/json":       listenServer.jsonDecoderFunctionV2,
			"application/x-protobuf": listenServer.protobufDecoderFunctionV2,
		}
		datapointHandler(writter, req, knownTypes, total, active, error)
	}

	mux.HandleFunc(
		"/datapoint",
		datapointHandlerV1)
	mux.HandleFunc(
		"/v1/datapoint",
		datapointHandlerV1)
	mux.HandleFunc(
		"/v2/datapoint",
		datapointHandlerV2)
	mux.HandleFunc(
		"/v1/collectd",
		collectdHandlerV1)
	mux.HandleFunc(
		"/v1/metric",
		listenServer.metricHandler)
	mux.HandleFunc(
		"/metric",
		listenServer.metricHandler)
	go server.Serve(listener)
	return &listenServer, err
}
