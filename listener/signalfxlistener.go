package listener

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/golang/glog"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/core/value"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type listenerServer struct {
	listener                net.Listener
	metricCreationsMap      map[string]com_signalfuse_metrics_protobuf.MetricType
	metricCreationsMapMutex *sync.Mutex
}

func (streamer *listenerServer) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

func (streamer *listenerServer) Close() {
	streamer.listener.Close()
}

func getMetricTypeFromMap(metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex, metricName string) com_signalfuse_metrics_protobuf.MetricType {
	metricCreationsMapMutex.Lock()
	defer metricCreationsMapMutex.Unlock()
	mt, ok := metricCreationsMap[metricName]
	if !ok {
		return com_signalfuse_metrics_protobuf.MetricType_GAUGE
	}
	return mt
}

var protoXXXDecodeVarint = proto.DecodeVarint

func protobufDecoding(body io.Reader, metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex, DatapointStreamingAPI core.DatapointStreamingAPI) error {
	bufferedBody := bufio.NewReaderSize(body, 32768)
	for {
		glog.Infof("Start of loop for %s", body)
		buf, err := bufferedBody.Peek(1) // should be big enough for any varint
		if err == io.EOF {
			glog.Infof("EOF")
			return nil
		}

		if err != nil {
			glog.Infof("peek error: %s", err)
			return err
		}
		glog.Infof("Decoding varint")
		num, bytesRead := protoXXXDecodeVarint(buf)
		glog.Infof("Decoding result: %d %d", num, bytesRead)
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
		mt := getMetricTypeFromMap(metricCreationsMap, metricCreationsMapMutex, msg.GetMetric())
		dp := protocoltypes.NewProtobufDataPointWithType(&msg, mt)
		glog.Infof("Adding a point")
		DatapointStreamingAPI.DatapointsChannel() <- dp
	}
}

func jsonDecoderFunction(DatapointStreamingAPI core.DatapointStreamingAPI, metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex) func(*http.Request) error {
	return func(req *http.Request) error {
		dec := json.NewDecoder(req.Body)
		for {
			var d protocoltypes.SignalfxJSONDatapointV1
			if err := dec.Decode(&d); err == io.EOF {
				break
			} else if err != nil {
				return err
			} else {
				glog.V(3).Info("Got a new point: %s", d)
				if d.Metric == "" {
					glog.Warningf("Invalid datapoint %s", d)
					continue
				}
				mt := getMetricTypeFromMap(metricCreationsMap, metricCreationsMapMutex, d.Metric)
				DatapointStreamingAPI.DatapointsChannel() <- core.NewRelativeTimeDatapoint(d.Metric, map[string]string{"sf_source": d.Source}, value.NewFloatWire(d.Value), mt, 0)
			}
		}
		return nil
	}
}

func protobufDecoderFunctionV2(DatapointStreamingAPI core.DatapointStreamingAPI, metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex) func(*http.Request) error {
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
			DatapointStreamingAPI.DatapointsChannel() <- dp
		}
		return nil
	}
}

func jsonDecoderFunctionV2(DatapointStreamingAPI core.DatapointStreamingAPI, metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex) func(*http.Request) error {
	return func(req *http.Request) error {
		dec := json.NewDecoder(req.Body)
		var d protocoltypes.SignalfxJSONDatapointV2
		if err := dec.Decode(&d); err != nil {
			return err
		}
		glog.V(3).Info("Got a new point: %s", d)
		for metricType, datapoints := range d {
			mt, ok := com_signalfuse_metrics_protobuf.MetricType_value[strings.ToUpper(metricType)]
			if !ok {
				glog.Warningf("Unknown metric type %s", metricType)
				continue
			}
			for _, jsonDatapoint := range datapoints {
				v, err := protocoltypes.ValueToDatapointValue(jsonDatapoint.Value)
				if err != nil {
					glog.Warningf("Unable to get value for datapoint: %s", err)
				} else {
					dp := core.NewRelativeTimeDatapoint(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, com_signalfuse_metrics_protobuf.MetricType(mt), jsonDatapoint.Timestamp)
					DatapointStreamingAPI.DatapointsChannel() <- dp
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

func protobufDecoderFunction(DatapointStreamingAPI core.DatapointStreamingAPI, metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex) func(*http.Request) error {
	return func(req *http.Request) error {
		glog.Infof("Request is %s", req)
		return protobufDecoding(req.Body, metricCreationsMap, metricCreationsMapMutex, DatapointStreamingAPI)
	}
}

var defaultConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("0.0.0.0:12345"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
}

// SignalFxListenerLoader loads a listener for signalfx protocol from config
func SignalFxListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultConfig)
	glog.Infof("Creating signalfx listener using final config %s", listenFrom)
	return StartServingHTTPOnPort(*listenFrom.ListenAddr, DatapointStreamingAPI, *listenFrom.TimeoutDuration)
}

var jsonXXXMarshal = json.Marshal

type decoderFunc func(core.DatapointStreamingAPI, map[string]com_signalfuse_metrics_protobuf.MetricType, *sync.Mutex) func(*http.Request) error

// StartServingHTTPOnPort servers http requests for Signalfx datapoints
func StartServingHTTPOnPort(listenAddr string, DatapointStreamingAPI core.DatapointStreamingAPI, clientTimeout time.Duration) (DatapointListener, error) {
	mux := http.NewServeMux()
	metricCreationsMap := make(map[string]com_signalfuse_metrics_protobuf.MetricType)
	metricCreationsMapMutex := &sync.Mutex{}

	datapointHandler := func(writter http.ResponseWriter, req *http.Request, knownTypes map[string]decoderFunc) {
		contentType := req.Header.Get("Content-type")
		decoderFunc, ok := knownTypes[contentType]

		if !ok {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Unknown content type"}`))
			return
		}
		err := decoderFunc(DatapointStreamingAPI, metricCreationsMap, metricCreationsMapMutex)(req)
		if err != nil {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(err.Error()))
		} else {
			writter.WriteHeader(http.StatusOK)
			writter.Write([]byte(`"OK"`))
		}
	}

	datapointHandlerV1 := func(writter http.ResponseWriter, req *http.Request) {
		knownTypes := map[string]decoderFunc{
			"":                       jsonDecoderFunction,
			"application/json":       jsonDecoderFunction,
			"application/x-protobuf": protobufDecoderFunction,
		}
		datapointHandler(writter, req, knownTypes)
	}

	datapointHandlerV2 := func(writter http.ResponseWriter, req *http.Request) {
		knownTypes := map[string]decoderFunc{
			"":                       jsonDecoderFunctionV2,
			"application/json":       jsonDecoderFunctionV2,
			"application/x-protobuf": protobufDecoderFunctionV2,
		}
		datapointHandler(writter, req, knownTypes)
	}

	metricHandler := func(writter http.ResponseWriter, req *http.Request) {
		dec := json.NewDecoder(req.Body)
		var d []protocoltypes.SignalfxMetricCreationStruct
		if err := dec.Decode(&d); err != nil {
			glog.Infof("Invalid metric creation request: %s", err)
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Invalid creation request"}`))
			return
		}
		glog.V(3).Info("Got metric types: %s", d)
		metricCreationsMapMutex.Lock()
		defer metricCreationsMapMutex.Unlock()
		ret := []protocoltypes.SignalfxMetricCreationResponse{}
		for _, m := range d {
			metricType, ok := com_signalfuse_metrics_protobuf.MetricType_value[m.MetricType]
			if !ok {
				writter.WriteHeader(http.StatusBadRequest)
				writter.Write([]byte(`{msg:"Invalid metric type"}`))
				return
			}
			metricCreationsMap[m.MetricName] = com_signalfuse_metrics_protobuf.MetricType(metricType)
			ret = append(ret, protocoltypes.SignalfxMetricCreationResponse{Code: 409})
		}
		toWrite, err := jsonXXXMarshal(ret)
		if err != nil {
			glog.Warningf("Unable to marshal json: %s", err)
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Unable to marshal json!"}`))
			return
		}
		writter.WriteHeader(http.StatusOK)
		writter.Write([]byte(toWrite))
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
		"/v1/metric",
		metricHandler)
	mux.HandleFunc(
		"/metric",
		metricHandler)
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
		metricCreationsMap:      metricCreationsMap,
		metricCreationsMapMutex: metricCreationsMapMutex,
	}
	go server.Serve(listener)
	return &listenServer, err
}
