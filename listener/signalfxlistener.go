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
	"time"
	"sync"
)

type listenerServer struct {
	listener net.Listener
	metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType
	metricCreationsMapMutex *sync.Mutex

}

func (streamer *listenerServer) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	return ret
}

func (streamer *listenerServer) Close() {
	streamer.listener.Close()
}

func jsonDecoderFunction(DatapointStreamingAPI core.DatapointStreamingAPI, metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex) func(*http.Request) error {
	return func(req *http.Request) error {
		dec := json.NewDecoder(req.Body)
		for {
			var d protocoltypes.SignalfxJsonDatapointV1
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
				mt := func() (com_signalfuse_metrics_protobuf.MetricType) {
					metricCreationsMapMutex.Lock()
					defer metricCreationsMapMutex.Unlock()
					mt, ok := metricCreationsMap[d.Metric]
					if !ok {
						return com_signalfuse_metrics_protobuf.MetricType_GAUGE
					} else {
						return mt
					}

				}()
				DatapointStreamingAPI.DatapointsChannel() <- core.NewRelativeTimeDatapoint(d.Metric, map[string]string{"sf_source": d.Source}, value.NewFloatWire(d.Value), mt, 0)
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
		if err != nil {
			return buf, err
		}
		if n == 0 {
			return buf, nil
		}
		totalBytesRead += uint64(n)
	}
	return buf, nil
}

func protobufDecoderFunction(DatapointStreamingAPI core.DatapointStreamingAPI, metricCreationsMap map[string]com_signalfuse_metrics_protobuf.MetricType, metricCreationsMapMutex *sync.Mutex) func(*http.Request) error {
	return func(req *http.Request) error {
		bufferedBody := bufio.NewReaderSize(req.Body, 32768)
		for {
			buf, err := bufferedBody.Peek(16) // should be big enough for any varint
			if err == io.EOF {
				return nil
			}

			if err != nil {
				glog.Infof("Got %s", err)
				return err
			}
			num, bytesRead := proto.DecodeVarint(buf)
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
			dp := func() (core.Datapoint) {
				metricCreationsMapMutex.Lock()
				defer metricCreationsMapMutex.Unlock()
				mt, ok := metricCreationsMap[msg.GetMetric()]
				if !ok {
					return protocoltypes.NewProtobufDataPoint(msg)
				} else {
					return protocoltypes.NewProtobufDataPointWithType(msg, mt)
				}

			}()
			DatapointStreamingAPI.DatapointsChannel() <- dp
		}
		return nil
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

// StartServingHTTPOnPort servers http requests for Signalfx datapoints
func StartServingHTTPOnPort(listenAddr string, DatapointStreamingAPI core.DatapointStreamingAPI, clientTimeout time.Duration) (DatapointListener, error) {
	mux := http.NewServeMux()
	metricCreationsMap := make(map[string]com_signalfuse_metrics_protobuf.MetricType)
	metricCreationsMapMutex := &sync.Mutex{}
	datapointHandler := func(writter http.ResponseWriter, req *http.Request) {
		contentType := req.Header.Get("Content-type")
		var decoderFunc func(*http.Request) error
		if contentType == "" || contentType == "application/json" {
			decoderFunc = jsonDecoderFunction(DatapointStreamingAPI, metricCreationsMap, metricCreationsMapMutex)
		} else if contentType == "" || contentType == "application/x-protobuf" {
			decoderFunc = protobufDecoderFunction(DatapointStreamingAPI, metricCreationsMap, metricCreationsMapMutex)
		} else {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Unknown content type"}`))
			return
		}
		err := decoderFunc(req)
		if err != nil {
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(err.Error()))
		} else {
			writter.WriteHeader(http.StatusOK)
			writter.Write([]byte(`"OK"`))
		}
	}

	metricHandler := func(writter http.ResponseWriter, req *http.Request) {
		dec := json.NewDecoder(req.Body)
		var d []protocoltypes.SignalfxMetricCreationStruct
		if err := dec.Decode(&d); err == io.EOF {
			return
		} else if err != nil {
			glog.Infof("Invalid metric creation request: %s", err)
			writter.WriteHeader(http.StatusBadRequest)
			writter.Write([]byte(`{msg:"Invalid creation request"}`))
			return
		} else {
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
			toWrite, err := json.Marshal(ret)
			if err != nil {
				glog.Warningf("Unable to marshal json: %s", err)
				writter.WriteHeader(http.StatusBadRequest)
				writter.Write([]byte(`{msg:"Unable to marshal json!"}`))
				return
			}
			writter.WriteHeader(http.StatusOK)
			writter.Write([]byte(toWrite))
		}
	}
	mux.HandleFunc(
		"/datapoint",
		datapointHandler)
	mux.HandleFunc(
		"/v1/datapoint",
		datapointHandler)
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
		listener: listener,
		metricCreationsMap: metricCreationsMap,
		metricCreationsMapMutex: metricCreationsMapMutex,
	}
	go server.Serve(listener)
	return &listenServer, err
}
