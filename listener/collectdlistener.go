package listener

import (
	"net"
	"net/http"
	"sync/atomic"
	"time"

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

type collectdListenerServer struct {
	name                  string
	listener              net.Listener
	server                *http.Server
	datapointStreamingAPI core.DatapointStreamingAPI
	decodingEngine        jsonengines.JSONDecodingEngine

	activeConnections int64
	totalConnections  int64
	totalDatapoints   int64
	invalidRequests   int64
}

func (streamer *collectdListenerServer) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	stats := map[string]int64{
		"total_datapoints":   atomic.LoadInt64(&streamer.totalDatapoints),
		"invalid_requests":   atomic.LoadInt64(&streamer.invalidRequests),
		"total_connections":  atomic.LoadInt64(&streamer.totalConnections),
		"active_connections": atomic.LoadInt64(&streamer.activeConnections),
	}
	for k, v := range stats {
		var t com_signalfuse_metrics_protobuf.MetricType
		if k == "active_connections" {
			t = com_signalfuse_metrics_protobuf.MetricType_GAUGE
		} else {
			t = com_signalfuse_metrics_protobuf.MetricType_CUMULATIVE_COUNTER
		}
		ret = append(
			ret,
			protocoltypes.NewOnHostDatapointDimensions(
				k,
				value.NewIntWire(v),
				t,
				map[string]string{"listener": streamer.name}))
	}
	return ret
}

func (streamer *collectdListenerServer) Close() {
	streamer.listener.Close()
}

func (streamer *collectdListenerServer) jsonDecode(req *http.Request) error {
	atomic.AddInt64(&streamer.totalConnections, 1)
	atomic.AddInt64(&streamer.activeConnections, 1)
	defer atomic.AddInt64(&streamer.activeConnections, -1)
	//	dec := json.NewDecoder(req.Body)
	d, err := streamer.decodingEngine.DecodeCollectdJSONWriteBody(req.Body)
	//	var d protocoltypes.CollectdJSONWriteBody
	if err != nil {
		atomic.AddInt64(&streamer.invalidRequests, 1)
		return err
	}
	for _, f := range d {
		if f.TypeS != nil && f.Time != nil {
			for i := range f.Dsnames {
				if i < len(f.Dstypes) && i < len(f.Values) {
					atomic.AddInt64(&streamer.totalDatapoints, 1)
					streamer.datapointStreamingAPI.DatapointsChannel() <- protocoltypes.NewCollectdDatapoint(f, uint(i))
				}
			}
		}
	}
	return nil
}

func (streamer *collectdListenerServer) handleCollectd(writter http.ResponseWriter, req *http.Request) {
	knownTypes := map[string]func(*http.Request) error{
		"application/json": streamer.jsonDecode,
	}
	contentType := req.Header.Get("Content-type")
	decoderFunc, ok := knownTypes[contentType]

	if !ok {
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
	}
}

var defaultCollectdConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:8081"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	ListenPath:      workarounds.GolangDoesnotAllowPointerToStringLiteral("/post-collectd"),
	JSONEngine:      workarounds.GolangDoesnotAllowPointerToStringLiteral("native"),
}

// CollectdListenerLoader loads a listener for collectd write_http protocol
func CollectdListenerLoader(DatapointStreamingAPI core.DatapointStreamingAPI, listenFrom *config.ListenFrom) (DatapointListener, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultCollectdConfig)
	log.WithField("listenFrom", listenFrom).Info("Creating signalfx listener using final config")
	engine, err := jsonengines.Load(*listenFrom.JSONEngine)
	if err != nil {
		return nil, err
	}
	return StartListeningCollectDHTTPOnPort(DatapointStreamingAPI, *listenFrom.ListenAddr, *listenFrom.ListenPath, *listenFrom.TimeoutDuration, engine)
}

// StartListeningCollectDHTTPOnPort servers http collectd requests
func StartListeningCollectDHTTPOnPort(DatapointStreamingAPI core.DatapointStreamingAPI,
	listenAddr string, listenPath string, clientTimeout time.Duration, decodingEngine jsonengines.JSONDecodingEngine) (DatapointListener, error) {
	mux := http.NewServeMux()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Handler:      mux,
		Addr:         listenAddr,
		ReadTimeout:  clientTimeout,
		WriteTimeout: clientTimeout,
	}

	listenServer := collectdListenerServer{
		listener:              listener,
		server:                server,
		datapointStreamingAPI: DatapointStreamingAPI,
		decodingEngine:        decodingEngine,
	}

	mux.HandleFunc(
		listenPath,
		listenServer.handleCollectd)

	go server.Serve(listener)
	return &listenServer, err
}
