package listener

import (
	"net"
	"net/http"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/jsonengines"
	"github.com/gorilla/mux"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
	"github.com/codegangsta/negroni"
	"fmt"
)

type collectdListenerServer struct {
	name                  string
	listener              net.Listener
	server                http.Server
	collectdDecoder collectdJsonDecoder
}

func (streamer *collectdListenerServer) GetStats() []core.Datapoint {
	ret := []core.Datapoint{}
	ret = append(ret, streamer.collectdDecoder.GetStats(map[string]string{"listener": streamer.name})...)
	return ret
}

func (streamer *collectdListenerServer) Close() {
	streamer.listener.Close()
}

type collectdJsonDecoder struct {
	TotalErrors int64
	decodingEngine        jsonengines.JSONDecodingEngine
	datapointTracker DatapointTracker
	metricTracker MetricTrackingMiddleware
}

func (decoder *collectdJsonDecoder) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	d, err := decoder.decodingEngine.DecodeCollectdJSONWriteBody(req.Body)
	if err != nil {
		atomic.AddInt64(&decoder.TotalErrors, 1)
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(fmt.Sprintf("Unable to decode json: %s", err.Error())))
		return
	}
	for _, f := range d {
		if f.TypeS != nil && f.Time != nil {
			for i := range f.Dsnames {
				if i < len(f.Dstypes) && i < len(f.Values) {
					decoder.datapointTracker.AddDatapoint(protocoltypes.NewCollectdDatapoint(f, uint(i)))
				}
			}
		}
	}
	rw.Write([]byte(`"OK"`))
}

func (decoder *collectdJsonDecoder) GetStats(dimensions map[string]string) []core.Datapoint {
	ret := []core.Datapoint{}
	ret = append(ret, decoder.datapointTracker.GetStats(dimensions)...)
	ret = append(ret, decoder.metricTracker.GetStats(dimensions)...)
	return ret
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

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter()

	listenServer := collectdListenerServer{
		listener:              listener,
		server:                http.Server{
			Handler:      r,
			Addr:         listenAddr,
			ReadTimeout:  clientTimeout,
			WriteTimeout: clientTimeout,
		},
		collectdDecoder: collectdJsonDecoder {
			decodingEngine: decodingEngine,
			datapointTracker: DatapointTracker {
				DatapointStreamingAPI: DatapointStreamingAPI,
			},
		},
	}

	baseNegroni := negroni.New(&listenServer.collectdDecoder.metricTracker)
	baseNegroni.UseHandler(&listenServer.collectdDecoder)
	r.Path(listenPath).Headers("Content-type", "application/json").Handler(baseNegroni)

	go listenServer.server.Serve(listener)
	return &listenServer, err
}
