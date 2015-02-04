package listener

import (
	"net"
	"net/http"
	"sync/atomic"
	"time"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/jsonengines"
	"github.com/signalfuse/signalfxproxy/protocoltypes"
)

type collectdListenerServer struct {
	name        string
	listener    net.Listener
	server      http.Server
	statKeepers []core.StatKeeper
}

func (streamer *collectdListenerServer) GetStats() []core.Datapoint {
	return core.CombineStats(streamer.statKeepers)
}

func (streamer *collectdListenerServer) Close() {
	streamer.listener.Close()
}

type collectdJSONDecoder struct {
	TotalErrors      int64
	decodingEngine   jsonengines.JSONDecodingEngine
	datapointTracker DatapointTracker
}

func (decoder *collectdJSONDecoder) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
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

// Usually want "listener: "<Name>"
func (decoder *collectdJSONDecoder) GetStats(dimensions map[string]string) []core.Datapoint {
	ret := []core.Datapoint{}
	ret = append(ret, decoder.datapointTracker.GetStats(dimensions)...)
	return ret
}

var defaultCollectdConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:8081"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	ListenPath:      workarounds.GolangDoesnotAllowPointerToStringLiteral("/post-collectd"),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("collectd"),
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
	return StartListeningCollectDHTTPOnPort(DatapointStreamingAPI, *listenFrom.ListenAddr, *listenFrom.ListenPath, *listenFrom.TimeoutDuration, engine, *listenFrom.Name)
}

// StartListeningCollectDHTTPOnPort servers http collectd requests
func StartListeningCollectDHTTPOnPort(DatapointStreamingAPI core.DatapointStreamingAPI,
	listenAddr string, listenPath string, clientTimeout time.Duration, decodingEngine jsonengines.JSONDecodingEngine, name string) (DatapointListener, error) {

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter()

	collectdDecoder := collectdJSONDecoder{
		decodingEngine: decodingEngine,
		datapointTracker: DatapointTracker{
			DatapointStreamingAPI: DatapointStreamingAPI,
		},
	}

	listenServer := collectdListenerServer{
		name:     name,
		listener: listener,
		server: http.Server{
			Handler:      r,
			Addr:         listenAddr,
			ReadTimeout:  clientTimeout,
			WriteTimeout: clientTimeout,
		},
	}

	middleware := MetricTrackingMiddleware{}

	listenServer.statKeepers = append(listenServer.statKeepers, &core.StatKeeperWrap{
		Base:       []core.DimensionStatKeeper{&middleware, &collectdDecoder},
		Dimensions: map[string]string{"listener": name},
	})

	n := negroni.New(&middleware, negroni.Wrap(&collectdDecoder))
	r.Path(listenPath).Headers("Content-type", "application/json").Handler(n)

	go listenServer.server.Serve(listener)
	return &listenServer, err
}
