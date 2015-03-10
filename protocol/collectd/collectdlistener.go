package collectd

import (
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cep21/gohelpers/structdefaults"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/gorilla/mux"
	"github.com/signalfx/metricproxy/config"

	"github.com/codegangsta/negroni"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/reqcounter"
	"github.com/signalfx/metricproxy/stats"
)

type listenerServer struct {
	name        string
	listener    net.Listener
	server      http.Server
	statKeepers []stats.Keeper
}

func (streamer *listenerServer) Stats() []datapoint.Datapoint {
	return stats.CombineStats(streamer.statKeepers)
}

func (streamer *listenerServer) Close() {
	streamer.listener.Close()
}

// JSONDecoder can decode collectd's native JSON datapoint format
type JSONDecoder struct {
	TotalErrors      int64
	DecodingEngine   JSONEngine
	DatapointTracker datapoint.Tracker
}

// DecodeJSON of a collectd HTTP request using collectd's native JSON http protocol
func DecodeJSON(req *http.Request, decodingEngine JSONEngine, datapointTracker datapoint.Adder) error {
	d, err := decodingEngine.DecodeJSONWriteBody(req.Body)
	if err != nil {
		return err
	}
	for _, f := range d {
		if f.TypeS != nil && f.Time != nil {
			for i := range f.Dsnames {
				if i < len(f.Dstypes) && i < len(f.Values) {
					datapointTracker.AddDatapoint(NewCollectdDatapoint(f, uint(i)))
				}
			}
		}
	}
	return nil
}

func (decoder *JSONDecoder) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	err := DecodeJSON(req, decoder.DecodingEngine, &decoder.DatapointTracker)
	if err != nil {
		atomic.AddInt64(&decoder.TotalErrors, 1)
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(fmt.Sprintf("Unable to decode json: %s", err.Error())))
		return
	}
	rw.Write([]byte(`"OK"`))
}

// Stats about this decoder, including how many datapoints it decoded
func (decoder *JSONDecoder) Stats(dimensions map[string]string) []datapoint.Datapoint {
	ret := []datapoint.Datapoint{}
	ret = append(ret, decoder.DatapointTracker.Stats(dimensions)...)
	return ret
}

var defaultCollectdConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:8081"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	ListenPath:      workarounds.GolangDoesnotAllowPointerToStringLiteral("/post-collectd"),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("collectd"),
	JSONEngine:      workarounds.GolangDoesnotAllowPointerToStringLiteral("native"),
}

// ListenerLoader loads a listener for collectd write_http protocol
func ListenerLoader(Streamer datapoint.Streamer, listenFrom *config.ListenFrom) (stats.ClosableKeeper, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultCollectdConfig)
	log.WithField("listenFrom", listenFrom).Info("Creating signalfx listener using final config")
	engine, err := LoadEngine(*listenFrom.JSONEngine)
	if err != nil {
		return nil, err
	}
	return StartListeningCollectDHTTPOnPort(Streamer, *listenFrom.ListenAddr, *listenFrom.ListenPath, *listenFrom.TimeoutDuration, engine, *listenFrom.Name)
}

// StartListeningCollectDHTTPOnPort servers http collectd requests
func StartListeningCollectDHTTPOnPort(Streamer datapoint.Streamer,
	listenAddr string, listenPath string, clientTimeout time.Duration, decodingEngine JSONEngine, name string) (stats.ClosableKeeper, error) {

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter()

	collectdDecoder := JSONDecoder{
		DecodingEngine: decodingEngine,
		DatapointTracker: datapoint.Tracker{
			Streamer: Streamer,
		},
	}

	listenServer := listenerServer{
		name:     name,
		listener: listener,
		server: http.Server{
			Handler:      r,
			Addr:         listenAddr,
			ReadTimeout:  clientTimeout,
			WriteTimeout: clientTimeout,
		},
	}

	middleware := reqcounter.RequestCounter{}

	listenServer.statKeepers = append(listenServer.statKeepers, &stats.KeeperWrap{
		Base:       []stats.DimensionKeeper{&middleware, &collectdDecoder},
		Dimensions: map[string]string{"listener": name},
	})

	n := negroni.New(&middleware, negroni.Wrap(&collectdDecoder))
	r.Path(listenPath).Headers("Content-type", "application/json").Handler(n)

	go listenServer.server.Serve(listener)
	return &listenServer, err
}
