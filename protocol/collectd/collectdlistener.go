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

	"strings"

	"encoding/json"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/protocol"
	"github.com/signalfx/metricproxy/stats"
	"golang.org/x/net/context"
)

// ListenerServer will listen for collectd datapoint connections
type ListenerServer struct {
	stats.Keeper
	name     string
	listener net.Listener
	server   http.Server
}

var _ protocol.Listener = &ListenerServer{}

// Close the socket currently open for collectd JSON connections
func (streamer *ListenerServer) Close() error {
	return streamer.listener.Close()
}

// JSONDecoder can decode collectd's native JSON datapoint format
type JSONDecoder struct {
	SendTo      dpsink.Sink
	DefaultDims map[string]string

	TotalErrors    int64
	TotalBlankDims int64
}

const sfxDimQueryParamPrefix string = "sfxdim_"

// ServeHTTPC decodes datapoints for the connection and sends them to the decoder's sink
func (decoder *JSONDecoder) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, req *http.Request) {
	err := decoder.Read(ctx, req)
	if err != nil {
		atomic.AddInt64(&decoder.TotalErrors, 1)
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte(fmt.Sprintf("Unable to decode json: %s", err.Error())))
		return
	}
	rw.Write([]byte(`"OK"`))
}

func newDataPoints(f *JSONWriteFormat, defaultDims map[string]string) []*datapoint.Datapoint {
	dps := make([]*datapoint.Datapoint, 0, len(f.Dsnames))
	for i := range f.Dsnames {
		if i < len(f.Dstypes) && i < len(f.Values) && f.Values[i] != nil {
			dps = append(dps, NewDatapoint(f, uint(i), defaultDims))
		}
	}
	return dps
}

func newEvent(f *JSONWriteFormat, defaultDims map[string]string) *event.Event {
	if f.Time != nil && f.Severity != nil && f.Message != nil {
		return NewEvent(f, defaultDims)
	}
	return nil
}

func (decoder *JSONDecoder) Read(ctx context.Context, req *http.Request) error {
	defaultDims := decoder.defaultDims(req)
	var d JSONWriteBody
	err := json.NewDecoder(req.Body).Decode(&d)
	if err != nil {
		return err
	}
	es := make([]*event.Event, 0, len(d)*2)
	dps := make([]*datapoint.Datapoint, 0, len(d)*2)
	for _, f := range d {
		if e := newEvent(f, defaultDims); e == nil {
			dps = append(dps, newDataPoints(f, defaultDims)...)
		} else {
			es = append(es, e)
		}
	}
	derr := decoder.SendTo.AddDatapoints(ctx, dps)
	eerr := decoder.SendTo.AddEvents(ctx, es)
	if derr != nil {
		return derr
	}
	return eerr
}

func (decoder *JSONDecoder) defaultDims(req *http.Request) map[string]string {
	params := req.URL.Query()
	defaultDims := make(map[string]string, len(decoder.DefaultDims))
	for key := range params {
		if strings.HasPrefix(key, sfxDimQueryParamPrefix) {
			value := params.Get(key)
			if len(value) == 0 {
				atomic.AddInt64(&decoder.TotalBlankDims, 1)
				continue
			}
			key = key[len(sfxDimQueryParamPrefix):]
			defaultDims[key] = value
		}
	}
	for k, v := range decoder.DefaultDims {
		defaultDims[k] = v
	}
	return defaultDims
}

// Stats about this decoder, including how many datapoints it decoded
func (decoder *JSONDecoder) Stats(dimensions map[string]string) []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		datapoint.New("total_blank_dims", dimensions, datapoint.NewIntValue(decoder.TotalBlankDims), datapoint.Counter, time.Now()),
		datapoint.New("invalid_collectd_json", dimensions, datapoint.NewIntValue(decoder.TotalErrors), datapoint.Counter, time.Now()),
	}
}

var defaultCollectdConfig = &config.ListenFrom{
	ListenAddr:      workarounds.GolangDoesnotAllowPointerToStringLiteral("127.0.0.1:8081"),
	TimeoutDuration: workarounds.GolangDoesnotAllowPointerToTimeLiteral(time.Second * 30),
	ListenPath:      workarounds.GolangDoesnotAllowPointerToStringLiteral("/post-collectd"),
	Name:            workarounds.GolangDoesnotAllowPointerToStringLiteral("collectd"),
	JSONEngine:      workarounds.GolangDoesnotAllowPointerToStringLiteral("native"),
}

// ListenerLoader loads a listener for collectd write_http protocol
func ListenerLoader(ctx context.Context, sink dpsink.Sink, listenFrom *config.ListenFrom) (*ListenerServer, error) {
	structdefaults.FillDefaultFrom(listenFrom, defaultCollectdConfig)
	log.WithField("listenFrom", listenFrom).Info("Creating listener using final config")
	return StartListeningCollectDHTTPOnPort(ctx, sink, *listenFrom.ListenAddr, *listenFrom.ListenPath, *listenFrom.TimeoutDuration, *listenFrom.Name, listenFrom.Dimensions)
}

// StartListeningCollectDHTTPOnPort servers http collectd requests
func StartListeningCollectDHTTPOnPort(ctx context.Context, sink dpsink.Sink,
	listenAddr string, listenPath string, clientTimeout time.Duration, name string, defaultDims map[string]string) (*ListenerServer, error) {

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	h, st := SetupHandler(ctx, name, sink, defaultDims)

	r := mux.NewRouter()
	r.Path(listenPath).Headers("Content-type", "application/json").Handler(h)

	listenServer := ListenerServer{
		Keeper:   st,
		name:     name,
		listener: listener,
		server: http.Server{
			Handler:      r,
			Addr:         listenAddr,
			ReadTimeout:  clientTimeout,
			WriteTimeout: clientTimeout,
		},
	}

	go listenServer.server.Serve(listener)
	return &listenServer, nil
}

// SetupHandler is shared between signalfx and here to setup listening for collectd connections.
// Will do shared basic setup like configuring request counters
func SetupHandler(ctx context.Context, name string, sink dpsink.Sink, defaultDims map[string]string) (*web.Handler, stats.Keeper) {
	metricTracking := web.RequestCounter{}
	counter := &dpsink.Counter{}
	collectdDecoder := JSONDecoder{
		SendTo:      dpsink.FromChain(sink, dpsink.NextWrap(counter)),
		DefaultDims: defaultDims,
	}
	h := web.NewHandler(ctx, &collectdDecoder).Add(web.NextHTTP(metricTracking.ServeHTTP))
	st := stats.ToKeeperMany(protocol.ListenerDims(name, "collectd"), &metricTracking, counter, &collectdDecoder)
	return h, st
}
