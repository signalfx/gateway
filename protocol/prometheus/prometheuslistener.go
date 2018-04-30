package prometheus

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/gorilla/mux"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/metricproxy/protocol"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Server is the prometheus server
type Server struct {
	protocol.CloseableHealthCheck
	listener  net.Listener
	server    http.Server
	collector sfxclient.Collector
	decoder   *decoder
}

// Close the socket currently open for collectd JSON connections
func (s *Server) Close() error {
	return s.listener.Close()
}

// Datapoints returns decoder datapoints
func (s *Server) Datapoints() []*datapoint.Datapoint {
	return append(s.collector.Datapoints(), s.HealthDatapoints()...)
}

var _ protocol.Listener = &Server{}

type decoder struct {
	TotalErrors        int64
	TotalNaNs          int64
	TotalBadDatapoints int64
	Bucket             *sfxclient.RollingBucket
	DrainSize          *sfxclient.RollingBucket
	SendTo             dpsink.Sink
	Logger             log.Logger
	readAll            func(r io.Reader) ([]byte, error)
}

func getDimensions(labels []*prompb.Label) map[string]string {
	dims := make(map[string]string, len(labels))
	for _, l := range labels {
		dims[l.Name] = l.Value
	}
	return dims
}

func getMetricName(dims map[string]string) string {
	for k, v := range dims {
		if k == model.MetricNameLabel {
			delete(dims, k)
			return v
		}
	}
	return ""
}

// types are encoded into metric names for more information see below
// https://prometheus.io/docs/practices/naming/
// https://prometheus.io/docs/concepts/metric_types/
// https://prometheus.io/docs/instrumenting/writing_exporters/#metrics
// https://prometheus.io/docs/practices/histograms/
func getMetricType(metric string) datapoint.MetricType {
	// _total is a convention for counters, you should use it if you’re using the COUNTER type.
	if strings.HasSuffix(metric, "_total") {
		return datapoint.Counter
	}
	// cumulative counters for the observation buckets, exposed as <basename>_bucket{le="<upper inclusive bound>"}
	if strings.HasSuffix(metric, "_bucket") {
		return datapoint.Counter
	}
	// the count of events that have been observed, exposed as <basename>_count
	if strings.HasSuffix(metric, "_count") {
		return datapoint.Counter
	}
	// _sum acts mostly like a counter, but can contain negative observations so must be sent in as a gauge
	// so everythign else is a gauge
	return datapoint.Gauge
}

func (d *decoder) getDatapoints(ts *prompb.TimeSeries) []*datapoint.Datapoint {
	dimensions := getDimensions(ts.Labels)
	metricName := getMetricName(dimensions)
	if metricName == "" {
		atomic.AddInt64(&d.TotalBadDatapoints, int64(len(ts.Samples)))
		return []*datapoint.Datapoint{}
	}
	metricType := getMetricType(metricName)

	dps := make([]*datapoint.Datapoint, 0, len(ts.Samples))
	for _, s := range ts.Samples {
		if math.IsNaN(s.Value) {
			atomic.AddInt64(&d.TotalNaNs, 1)
			continue
		}
		var value datapoint.Value
		if s.Value == float64(int64(s.Value)) {
			value = datapoint.NewIntValue(int64(s.Value))
		} else {
			value = datapoint.NewFloatValue(s.Value)
		}
		timestamp := time.Unix(0, int64(time.Millisecond)*s.Timestamp)
		dps = append(dps, datapoint.New(metricName, dimensions, value, metricType, timestamp))
	}
	return dps
}

// ServeHTTPC decodes datapoints for the connection and sends them to the decoder's sink
func (d *decoder) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, req *http.Request) {
	start := time.Now()
	defer d.Bucket.Add(float64(time.Now().Sub(start).Nanoseconds()))
	var err error
	var compressed []byte
	defer func() {
		if err != nil {
			atomic.AddInt64(&d.TotalErrors, 1)
			log.IfErr(d.Logger, err)
		}
	}()
	compressed, err = d.readAll(req.Body)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	var reqBuf []byte
	reqBuf, err = snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	var r prompb.WriteRequest
	if err = proto.Unmarshal(reqBuf, &r); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	dps := make([]*datapoint.Datapoint, 0, len(r.Timeseries))
	for _, ts := range r.Timeseries {
		datapoints := d.getDatapoints(ts)
		dps = append(dps, datapoints...)
	}

	d.DrainSize.Add(float64(len(dps)))
	if len(dps) > 0 {
		err = d.SendTo.AddDatapoints(ctx, dps)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

// Datapoints about this decoder, including how many datapoints it decoded
func (d *decoder) Datapoints() []*datapoint.Datapoint {
	dps := d.Bucket.Datapoints()
	dps = append(dps, d.DrainSize.Datapoints()...)
	dps = append(dps,
		sfxclient.Cumulative("prometheus.invalid_requests", nil, atomic.LoadInt64(&d.TotalErrors)),
		sfxclient.Cumulative("prometheus.total_NAN_samples", nil, atomic.LoadInt64(&d.TotalNaNs)),
		sfxclient.Cumulative("prometheus.total_bad_datapoints", nil, atomic.LoadInt64(&d.TotalBadDatapoints)),
	)
	return dps
}

// Config controls optional parameters for collectd listeners
type Config struct {
	ListenAddr      *string
	ListenPath      *string
	Timeout         *time.Duration
	StartingContext context.Context
	HealthCheck     *string
	HTTPChain       web.NextConstructor
	Logger          log.Logger
}

var defaultConfig = &Config{
	ListenAddr:      pointer.String("127.0.0.1:1234"),
	ListenPath:      pointer.String("/write"),
	Timeout:         pointer.Duration(time.Second * 30),
	HealthCheck:     pointer.String("/healthz"),
	Logger:          log.Discard,
	StartingContext: context.Background(),
}

// NewListener serves http prometheus requests
func NewListener(sink dpsink.Sink, passedConf *Config) (*Server, error) {
	conf := pointer.FillDefaultFrom(passedConf, defaultConfig).(*Config)

	listener, err := net.Listen("tcp", *conf.ListenAddr)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter()
	metricTracking := &web.RequestCounter{}
	fullHandler := web.NewHandler(conf.StartingContext, web.FromHTTP(r))
	if conf.HTTPChain != nil {
		fullHandler.Add(web.NextHTTP(metricTracking.ServeHTTP))
		fullHandler.Add(conf.HTTPChain)
	}
	decoder := decoder{
		SendTo:  sink,
		Logger:  conf.Logger,
		readAll: ioutil.ReadAll,
		Bucket: sfxclient.NewRollingBucket("request_time.ns", map[string]string{
			"endpoint":  "prometheus",
			"direction": "listener",
		}),
		DrainSize: sfxclient.NewRollingBucket("drain_size", map[string]string{
			"direction":   "forwarder",
			"destination": "signalfx",
		}),
	}
	listenServer := Server{
		listener: listener,
		server: http.Server{
			Handler:      fullHandler,
			Addr:         listener.Addr().String(),
			ReadTimeout:  *conf.Timeout,
			WriteTimeout: *conf.Timeout,
		},
		decoder: &decoder,
		collector: sfxclient.NewMultiCollector(
			metricTracking,
			&decoder),
	}
	listenServer.SetupHealthCheck(conf.HealthCheck, r, conf.Logger)
	httpHandler := web.NewHandler(conf.StartingContext, listenServer.decoder)
	SetupPrometheusPaths(r, httpHandler, *conf.ListenPath)

	go func() {
		log.IfErr(conf.Logger, listenServer.server.Serve(listener))
	}()
	return &listenServer, nil
}

// SetupPrometheusPaths tells the router which paths the given handler should handle
func SetupPrometheusPaths(r *mux.Router, handler http.Handler, endpoint string) {
	r.Path(endpoint).Methods("POST").Headers("Content-Type", "application/x-protobuf").Handler(handler)
}
