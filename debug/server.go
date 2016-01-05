package debug

import (
	"github.com/gorilla/mux"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/explorable"
	"github.com/signalfx/golib/expvar2"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/metricproxy/logkey"
	"net"
	"net/http"
	"net/http/pprof"
)

// Server exposes private debugging information
type Server struct {
	httpServer http.Server
	Exp2       *expvar2.Handler
	listener   net.Listener
}

// Config controls optional parameters for the debug server
type Config struct {
	Logger log.Logger
}

// DefaultConfig is used by default for unset config parameters
var DefaultConfig = &Config{
	Logger: log.DefaultLogger.CreateChild(),
}

// Close stops the listening HTTP server
func (s *Server) Close() error {
	return s.listener.Close()
}

// Addr returns the net address of the listening HTTP server
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// New creates a new debug server
func New(listenAddr string, explorableObj interface{}, conf *Config) (*Server, error) {
	conf = pointer.FillDefaultFrom(conf, DefaultConfig).(*Config)
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, errors.Annotatef(err, "unable to setup listening on %s", listenAddr)
	}
	m := mux.NewRouter()
	s := &Server{
		httpServer: http.Server{
			Addr:    listenAddr,
			Handler: m,
		},
		listener: l,
		Exp2:     expvar2.New(),
	}
	m.PathPrefix("/debug/pprof/cmdline").HandlerFunc(pprof.Cmdline)
	m.PathPrefix("/debug/pprof/profile").HandlerFunc(pprof.Profile)
	m.PathPrefix("/debug/pprof/trace").HandlerFunc(pprof.Trace)
	m.PathPrefix("/debug/pprof/symbol").HandlerFunc(pprof.Symbol)
	m.PathPrefix("/debug/pprof/").HandlerFunc(pprof.Index)
	e := &explorable.Handler{
		Val:      explorableObj,
		BasePath: "/debug/explorer/",
		Logger:   log.NewContext(conf.Logger).With(logkey.Protocol, "explorable"),
	}
	m.PathPrefix("/debug/explorer/").Handler(e)
	m.Path("/debug/vars").Handler(s.Exp2)
	go func() {
		err := s.httpServer.Serve(l)
		conf.Logger.Log(log.Err, err, "debug server finished")
	}()
	return s, nil
}
