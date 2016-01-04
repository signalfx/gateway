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

type Server struct {
	httpServer http.Server
	Exp2       *expvar2.Handler
	listener   net.Listener
}

type Config struct {
	Logger log.Logger
}

var DefaultConfig = &Config{
	Logger: log.DefaultLogger.CreateChild(),
}

func (s *Server) Close() error {
	return s.listener.Close()
}

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
