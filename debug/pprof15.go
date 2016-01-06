// +build go1.5

package debug

import (
	"github.com/gorilla/mux"
	"net/http/pprof"
)

func setupTrace(m *mux.Router) {
	m.PathPrefix("/debug/pprof/trace").HandlerFunc(pprof.Trace)
}
