// +build !go1.5

package debug

import (
	"github.com/gorilla/mux"
)

func setupTrace(m *mux.Router) {
	// Ignored.  Trace not supported in < 1.5
}
