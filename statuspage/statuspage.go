package statuspage

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"runtime"

	"github.com/signalfx/metricproxy/config"
	"github.com/signalfx/metricproxy/datapoint"
	"github.com/signalfx/metricproxy/stats"
)

// ProxyStatusPage holds status information about the running process that you can expose with
// an HTTP endpoint
type ProxyStatusPage interface {
	StatusPage() http.HandlerFunc
	HealthPage() http.HandlerFunc
}

type proxyStatusPageImpl struct {
	loadedConfig *config.ProxyConfig
	statKeepers  []stats.Keeper
}

// New returns a new ProxyStatusPage that will expose the given config and stats from
// statKeepers
func New(loadedConfig *config.ProxyConfig, statKeepers []stats.Keeper) ProxyStatusPage {
	return &proxyStatusPageImpl{
		loadedConfig: loadedConfig,
		statKeepers:  statKeepers,
	}
}

func (proxy *proxyStatusPageImpl) StatusPage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write(([]byte)(fmt.Sprintf("Golang version: %s\n", runtime.Version())))
		w.Write(([]byte)("Loaded config (raw):\n"))
		w.Write(([]byte)(fmt.Sprintf("%+v", proxy.loadedConfig)))

		bytes, err := json.MarshalIndent(*proxy.loadedConfig, "", "  ")
		if err == nil {
			w.Write(([]byte)("\nLoaded config (json):\n"))
			w.Write(bytes)
		}

		w.Write(([]byte)("\nArgs:\n"))
		w.Write(([]byte)(strings.Join(os.Args, "\n")))
		w.Write(([]byte)("\nStats:\n"))
		stats := []datapoint.Datapoint{}
		for _, keeper := range proxy.statKeepers {
			stats = append(stats, keeper.Stats()...)
		}
		for _, stat := range stats {
			w.Write(([]byte)(stat.String() + "\n"))
		}
	}
}

func (proxy *proxyStatusPageImpl) HealthPage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write(([]byte)("OK"))
	}
}
