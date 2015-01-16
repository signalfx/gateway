package statuspage

import (
	"encoding/json"
	"fmt"
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"net/http"
	"os"
	"strings"
)

// ProxyStatusPage holds status information about the running process that you can expose with
// an HTTP endpoint
type ProxyStatusPage interface {
	StatusPage() http.HandlerFunc
	HealthPage() http.HandlerFunc
}

type proxyStatusPageImpl struct {
	loadedConfig *config.LoadedConfig
	statKeepers  []core.StatKeeper
}

// NewProxyStatusPage returns a new ProxyStatusPage that will expose the given config and stats from
// statKeepers
func NewProxyStatusPage(loadedConfig *config.LoadedConfig, statKeepers []core.StatKeeper) ProxyStatusPage {
	return &proxyStatusPageImpl{
		loadedConfig: loadedConfig,
		statKeepers:  statKeepers,
	}
}

func (proxy *proxyStatusPageImpl) StatusPage() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		stats := []core.Datapoint{}
		for _, keeper := range proxy.statKeepers {
			stats = append(stats, keeper.GetStats()...)
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
