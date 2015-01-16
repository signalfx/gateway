package statuspage

import (
	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/core"
	"github.com/signalfuse/signalfxproxy/stats"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthPage(t *testing.T) {
	w := httptest.NewRecorder()
	r := http.Request{}
	page := NewProxyStatusPage(nil, nil)
	page.HealthPage()(w, &r)
	assert.NotNil(t, page)
	assert.Equal(t, "OK", w.Body.String())
}

func TestStatusPage(t *testing.T) {
	w := httptest.NewRecorder()
	r := http.Request{}
	loadedConfig := config.LoadedConfig{}
	page := NewProxyStatusPage(&loadedConfig, []core.StatKeeper{stats.NewGolangStatKeeper()})
	page.StatusPage()(w, &r)
	assert.NotNil(t, page)
	assert.Contains(t, w.Body.String(), "Args")
}
