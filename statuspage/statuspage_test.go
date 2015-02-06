package statuspage

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/signalfuse/signalfxproxy/config"
	"github.com/signalfuse/signalfxproxy/stats"
	"github.com/stretchr/testify/assert"
)

func TestHealthPage(t *testing.T) {
	w := httptest.NewRecorder()
	r := http.Request{}
	page := New(nil, nil)
	page.HealthPage()(w, &r)
	assert.NotNil(t, page)
	assert.Equal(t, "OK", w.Body.String())
}

func TestStatusPage(t *testing.T) {
	w := httptest.NewRecorder()
	r := http.Request{}
	loadedConfig := config.ProxyConfig{}
	page := New(&loadedConfig, []stats.Keeper{stats.NewGolangKeeper()})
	page.StatusPage()(w, &r)
	assert.NotNil(t, page)
	assert.Contains(t, w.Body.String(), "Args")
}
