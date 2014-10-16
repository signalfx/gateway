package metricdeconstructor

import (
	"github.com/cep21/gohelpers/a"
	"testing"
)

func TestLoad(t *testing.T) {
	m, err := Load("", "ignored")
	a.ExpectNotEquals(t, m, nil, "Expected non nil error")
	a.ExpectEquals(t, err, nil, "Expected non nil error")

	m, err = Load("datadog", "ignored")
	a.ExpectNotEquals(t, m, nil, "Expected non nil error")
	a.ExpectEquals(t, err, nil, "Expected non nil error")

	m, err = Load("NOTFOUND", "ignored")
	a.ExpectEquals(t, m, nil, "Expected non nil error")
	a.ExpectNotEquals(t, err, nil, "Expected non nil error")
}
