package metricdeconstructor

import (
	"github.com/cep21/gohelpers/a"
	"testing"
)

func TestLoad(t *testing.T) {
	m, err := Load("", "ignored")
	a.ExpectNotNil(t, m)
	a.ExpectNil(t, err)

	m, err = Load("commakeys", "unknown")
	a.ExpectNil(t, m)
	a.ExpectNotNil(t, err)

	m, err = Load("commakeys", "")
	a.ExpectNotNil(t, m)
	a.ExpectNil(t, err)

	m, err = Load("NOTFOUND", "ignored")
	a.ExpectNil(t, m)
	a.ExpectNotNil(t, err)
}
