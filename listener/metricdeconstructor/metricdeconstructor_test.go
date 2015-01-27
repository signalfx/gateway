package metricdeconstructor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	m, err := Load("", "ignored")
	assert.NotNil(t, m)
	assert.Nil(t, err)

	m, err = Load("commakeys", "unknown")
	assert.Nil(t, m)
	assert.Error(t, err)

	m, err = Load("commakeys", "")
	assert.NotNil(t, m)
	assert.Nil(t, err)

	m, err = Load("NOTFOUND", "ignored")
	assert.Nil(t, m)
	assert.Error(t, err)
}
