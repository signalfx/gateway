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

func TestLoadJSON(t *testing.T) {
	m, err := LoadJSON("delimiter", make(map[string]interface{}))
	assert.NotNil(t, m)
	assert.NoError(t, err)

	m, err = LoadJSON("NOTFOUND", make(map[string]interface{}))
	assert.Nil(t, m)
	assert.Error(t, err)
}
