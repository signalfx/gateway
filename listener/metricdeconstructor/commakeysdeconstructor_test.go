package metricdeconstructor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommaKeysLoaderDeconstructor(t *testing.T) {
	i := &commaKeysLoaderDeconstructor{}
	m, d, e := i.Parse("original.metric[host:bob]")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metric", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob"}, d, "Should get dimensions")

	m, d, e = i.Parse("original.metric[host:bob,testing,type:dev]")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metric", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob", "type": "dev"}, d, "Should get dimensions")

	m, d, e = i.Parse("original.metric[host:bob,testing,type:dev].count")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metric.count", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob", "type": "dev"}, d, "Should get dimensions")

	m, d, e = i.Parse("original.metric[host:bob:bob2,testing,type:dev].count")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metric.count", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob:bob2", "type": "dev"}, d, "Should get dimensions")

	i = &commaKeysLoaderDeconstructor{colonInKey: true}
	m, d, e = i.Parse("original.metric[host:bob:bob2,testing,type:dev].count")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metric.count", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host:bob": "bob2", "type": "dev"}, d, "Should get dimensions")
}

func TestCommaKeysLoaderDeconstructorMissingBracket(t *testing.T) {
	i := &commaKeysLoaderDeconstructor{}
	m, d, e := i.Parse("original.metric[host:bob")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metric[host:bob", m, "Should get metric back")
	assert.Equal(t, map[string]string{}, d, "Should get dimensions")

	m, d, e = i.Parse("original.metrichost:bob]")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metrichost:bob]", m, "Should get metric back")
	assert.Equal(t, map[string]string{}, d, "Should get dimensions")

	m, d, e = i.Parse("original.metric[")
	assert.Equal(t, nil, e, "Should get no errors")
	assert.Equal(t, "original.metric[", m, "Should get metric back")
	assert.Equal(t, map[string]string{}, d, "Should get dimensions")
}
