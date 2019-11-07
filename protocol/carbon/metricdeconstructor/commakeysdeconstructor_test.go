package metricdeconstructor

import (
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/stretchr/testify/assert"
)

func TestCommaKeysLoaderDeconstructor(t *testing.T) {
	i, e := commaKeysLoader("")
	assert.NoError(t, e)
	m, mt, d, e := i.Parse("original.metric[host:bob]")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, "original.metric", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob"}, d, "Should get dimensions")

	m, mt, d, e = i.Parse("original.metric[host:bob,testing,type:dev]")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, "original.metric", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob", "type": "dev"}, d, "Should get dimensions")

	m, mt, d, e = i.Parse("original.metric[host:bob,testing,type:dev].count")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, "original.metric.count", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob", "type": "dev"}, d, "Should get dimensions")

	m, mt, d, e = i.Parse("original.metric[host:bob:bob2,testing,type:dev].count")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, "original.metric.count", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob:bob2", "type": "dev"}, d, "Should get dimensions")

	i, e = commaKeysLoader("coloninkey")
	assert.NoError(t, e)
	m, mt, d, e = i.Parse("original.metric[host:bob:bob2,testing,type:dev].count")
	assert.Equal(t, datapoint.Gauge, mt)
	assert.NoError(t, e)
	assert.Equal(t, "original.metric.count", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host:bob": "bob2", "type": "dev"}, d, "Should get dimensions")
}

func TestCommaKeysLoaderDeconstructorMissingBracket(t *testing.T) {
	i := &commaKeysLoaderDeconstructor{}
	m, mt, d, e := i.Parse("original.metric[host:bob")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, "original.metric[host:bob", m, "Should get metric back")
	assert.Equal(t, map[string]string{}, d, "Should get dimensions")

	m, mt, d, e = i.Parse("original.metrichost:bob]")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, "original.metrichost:bob]", m, "Should get metric back")
	assert.Equal(t, map[string]string{}, d, "Should get dimensions")

	m, mt, d, e = i.Parse("original.metric[")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, "original.metric[", m, "Should get metric back")
	assert.Equal(t, map[string]string{}, d, "Should get dimensions")
}

func TestCommaKeysLoaderFailure(t *testing.T) {
	_, e := commaKeysLoader("unknown:type")
	assert.Error(t, e)
}

func TestCommaKeysLoaderMetricTypeDim(t *testing.T) {
	i, e := commaKeysLoader("mtypedim:sf_type")
	assert.NoError(t, e)
	m, mt, d, e := i.Parse("original.metric[host:bob,sf_type:cumulative_counter]")
	assert.NoError(t, e)
	assert.Equal(t, datapoint.Counter, mt)
	assert.Equal(t, "original.metric", m, "Should get metric back")
	assert.Equal(t, map[string]string{"host": "bob"}, d, "Should get dimensions")
}
