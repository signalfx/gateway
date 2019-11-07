package metricdeconstructor

import (
	"testing"

	"github.com/signalfx/golib/v3/datapoint"
	"github.com/stretchr/testify/assert"
)

func TestIdentityMetricDeconstructor(t *testing.T) {
	i := &IdentityMetricDeconstructor{}
	m, mt, d, e := i.Parse("originalmetric")
	assert.Equal(t, "originalmetric", m, "Should get metric back")
	assert.Equal(t, 0, len(d), "Should get no dimensions")
	assert.Equal(t, datapoint.Gauge, mt)
	assert.Equal(t, nil, e, "Should get no errors")
}
