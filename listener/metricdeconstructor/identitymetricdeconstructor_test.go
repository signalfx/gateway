package metricdeconstructor

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIdentityMetricDeconstructor(t *testing.T) {
	i := &identityMetricDeconstructor{}
	m, d, e := i.Parse("originalmetric")
	assert.Equal(t, "originalmetric", m, "Should get metric back")
	assert.Equal(t, 0, len(d), "Should get no dimensions")
	assert.Equal(t, nil, e, "Should get no errors")
}
