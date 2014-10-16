package metricdeconstructor

import (
	"github.com/cep21/gohelpers/a"
	"testing"
)

func TestIdentityMetricDeconstructor(t *testing.T) {
	i := &identityMetricDeconstructor{}
	m, d, e := i.Parse("originalmetric")
	a.ExpectEquals(t, "originalmetric", m, "Should get metric back")
	a.ExpectEquals(t, 0, len(d), "Should get no dimensions")
	a.ExpectEquals(t, nil, e, "Should get no errors")
}
