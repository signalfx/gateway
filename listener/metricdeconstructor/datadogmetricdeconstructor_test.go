package metricdeconstructor

import (
	"github.com/cep21/gohelpers/a"
	"testing"
)

func TestDatadogMetricDeconstructor(t *testing.T) {
	i := &datadogMetricDeconstructor{}
	m, d, e := i.Parse("original.metric[host:bob]")
	a.ExpectEquals(t, nil, e, "Should get no errors")
	a.ExpectEquals(t, "original.metric", m, "Should get metric back")
	a.ExpectEquals(t, map[string]string{"host": "bob"}, d, "Should get dimensions")

	i = &datadogMetricDeconstructor{}
	m, d, e = i.Parse("original.metric[host:bob,testing,type:dev]")
	a.ExpectEquals(t, nil, e, "Should get no errors")
	a.ExpectEquals(t, "original.metric", m, "Should get metric back")
	a.ExpectEquals(t, map[string]string{"host": "bob", "type": "dev"}, d, "Should get dimensions")
}

func TestDatadogMetricInvalidDeconstructorMissingBracket(t *testing.T) {
	i := &datadogMetricDeconstructor{}
	m, d, e := i.Parse("original.metric[host:bob")
	a.ExpectEquals(t, nil, e, "Should get no errors")
	a.ExpectEquals(t, "original.metric[host:bob", m, "Should get metric back")
	a.ExpectEquals(t, map[string]string{}, d, "Should get dimensions")

	i = &datadogMetricDeconstructor{}
	m, d, e = i.Parse("original.metrichost:bob]")
	a.ExpectEquals(t, nil, e, "Should get no errors")
	a.ExpectEquals(t, "original.metrichost:bob]", m, "Should get metric back")
	a.ExpectEquals(t, map[string]string{}, d, "Should get dimensions")
}
