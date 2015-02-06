package carbon

import (
	"errors"
	"testing"

	"github.com/signalfuse/signalfxproxy/datapoint"
	"github.com/signalfuse/signalfxproxy/protocol/carbon/metricdeconstructor"
	"github.com/stretchr/testify/assert"
)

type errorDeconstructor struct{}

func (parser *errorDeconstructor) Parse(originalMetric string) (string, map[string]string, error) {
	return "", nil, errors.New("error parsing")
}

func TestNewCarbonDatapoint(t *testing.T) {
	identityParser, _ := metricdeconstructor.Load("", "")
	dp, err := NewCarbonDatapoint("hello 3 3", identityParser)
	assert.Equal(t, nil, err, "Should be a valid carbon line")
	assert.Equal(t, "hello", dp.Metric(), "Should get metric back")

	_, err = NewCarbonDatapoint("INVALIDLINE", identityParser)
	assert.NotEqual(t, nil, err, "Line should be invalid")

	_, err = NewCarbonDatapoint("hello 3 bob", identityParser)
	assert.NotEqual(t, nil, err, "Line should be invalid")

	_, err = NewCarbonDatapoint("hello bob 3", identityParser)
	assert.NotEqual(t, nil, err, "Line should be invalid")

	dp, _ = NewCarbonDatapoint("hello 3.3 3", identityParser)
	f := dp.Value().(datapoint.FloatValue).Float()
	assert.Equal(t, 3.3, f, "Should get value back")

	carbonDp, _ := dp.(Native)
	assert.Equal(t, "hello 3.3 3", carbonDp.ToCarbonLine(), "Should get the carbon line back")

	dp, err = NewCarbonDatapoint("hello 3 3", &errorDeconstructor{})
	assert.NotEqual(t, nil, err, "Should NOT be a valid carbon line")
}
