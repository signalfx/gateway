package protocoltypes

import (
	"errors"
	"github.com/cep21/gohelpers/a"
	"github.com/signalfuse/signalfxproxy/listener/metricdeconstructor"
	"testing"
)

type errorDeconstructor struct{}

func (parser *errorDeconstructor) Parse(originalMetric string) (string, map[string]string, error) {
	return "", nil, errors.New("error parsing")
}

func TestNewCarbonDatapoint(t *testing.T) {
	identityParser, _ := metricdeconstructor.Load("", "")
	dp, err := NewCarbonDatapoint("hello 3 3", identityParser)
	a.ExpectEquals(t, nil, err, "Should be a valid carbon line")
	a.ExpectEquals(t, "hello", dp.Metric(), "Should get metric back")

	_, err = NewCarbonDatapoint("INVALIDLINE", identityParser)
	a.ExpectNotEquals(t, nil, err, "Line should be invalid")

	_, err = NewCarbonDatapoint("hello 3 bob", identityParser)
	a.ExpectNotEquals(t, nil, err, "Line should be invalid")

	_, err = NewCarbonDatapoint("hello bob 3", identityParser)
	a.ExpectNotEquals(t, nil, err, "Line should be invalid")

	dp, _ = NewCarbonDatapoint("hello 3.3 3", identityParser)
	f, _ := dp.Value().FloatValue()
	a.ExpectEquals(t, 3.3, f, "Should get value back")

	carbonDp, _ := dp.(CarbonReady)
	a.ExpectEquals(t, "hello 3.3 3", carbonDp.ToCarbonLine(), "Should get the carbon line back")

	dp, err = NewCarbonDatapoint("hello 3 3", &errorDeconstructor{})
	a.ExpectNotEquals(t, nil, err, "Should NOT be a valid carbon line")
}
