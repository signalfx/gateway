package protocoltypes

import (
	"github.com/cep21/gohelpers/a"
	"testing"
)

func TestNewCarbonDatapoint(t *testing.T) {
	dp, err := NewCarbonDatapoint("hello 3 3")
	a.ExpectEquals(t, nil, err, "Should be a valid carbon line")
	a.ExpectEquals(t, "hello", dp.Metric(), "Should get metric back")

	_, err = NewCarbonDatapoint("INVALIDLINE")
	a.ExpectNotEquals(t, nil, err, "Line should be invalid")

	_, err = NewCarbonDatapoint("hello 3 bob")
	a.ExpectNotEquals(t, nil, err, "Line should be invalid")

	_, err = NewCarbonDatapoint("hello bob 3")
	a.ExpectNotEquals(t, nil, err, "Line should be invalid")

	dp, _ = NewCarbonDatapoint("hello 3.3 3")
	f, _ := dp.Value().FloatValue()
	a.ExpectEquals(t, 3.3, f, "Should get value back")

	carbonDp, _ := dp.(CarbonReady)
	a.ExpectEquals(t, "hello 3.3 3", carbonDp.ToCarbonLine(), "Should get the carbon line back")
}
