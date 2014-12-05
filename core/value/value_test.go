package value

import (
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntWire(t *testing.T) {
	iv := NewIntWire(3)
	assert.Equal(t, iv.String(), "3", "Expect 3")
	i, err := iv.IntValue()
	assert.Equal(t, int64(3), i, "Expect 3")
	assert.Equal(t, nil, err, "Expect 3")
	f, err := iv.FloatValue()
	assert.Equal(t, 3.0, f, "Expect 3")
}

func TestFloatWire(t *testing.T) {
	iv := NewFloatWire(3)
	assert.Equal(t, iv.String(), "3", "Expect 3")
	_, err := iv.IntValue()
	assert.NotEqual(t, nil, err, "Expect float only")
	f, err := iv.FloatValue()
	assert.Equal(t, 3.0, f, "Expect 3")
}

func TestStrWire(t *testing.T) {
	iv := NewStrWire("val")
	assert.Equal(t, iv.String(), "val", "Expect val")
	_, err := iv.IntValue()
	assert.NotEqual(t, nil, err, "Expect no int")
	_, err = iv.FloatValue()
	assert.NotEqual(t, nil, err, "Expect no float")
}

func TestDatumValue(t *testing.T) {
	iv := NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{DoubleValue: workarounds.GolangDoesnotAllowPointerToFloat64Literal(3.0)})
	assert.Equal(t, iv.String(), "doubleValue:3 ", "Expect 3")
	assert.Equal(t, iv.WireValue(), "3", "Expect 3")
	_, err := iv.IntValue()
	assert.NotEqual(t, nil, err, "Expect float only")
	f, err := iv.FloatValue()
	assert.Equal(t, 3.0, f, "Expect 3")

	iv = NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(3)})
	assert.Equal(t, iv.WireValue(), "3", "Expect 3")
	i, err := iv.IntValue()
	assert.Equal(t, int64(3), i, "Expect 3")
	assert.Equal(t, nil, err, "Expect float only")

	_, err = iv.FloatValue()
	assert.NotEqual(t, nil, err, "Expect float only")

	iv = NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{StrValue: workarounds.GolangDoesnotAllowPointerToStringLiteral("hello")})
	assert.Equal(t, iv.WireValue(), "hello", "Expect hello")
}
