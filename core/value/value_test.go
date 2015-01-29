package value

import (
	"testing"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"github.com/stretchr/testify/assert"
)

func TestIntWire(t *testing.T) {
	iv := NewIntWire(3)
	assert.Equal(t, iv.String(), "3")
	i := iv.IntValue()
	assert.Equal(t, int64(3), i)
}

func TestFloatWire(t *testing.T) {
	iv := NewFloatWire(3)
	assert.Equal(t, iv.String(), "3")
	f := iv.FloatValue()
	assert.Equal(t, 3.0, f)
}

func TestStrWire(t *testing.T) {
	iv := NewStrWire("val")
	assert.Equal(t, iv.String(), "val")
}

func TestDatumValue(t *testing.T) {
	iv := NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{DoubleValue: workarounds.GolangDoesnotAllowPointerToFloat64Literal(3.0)})
	assert.Equal(t, iv.String(), "3")
	assert.Equal(t, iv.(FloatValue).FloatValue(), 3.0)

	iv = NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(3)})
	assert.Equal(t, iv.(IntDatapoint).IntValue(), 3)

	iv = NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{StrValue: workarounds.GolangDoesnotAllowPointerToStringLiteral("hello")})
	assert.Equal(t, iv.String(), "hello")
}
