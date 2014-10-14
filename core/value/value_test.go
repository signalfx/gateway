package value

import (
	"github.com/cep21/gohelpers/a"
	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
	"testing"
)

func TestIntWire(t *testing.T) {
	iv := NewIntWire(3)
	a.ExpectEquals(t, iv.String(), "3", "Expect 3")
	i, err := iv.IntValue()
	a.ExpectEquals(t, int64(3), i, "Expect 3")
	a.ExpectEquals(t, nil, err, "Expect 3")
	f, err := iv.FloatValue()
	a.ExpectEquals(t, 3.0, f, "Expect 3")
}

func TestFloatWire(t *testing.T) {
	iv := NewFloatWire(3)
	a.ExpectEquals(t, iv.String(), "3", "Expect 3")
	_, err := iv.IntValue()
	a.ExpectNotEquals(t, nil, err, "Expect float only")
	f, err := iv.FloatValue()
	a.ExpectEquals(t, 3.0, f, "Expect 3")
}

func TestStrWire(t *testing.T) {
	iv := NewStrWire("val")
	a.ExpectEquals(t, iv.String(), "val", "Expect val")
	_, err := iv.IntValue()
	a.ExpectNotEquals(t, nil, err, "Expect no int")
	_, err = iv.FloatValue()
	a.ExpectNotEquals(t, nil, err, "Expect no float")
}

func TestDatumValue(t *testing.T) {
	iv := NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{DoubleValue: workarounds.GolangDoesnotAllowPointerToFloat64Literal(3.0)})
	a.ExpectEquals(t, iv.String(), "doubleValue:3 ", "Expect 3")
	a.ExpectEquals(t, iv.WireValue(), "3", "Expect 3")
	_, err := iv.IntValue()
	a.ExpectNotEquals(t, nil, err, "Expect float only")
	f, err := iv.FloatValue()
	a.ExpectEquals(t, 3.0, f, "Expect 3")

	iv = NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{IntValue: workarounds.GolangDoesnotAllowPointerToIntLiteral(3)})
	a.ExpectEquals(t, iv.WireValue(), "3", "Expect 3")
	i, err := iv.IntValue()
	a.ExpectEquals(t, int64(3), i, "Expect 3")
	a.ExpectEquals(t, nil, err, "Expect float only")

	_, err = iv.FloatValue()
	a.ExpectNotEquals(t, nil, err, "Expect float only")

	iv = NewDatumWire(&com_signalfuse_metrics_protobuf.Datum{StrValue: workarounds.GolangDoesnotAllowPointerToStringLiteral("hello")})
	a.ExpectEquals(t, iv.WireValue(), "hello", "Expect hello")
}
