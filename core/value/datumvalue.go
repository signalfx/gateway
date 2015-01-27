package value

import (
	"errors"
	"strconv"

	"github.com/signalfuse/com_signalfuse_metrics_protobuf"
)

type datumWire struct {
	val *com_signalfuse_metrics_protobuf.Datum
}

func (wireVal *datumWire) WireValue() string {
	if wireVal.val.DoubleValue != nil {
		return strconv.FormatFloat(wireVal.val.GetDoubleValue(), 'f', -1, 64)
	} else if wireVal.val.IntValue != nil {
		return strconv.FormatInt(wireVal.val.GetIntValue(), 10)
	} else {
		return wireVal.val.GetStrValue()
	}
}

func (wireVal *datumWire) FloatValue() (float64, error) {
	if wireVal.val.DoubleValue != nil {
		return wireVal.val.GetDoubleValue(), nil
	}
	return 0, errors.New("unset float value")
}

func (wireVal *datumWire) IntValue() (int64, error) {
	if wireVal.val.IntValue != nil {
		return wireVal.val.GetIntValue(), nil
	}
	return 0, errors.New("unset int value")
}

func (wireVal *datumWire) String() string {
	return wireVal.val.String()
}

// NewDatumWire creates new datapoint value referenced from a value of the datum
func NewDatumWire(val *com_signalfuse_metrics_protobuf.Datum) DatapointValue {
	return &datumWire{val: val}
}
