package signalfx

import (
	"testing"

	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/gohelpers/workarounds"
	. "github.com/smartystreets/goconvey/convey"

	"errors"
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/signalfx/gateway/protocol/signalfx/format"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/pointer"
)

func TestValueToValue(t *testing.T) {
	testVal := func(toSend interface{}, expected string) datapoint.Value {
		dv, err := ValueToValue(toSend)
		So(err, ShouldBeNil)
		So(expected, ShouldEqual, dv.String())
		return dv
	}
	Convey("v2v conversion", t, func() {
		Convey("test basic conversions", func() {
			testVal(int64(1), "1")
			testVal(float64(.2), "0.2")
			testVal(int(3), "3")
			testVal("4", "4")
			_, err := ValueToValue(errors.New("testing"))
			So(err, ShouldNotBeNil)
		})
		Convey("show that maxfloat64 is too large to be a long", func() {
			dv := testVal(math.MaxFloat64, "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
			_, ok := dv.(datapoint.FloatValue)
			So(ok, ShouldBeTrue)

		})
		Convey("show that maxint32 will be a long", func() {
			dv := testVal(math.MaxInt32, "2147483647")
			_, ok := dv.(datapoint.IntValue)
			So(ok, ShouldBeTrue)
		})
		Convey("show that float(maxint64) will be a float due to edgyness of conversions", func() {
			dv := testVal(float64(math.MaxInt64), "9223372036854776000")
			_, ok := dv.(datapoint.FloatValue)
			So(ok, ShouldBeTrue)
		})
	})
}

func TestNewProtobufDataPointWithType(t *testing.T) {
	Convey("A nil datapoint value", t, func() {
		dp := com_signalfx_metrics_protobuf.DataPoint{}
		Convey("should error when converted", func() {
			_, err := NewProtobufDataPointWithType(&dp, com_signalfx_metrics_protobuf.MetricType_COUNTER)
			So(err, ShouldEqual, errDatapointValueNotSet)
		})
		Convey("with a value", func() {
			dp.Value = &com_signalfx_metrics_protobuf.Datum{
				IntValue: pointer.Int64(1),
			}
			Convey("source should set", func() {
				dp.Source = pointer.String("hello")
				dp2, err := NewProtobufDataPointWithType(&dp, com_signalfx_metrics_protobuf.MetricType_COUNTER)
				So(err, ShouldBeNil)
				So(dp2.Dimensions["sf_source"], ShouldEqual, "hello")
			})
		})
	})
}

func TestPropertyAsRawType(t *testing.T) {
	Convey("With value raw test PropertyAsRawType", t, func() {
		type testCase struct {
			v   *com_signalfx_metrics_protobuf.PropertyValue
			exp interface{}
		}
		cases := []testCase{
			{
				v:   nil,
				exp: nil,
			},
			{
				v: &com_signalfx_metrics_protobuf.PropertyValue{
					BoolValue: proto.Bool(false),
				},
				exp: false,
			},
			{
				v: &com_signalfx_metrics_protobuf.PropertyValue{
					IntValue: proto.Int64(123),
				},
				exp: 123,
			},
			{
				v: &com_signalfx_metrics_protobuf.PropertyValue{
					DoubleValue: proto.Float64(2.0),
				},
				exp: 2.0,
			},
			{
				v: &com_signalfx_metrics_protobuf.PropertyValue{
					StrValue: proto.String("hello"),
				},
				exp: "hello",
			},
			{
				v:   &com_signalfx_metrics_protobuf.PropertyValue{},
				exp: nil,
			},
		}
		for _, c := range cases {
			So(PropertyAsRawType(c.v), ShouldEqual, c.exp)
		}
	})
}

func TestBodySendFormatV2(t *testing.T) {
	Convey("BodySendFormatV2 should String()-ify", t, func() {
		x := signalfxformat.BodySendFormatV2{
			Metric: "hi",
		}
		So(x.String(), ShouldContainSubstring, "hi")
	})
}

func TestNewProtobufEvent(t *testing.T) {
	Convey("given a protobuf event with a nil property value", t, func() {
		protoEvent := &com_signalfx_metrics_protobuf.Event{
			EventType:  workarounds.GolangDoesnotAllowPointerToStringLiteral("mwp.test2"),
			Dimensions: []*com_signalfx_metrics_protobuf.Dimension{},
			Properties: []*com_signalfx_metrics_protobuf.Property{
				{
					Key:   workarounds.GolangDoesnotAllowPointerToStringLiteral("version"),
					Value: &com_signalfx_metrics_protobuf.PropertyValue{},
				},
			},
		}
		Convey("should error when converted", func() {
			_, err := NewProtobufEvent(protoEvent)
			So(err, ShouldEqual, errPropertyValueNotSet)
		})

	})
}

func TestFromMT(t *testing.T) {
	Convey("invalid fromMT types should panic", t, func() {
		So(func() {
			fromMT(com_signalfx_metrics_protobuf.MetricType(1001))
		}, ShouldPanic)
	})
}

func TestNewDatumValue(t *testing.T) {
	Convey("datum values should convert", t, func() {
		Convey("string should convert", func() {
			s1 := "abc"
			So(s1, ShouldEqual, NewDatumValue(&com_signalfx_metrics_protobuf.Datum{StrValue: &s1}).(datapoint.StringValue).String())
		})
		Convey("floats should convert", func() {
			f1 := 1.2
			So(f1, ShouldEqual, NewDatumValue(&com_signalfx_metrics_protobuf.Datum{DoubleValue: &f1}).(datapoint.FloatValue).Float())
		})
		Convey("int should convert", func() {
			i1 := int64(3)
			So(i1, ShouldEqual, NewDatumValue(&com_signalfx_metrics_protobuf.Datum{IntValue: &i1}).(datapoint.IntValue).Int())
		})
	})
}
