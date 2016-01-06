package signalfx

import (
	"testing"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	. "github.com/smartystreets/goconvey/convey"

	"errors"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/pointer"
)

func TestValueToValue(t *testing.T) {
	Convey("v2v conversion", t, func() {
		testVal := func(toSend interface{}, expected string) {
			dv, err := ValueToValue(toSend)
			So(err, ShouldBeNil)
			So(expected, ShouldEqual, dv.String())
		}
		testVal(int64(1), "1")
		testVal(float64(.2), "0.2")
		testVal(int(3), "3")
		testVal("4", "4")
		_, err := ValueToValue(errors.New("testing"))
		So(err, ShouldNotBeNil)
	})
}

func TestNewProtobufDataPointWithType(t *testing.T) {
	Convey("A nil datapoint value", t, func() {
		dp := com_signalfx_metrics_protobuf.DataPoint{}
		Convey("should error when converted", func() {
			_, err := NewProtobufDataPointWithType(&dp, com_signalfx_metrics_protobuf.MetricType_COUNTER)
			So(err, ShouldEqual, errDatapointValueNotSet)
		})
		Convey("source should set", func() {
			dp.Source = pointer.String("hello")
			dp.Value = &com_signalfx_metrics_protobuf.Datum{
				IntValue: pointer.Int64(1),
			}
			dp2, err := NewProtobufDataPointWithType(&dp, com_signalfx_metrics_protobuf.MetricType_COUNTER)
			So(err, ShouldBeNil)
			So(dp2.Dimensions["sf_source"], ShouldEqual, "hello")
		})
	})
}

func TestBodySendFormatV2(t *testing.T) {
	Convey("BodySendFormatV2 should String()-ify", t, func() {
		x := BodySendFormatV2{
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
