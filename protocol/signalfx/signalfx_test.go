package signalfx

import (
	"testing"

	"github.com/cep21/gohelpers/workarounds"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	. "github.com/smartystreets/goconvey/convey"

	"errors"
	"github.com/golang/protobuf/proto"
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
			Convey("properties should set", func() {
				dp.Properties = []*com_signalfx_metrics_protobuf.Property{
					{
						Key: proto.String("name"),
						Value: &com_signalfx_metrics_protobuf.PropertyValue{
							StrValue: proto.String("jack"),
						},
					},
					{
						Key:   proto.String("missing"),
						Value: &com_signalfx_metrics_protobuf.PropertyValue{},
					},
					{
						Key: proto.String(""),
						Value: &com_signalfx_metrics_protobuf.PropertyValue{
							StrValue: proto.String("notset"),
						},
					},
				}
				dp2, err := NewProtobufDataPointWithType(&dp, com_signalfx_metrics_protobuf.MetricType_COUNTER)
				So(err, ShouldBeNil)
				So(dp2.GetProperties()["name"], ShouldEqual, "jack")
				So(len(dp2.GetProperties()), ShouldEqual, 1)
			})
		})
	})
}

func TestValueToRaw(t *testing.T) {
	Convey("With value raw test valueToRaw", t, func() {
		type testCase struct {
			v   ValueToSend
			exp interface{}
		}
		cases := []testCase{
			{
				v:   ValueToSend(1.0),
				exp: 1.0,
			},
			{
				v:   ValueToSend(int64(1)),
				exp: int64(1),
			},
			{
				v:   ValueToSend(1),
				exp: 1,
			},
			{
				v:   ValueToSend("hi"),
				exp: "hi",
			},
			{
				v:   ValueToSend(true),
				exp: true,
			},
			{
				v:   ValueToSend(func() {}),
				exp: nil,
			},
		}
		for _, c := range cases {
			So(valueToRaw(c.v), ShouldEqual, c.exp)
		}
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
