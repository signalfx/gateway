package filtering

import (
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

type TestFilteredForwarder struct {
	FilteredForwarder
}

func Test(t *testing.T) {
	Convey("bad regexes throw errors", t, func() {
		filters := FilterObj{
			Deny: []string{"["},
		}
		forwarder := TestFilteredForwarder{}
		err := forwarder.Setup(&filters)
		So(err, ShouldNotBeNil)
		filters = FilterObj{
			Allow: []string{"["},
		}
		err = forwarder.Setup(&filters)
		So(err, ShouldNotBeNil)
	})
	Convey("good regexes don't throw errors and work together", t, func() {
		filters := FilterObj{
			Deny:  []string{"^cpu.*"},
			Allow: []string{"^cpu.idle"},
		}
		forwarder := TestFilteredForwarder{}
		err := forwarder.Setup(&filters)
		So(err, ShouldBeNil)
		dp := dptest.DP()
		Convey("cpu.idle is allowed even though cpu.* is denied", func() {
			dp.Metric = "cpu.idle"
			datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
			So(len(datapoints), ShouldEqual, 1)
			So(forwarder.FilteredDatapoints, ShouldEqual, 0)
			Convey("metrics that match deny but not allow are denied", func() {
				dp.Metric = "cpu.user"
				So(forwarder.FilteredDatapoints, ShouldEqual, 0)
				datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
				So(len(datapoints), ShouldEqual, 0)
				So(forwarder.FilteredDatapoints, ShouldEqual, 1)
				Convey("other metrics that do not explicitly pass allow are denied", func() {
					dp.Metric = "i.should.be.denied"
					datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
					So(len(datapoints), ShouldEqual, 0)
					So(forwarder.FilteredDatapoints, ShouldEqual, 2)
				})
			})
		})
	})
	Convey("allow by itself denies what it doesn't match", t, func() {
		filters := FilterObj{
			Allow: []string{"^cpu.*"},
		}
		forwarder := TestFilteredForwarder{}
		err := forwarder.Setup(&filters)
		So(err, ShouldBeNil)
		dp := dptest.DP()
		Convey("metrics starting with cpu get accepted", func() {
			dp.Metric = "cpu.idle"
			datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
			So(len(datapoints), ShouldEqual, 1)
			So(forwarder.FilteredDatapoints, ShouldEqual, 0)
			Convey("metrics that don't match allow are denied", func() {
				dp.Metric = "im.not.a.matcher"
				datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
				So(len(datapoints), ShouldEqual, 0)
				So(forwarder.FilteredDatapoints, ShouldEqual, 1)
			})
		})
	})
	Convey("deny by itself accepts what it doesn't match", t, func() {
		filters := FilterObj{
			Deny: []string{"^cpu.*"},
		}
		forwarder := TestFilteredForwarder{}
		err := forwarder.Setup(&filters)
		So(err, ShouldBeNil)
		dp := dptest.DP()
		Convey("metrics starting with cpu get denied", func() {
			dp.Metric = "cpu.idle"
			datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
			So(len(datapoints), ShouldEqual, 0)
			So(forwarder.FilteredDatapoints, ShouldEqual, 1)
			Convey("metrics that don't match cpu are acccepted", func() {
				dp.Metric = "im.not.a.matcher"
				datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
				So(len(datapoints), ShouldEqual, 1)
				So(forwarder.FilteredDatapoints, ShouldEqual, 1)
				So(len(forwarder.GetFilteredDatapoints()), ShouldEqual, 1)
			})
		})
	})
	Convey("no rules let everything through", t, func() {
		filters := FilterObj{}
		forwarder := TestFilteredForwarder{}
		err := forwarder.Setup(&filters)
		So(err, ShouldBeNil)
		dp := dptest.DP()
		Convey("metrics starting with cpu get denied", func() {
			dp.Metric = "cpu.idle"
			datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
			So(len(datapoints), ShouldEqual, 1)
			So(forwarder.FilteredDatapoints, ShouldEqual, 0)
			Convey("metrics that don't match cpu are acccepted", func() {
				dp.Metric = "im.not.a.matcher"
				datapoints := forwarder.FilterDatapoints([]*datapoint.Datapoint{dp})
				So(len(datapoints), ShouldEqual, 1)
				So(forwarder.FilteredDatapoints, ShouldEqual, 0)
				So(len(forwarder.GetFilteredDatapoints()), ShouldEqual, 1)
			})
		})
	})
}
