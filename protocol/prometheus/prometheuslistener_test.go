package prometheus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/pointer"
	"github.com/signalfx/golib/web"
	. "github.com/smartystreets/goconvey/convey"
)

func getPayload(incoming *prompb.WriteRequest) []byte {
	if incoming == nil {
		incoming = getWriteRequest()
	}
	bytes, err := proto.Marshal(incoming)
	So(err, ShouldBeNil)
	// assert.NilError(t, err)
	return snappy.Encode(nil, bytes)
}

func getWriteRequest() *prompb.WriteRequest {
	return &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []*prompb.Label{
					{Name: "key", Value: "value"},
					{Name: model.MetricNameLabel, Value: "process_cpu_seconds_total"}, // counter
				},
				Samples: []*prompb.Sample{
					{Value: 1, Timestamp: time.Now().Unix() * 1000},
				},
			},
		},
	}
}

func TestGetStuff(t *testing.T) {
	Convey("test metric types", t, func() {
		So(getMetricType("im_a_counter_bucket"), ShouldEqual, datapoint.Counter)
		So(getMetricType("im_a_counter_count"), ShouldEqual, datapoint.Counter)
		So(getMetricType("everything_else"), ShouldEqual, datapoint.Gauge)
	})
}

func TestErrorCases(t *testing.T) {
	Convey("test listener", t, func() {
		callCount := int64(0)
		conf := &Config{
			ListenAddr: pointer.String("127.0.0.1:0"),
			HTTPChain: func(ctx context.Context, rw http.ResponseWriter, r *http.Request, next web.ContextHandler) {
				atomic.AddInt64(&callCount, 1)
				next.ServeHTTPC(ctx, rw, r)
			},
		}
		sendTo := dptest.NewBasicSink()
		Convey("test same port", func() {
			conf.ListenAddr = pointer.String("127.0.0.1:99999999r")
			_, err := NewListener(sendTo, conf)
			So(err, ShouldNotBeNil)
		})
	})
}

type errSink struct{}

func (e *errSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) error {
	return errors.New("nope")
}

func (e *errSink) AddEvents(ctx context.Context, points []*event.Event) error {
	return errors.New("nope")
}

func TestListener(t *testing.T) {
	Convey("test listener", t, func() {
		callCount := int64(0)
		conf := &Config{
			ListenAddr: pointer.String("127.0.0.1:0"),
			HTTPChain: func(ctx context.Context, rw http.ResponseWriter, r *http.Request, next web.ContextHandler) {
				atomic.AddInt64(&callCount, 1)
				next.ServeHTTPC(ctx, rw, r)
			},
		}
		sendTo := dptest.NewBasicSink()
		listener, err := NewListener(sendTo, conf)
		So(err, ShouldBeNil)
		client := &http.Client{}
		baseURL := fmt.Sprintf("http://%s/write", listener.server.Addr)
		Convey("Should expose health check", func() {
			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/healthz", listener.server.Addr), nil)
			So(err, ShouldBeNil)
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			So(atomic.LoadInt64(&callCount), ShouldEqual, 1)
		})
		Convey("Should be able to receive datapoints", func() {
			sendTo.Resize(10)
			req, err := http.NewRequest("POST", baseURL, bytes.NewReader(getPayload(nil)))
			So(err, ShouldBeNil)
			req.Header.Set("Content-Type", "application/x-protobuf")
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			datapoints := <-sendTo.PointsChan
			So(len(datapoints), ShouldEqual, 1)
		})
		Convey("Is a Collector", func() {
			dps := listener.Datapoints()
			So(len(dps), ShouldEqual, 13)
		})
		Convey("test getDatapoints edge cases", func() {
			ts := getWriteRequest().Timeseries[0]
			Convey("check nan", func() {
				ts.Samples[0].Value = math.NaN()
				dps := listener.decoder.getDatapoints(ts)
				So(len(dps), ShouldEqual, 0)
				for atomic.LoadInt64(&listener.decoder.TotalNaNs) != 1 {
					runtime.Gosched()
				}
			})
			Convey("check float conversion", func() {
				ts.Samples[0].Value = 1.71245
				dps := listener.decoder.getDatapoints(ts)
				So(len(dps), ShouldEqual, 1)
				So(dps[0].Value, ShouldResemble, datapoint.NewFloatValue(1.71245))
			})
		})
		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
}

type errRead struct{}

func (e *errRead) Read(p []byte) (int, error) {
	panic("blarg")
}

func TestBad(t *testing.T) {
	Convey("test listener", t, func() {
		callCount := int64(0)
		conf := &Config{
			ListenAddr: pointer.String("127.0.0.1:0"),
			HTTPChain: func(ctx context.Context, rw http.ResponseWriter, r *http.Request, next web.ContextHandler) {
				atomic.AddInt64(&callCount, 1)
				next.ServeHTTPC(ctx, rw, r)
			},
		}
		sendTo := dptest.NewBasicSink()
		listener, err := NewListener(sendTo, conf)
		So(err, ShouldBeNil)
		client := &http.Client{}
		baseURL := fmt.Sprintf("http://%s/write", listener.server.Addr)
		sendTo.Resize(10)
		Convey("Should bork on nil data", func() {
			req, err := http.NewRequest("POST", baseURL, nil)
			So(err, ShouldBeNil)
			req.Header.Set("Content-Type", "application/x-protobuf")
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusBadRequest)
		})
		Convey("Should bork bad readAll", func() {
			listener.decoder.readAll = func(r io.Reader) ([]byte, error) {
				return nil, errors.New("nope")
			}
			req, err := http.NewRequest("POST", baseURL, bytes.NewReader(getPayload(nil)))
			So(err, ShouldBeNil)
			req.Header.Set("Content-Type", "application/x-protobuf")
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusInternalServerError)
		})
		Convey("Should bork on bad data", func() {
			jeff := []byte("blarg")
			osnap := snappy.Encode(nil, jeff)
			req, err := http.NewRequest("POST", baseURL, bytes.NewReader(osnap))
			So(err, ShouldBeNil)
			req.Header.Set("Content-Type", "application/x-protobuf")
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusBadRequest)
		})
		Convey("count bad datapoint", func() {
			incoming := getWriteRequest()
			incoming.Timeseries[0].Labels = []*prompb.Label{}
			req, err := http.NewRequest("POST", baseURL, bytes.NewReader(getPayload(incoming)))
			So(err, ShouldBeNil)
			req.Header.Set("Content-Type", "application/x-protobuf")
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			for atomic.LoadInt64(&listener.decoder.TotalBadDatapoints) != 1 {
				runtime.Gosched()
			}
		})
		Convey("sink throws an error", func() {
			listener.decoder.SendTo = new(errSink)
			req, err := http.NewRequest("POST", baseURL, bytes.NewReader(getPayload(nil)))
			So(err, ShouldBeNil)
			req.Header.Set("Content-Type", "application/x-protobuf")
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusInternalServerError)
		})
		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
}
