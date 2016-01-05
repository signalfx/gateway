package signalfx

import (
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/nettest"
	"github.com/signalfx/golib/pointer"
	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

var errReadErr = errors.New("could not read")

type errorReader struct{}

func (errorReader *errorReader) Read([]byte) (int, error) {
	return 0, errReadErr
}

func TestSignalfxProtoDecoders(t *testing.T) {
	readerCheck := func(decoder ErrorReader) {
		Convey("Should expect invalid content length", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(nil)),
			}
			req.ContentLength = -1
			ctx := context.Background()
			So(decoder.Read(ctx, req), ShouldEqual, errInvalidContentLength)
		})
		Convey("should check read errors", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(&errorReader{}),
			}
			req.ContentLength = 1
			ctx := context.Background()
			So(decoder.Read(ctx, req), ShouldEqual, errReadErr)
		})
	}
	Convey("a setup ProtobufDecoderV2", t, func() {
		decoder := ProtobufDecoderV2{Logger: log.Discard}
		readerCheck(&decoder)
	})
	Convey("a setup ProtobufEventDecoderV2", t, func() {
		decoder := ProtobufEventDecoderV2{Logger: log.Discard}
		readerCheck(&decoder)
	})
}

func TestSignalfxProtobufV1Decoder(t *testing.T) {
	Convey("a setup metric decoder", t, func() {
		typeGetter := metricHandler{
			metricCreationsMap: make(map[string]com_signalfx_metrics_protobuf.MetricType),
		}
		sendTo := dptest.NewBasicSink()
		ctx := context.Background()

		decoder := ProtobufDecoderV1{
			TypeGetter: &typeGetter,
			Sink:       sendTo,
			Logger:     log.Discard,
		}
		Convey("Should expect invalid protobufs", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(strings.NewReader("INVALID_PROTOBUF")),
			}
			So(decoder.Read(ctx, req), ShouldNotBeNil)
		})
		Convey("Should get error without anything left to read", func() {
			varintBytes := proto.EncodeVarint(uint64(100))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(varintBytes)),
			}
			So(decoder.Read(ctx, req), ShouldNotBeNil)
		})
		Convey("Should get error reading len zero", func() {
			varintBytes := proto.EncodeVarint(uint64(0))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(append(varintBytes, []byte("asfasdfsda")...))),
			}
			So(decoder.Read(ctx, req), ShouldNotBeNil)
		})

		Convey("Should get error reading big lengths", func() {
			varintBytes := proto.EncodeVarint(uint64(200000))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(append(varintBytes, []byte("abasdfsadfafdsc")...))),
			}
			So(decoder.Read(ctx, req), ShouldEqual, errProtobufTooLarge)
		})

		Convey("Should get error reading invalid varints", func() {
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader([]byte{byte(255), byte(147), byte(235), byte(235), byte(235)})),
			}
			So(decoder.Read(ctx, req), ShouldEqual, errInvalidProtobufVarint)
		})
		Convey("Should get error reading invalid protobuf after varint", func() {
			varintBytes := proto.EncodeVarint(uint64(20))
			req := &http.Request{
				Body: ioutil.NopCloser(bytes.NewReader(append(varintBytes, []byte("01234567890123456789")...))),
			}
			So(decoder.Read(ctx, req).Error(), ShouldContainSubstring, "proto")
		})
	})
}

func verifyEventRequest(baseURI string, contentType string, path string, body io.Reader, channel *dptest.BasicSink, eventType string, category string, dimensions map[string]string, properties map[string]interface{}, reqErr error) {
	Convey("given a new request with path "+path, func() {
		req, err := http.NewRequest("POST", baseURI+path, body)
		if reqErr != nil {
			Convey("we should get the error provided"+reqErr.Error(), func() {
				So(err, ShouldNotBeNil)
				So(reqErr, ShouldEqual, err)
			})
		} else {
			Convey("we should be able to successfully parse the payload", func() {
				So(err, ShouldBeNil)
				doneSignal := make(chan struct{})
				var eOut *event.Event
				go func() {
					eOut = channel.NextEvent()
					doneSignal <- struct{}{}
				}()
				client := &http.Client{}
				if contentType != "" {
					req.Header.Add("Content-Type", contentType)
				}
				resp, err := client.Do(req)
				So(err, ShouldBeNil)
				So(resp.StatusCode, ShouldEqual, http.StatusOK)
				Convey("and the generated event should be what we expect", func() {
					runtime.Gosched()
					_ = <-doneSignal
					So(eventType, ShouldEqual, eOut.EventType)
					So(category, ShouldEqual, eOut.Category)
					if dimensions != nil {
						So(dimensions, ShouldResemble, eOut.Dimensions)
					}
					if properties != nil {
						So(properties, ShouldResemble, eOut.Meta)
					}
					So(eOut.Timestamp.Nanosecond(), ShouldBeGreaterThan, 0)
				})
			})
		}
	})
}

func TestSignalfxListenerFailure(t *testing.T) {
	Convey("invalid addr should not listen", t, func() {
		listenConf := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:9999999999"),
		}
		_, err := NewListener(nil, listenConf)
		So(err, ShouldNotBeNil)
	})
}

func TestMapToProperties(t *testing.T) {
	if len(mapToProperties(map[string]interface{}{"test": errors.New("unknown")})) > 0 {
		t.Error("Unexpected map length")
	}
}

func TestCheckResp(t *testing.T) {
	Convey("With a resp", t, func() {
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(strings.NewReader(`"OK"`)),
		}
		Convey("I should check read errors", func() {
			resp.Body = ioutil.NopCloser(&errorReader{})
			So(errors.Details(checkResp(resp)), ShouldContainSubstring, "unable to verify response body")
		})
		Convey("I should check status code", func() {
			resp.StatusCode = http.StatusNotFound
			So(checkResp(resp).Error(), ShouldContainSubstring, "invalid status code")
		})
		Convey("I should check for invalid JSON", func() {
			resp.Body = ioutil.NopCloser(strings.NewReader(`INVALID_JSON`))
			So(checkResp(resp), ShouldNotBeNil)
		})
		Convey("I should check for valid JSON that is not OK", func() {
			resp.Body = ioutil.NopCloser(strings.NewReader(`"NOT_OK"`))
			So(errors.Details(checkResp(resp)), ShouldContainSubstring, "Resp body not ok")
		})
	})
}

func TestSignalfxListener(t *testing.T) {
	Convey("given a signalfx listener", t, func() {
		sendTo := dptest.NewBasicSink()
		sendTo.Resize(1)
		ctx := context.Background()
		logBuf := &bytes.Buffer{}
		logger := log.NewLogfmtLogger(logBuf, log.Panic)
		listenConf := &ListenerConfig{
			ListenAddr: pointer.String("127.0.0.1:0"),
			Logger:     logger,
		}
		listener, err := NewListener(sendTo, listenConf)
		So(err, ShouldBeNil)
		baseURI := fmt.Sprintf("http://127.0.0.1:%d", nettest.TCPPort(listener.listener))
		Convey("And a signalfx forwarder", func() {
			forwardConfig := &ForwarderConfig{
				DatapointURL: pointer.String(fmt.Sprintf("%s/v2/datapoint", baseURI)),
				EventURL:     pointer.String(fmt.Sprintf("%s/v2/event", baseURI)),
				Logger:       logger,
			}
			forwarder := NewForwarder(forwardConfig)
			So(forwarder.Datapoints(), ShouldEqual, nil)
			Convey("event post to nowhere should fail", func() {
				forwarder.eventURL = "http://localhost:1"
				So(forwarder.AddEvents(ctx, []*event.Event{dptest.E()}), ShouldNotBeNil)
			})
			Convey("Should be able to send a point", func() {
				dpSent := dptest.DP()
				dpSent.Timestamp = dpSent.Timestamp.Round(time.Millisecond)
				So(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{dpSent}), ShouldBeNil)
				dpSeen := sendTo.Next()
				So(dpSent.String(), ShouldEqual, dpSeen.String())
			})
			Convey("Should be able to send zero len events", func() {
				So(forwarder.AddEvents(ctx, []*event.Event{}), ShouldBeNil)
			})
			Convey("Should be able to send zero len datapoints", func() {
				So(forwarder.AddDatapoints(ctx, []*datapoint.Datapoint{}), ShouldBeNil)
			})
			Convey("Should check event marshal failures", func() {
				forwarder.protoMarshal = func(pb proto.Message) ([]byte, error) {
					return nil, errors.New("nope")
				}
				So(forwarder.AddEvents(ctx, []*event.Event{dptest.E()}), ShouldNotBeNil)
			})
			Convey("Should be able to send events", func() {
				eventSent := dptest.E()
				eventSent.Timestamp = eventSent.Timestamp.Round(time.Millisecond)
				eventSent.Dimensions["..."] = "testing"
				So(forwarder.AddEvents(ctx, []*event.Event{eventSent}), ShouldBeNil)
				eventSeen := sendTo.NextEvent()
				eventSent.Dimensions["___"] = "testing" // dots should turn into _
				delete(eventSent.Dimensions, "...")
				So(eventSeen.Dimensions, ShouldResemble, eventSent.Dimensions)
				So(eventSeen.Meta, ShouldResemble, eventSent.Meta)
				eventSeen.Dimensions = nil
				eventSent.Dimensions = nil
				eventSeen.Meta = nil
				eventSent.Meta = nil
				So(eventSeen.String(), ShouldEqual, eventSent.String())
			})
			Convey("Should filter event properties/dimensions that are empty", func() {
				eventSent := dptest.E()
				eventSent.Dimensions = map[string]string{"": "test"}
				eventSent.Meta = map[string]interface{}{"": "test"}
				eventSent.Timestamp = eventSent.Timestamp.Round(time.Millisecond)
				So(forwarder.AddEvents(ctx, []*event.Event{eventSent}), ShouldBeNil)
				eventSeen := sendTo.NextEvent()
				eventSent.Dimensions = nil
				eventSent.Meta = nil
				So(eventSeen.String(), ShouldEqual, eventSent.String())
			})
			Reset(func() {
				So(forwarder.Close(), ShouldBeNil)
			})
		})

		trySend := func(body string, contentType string, pathSuffix string) {
			client := http.Client{}
			req, err := http.NewRequest("POST", baseURI+pathSuffix, strings.NewReader(body))
			So(err, ShouldBeNil)
			req.Header.Add("Content-Type", contentType)
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
		}
		verifyStatusCode := func(body string, contentType string, pathSuffix string, expectedStatusCode int) {
			client := http.Client{}
			req, err := http.NewRequest("POST", baseURI+pathSuffix, strings.NewReader(body))
			So(err, ShouldBeNil)
			req.Header.Add("Content-Type", contentType)
			resp, err := client.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, expectedStatusCode)
		}
		Convey("given a json body without a timestamp or category", func() {
			body := bytes.NewBuffer([]byte(`[{"eventType": "mwp.test2", "dimensions": {"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, "properties": {"version": "2015-11-23-4"}}]`))
			Convey("verify the timestamp is filled in and the category becomes USER_DEFINED", func() {
				verifyEventRequest(baseURI, "application/json", "/v2/event", body, sendTo, "mwp.test2", "USER_DEFINED", map[string]string{"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, map[string]interface{}{"version": "2015-11-23-4"}, nil)
			})
		})
		Convey("given a json body with a timestamp and category ALERT", func() {
			body := bytes.NewBuffer([]byte(`[{"category":"ALERT", "eventType": "mwp.test2", "dimensions": {"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, "properties": {"version": "2015-11-23-4"}, "timestamp":12345}]`))
			Convey("verify the timestamp is acurate the category is ALERT", func() {
				verifyEventRequest(baseURI, "application/json", "/v2/event", body, sendTo, "mwp.test2", "ALERT", map[string]string{"instance": "mwpinstance4", "host": "myhost-4", "service": "myservice4"}, map[string]interface{}{"version": "2015-11-23-4"}, nil)
			})
		})
		Convey("Should be able to send a v2 JSON point", func() {
			now := time.Now().Round(time.Second)
			v1Body := fmt.Sprintf(`{"gauge": [{"metric":"bob", "value": 3, "timestamp": %d}]}`, now.UnixNano()/time.Millisecond.Nanoseconds())
			trySend(v1Body, "application/json", "/v2/datapoint")
			datapointSent := datapoint.Datapoint{
				Metric:    "bob",
				Value:     datapoint.NewIntValue(3),
				Timestamp: now,
			}
			datapointSeen := sendTo.Next()
			So(datapointSent.String(), ShouldEqual, datapointSeen.String())
		})
		Convey("invalid requests should error", func() {
			verifyStatusCode("INVALID_JSON", "application/json", "/v1/datapoint", http.StatusBadRequest)
			verifyStatusCode("INVALID_PROTOBUF", "application/x-protobuf", "/v1/datapoint", http.StatusBadRequest)
			verifyStatusCode("INVALID_JSON", "application/json", "/v2/datapoint", http.StatusBadRequest)

			verifyStatusCode("INVALID_JSON", "application/json", "/v1/metric", http.StatusBadRequest)
			verifyStatusCode(`[{"sf_metric":"bob", "sf_metricType":"GAU_GE"}]`, "application/json", "/v1/metric", http.StatusBadRequest)
			verifyStatusCode("INVALID_JSON", "application/json", "/v2/event", http.StatusBadRequest)
			verifyStatusCode("INVALID_PROTOBUF", "application/x-protobuf", "/v2/event", http.StatusBadRequest)

			verifyStatusCode("INVALID_JSON", "application/json", "/v1/event", http.StatusNotFound)

			dps := listener.Datapoints()
			So(dptest.ExactlyOneDims(dps, "total_errors", map[string]string{"type": "sfx_protobuf_v2"}).Value.String(), ShouldEqual, "0")
			verifyStatusCode("INVALID_PROTOBUF", "application/x-protobuf", "/v2/datapoint", http.StatusBadRequest)
			dps = listener.Datapoints()
			So(dptest.ExactlyOneDims(dps, "total_errors", map[string]string{"type": "sfx_protobuf_v2"}).Value.String(), ShouldEqual, "1")
		})
		Convey("Should be able to send a v2 JSON event", func() {
			now := time.Now().Round(time.Second)
			body := fmt.Sprintf(`[{"eventType":"ET", "category": "cat", "timestamp": %d}]`, now.UnixNano()/time.Millisecond.Nanoseconds())
			trySend(body, "application/json", "/v2/event")
			eventSent := event.Event{
				EventType: "ET",
				Category:  "cat",
				Timestamp: now,
			}
			eventSeen := sendTo.NextEvent()
			So(eventSent.String(), ShouldEqual, eventSeen.String())
		})
		Convey("Should be able to send a v1 metric type", func() {
			trySend(`[{"sf_metric":"bob", "sf_metricType": "GAUGE"}]`, "application/json", "/v1/metric")
			So(listener.metricHandler.GetMetricTypeFromMap("bob"), ShouldEqual, com_signalfx_metrics_protobuf.MetricType_GAUGE)
		})
		Convey("Should check v1 JSON errors", func() {
			listener.metricHandler.jsonMarshal = func(v interface{}) ([]byte, error) {
				return nil, errors.New("nope")
			}
			verifyStatusCode(`[{"sf_metric":"bob", "sf_metricType": "GAUGE"}]`, "application/json", "/v1/metric", http.StatusBadRequest)
		})
		Convey("Should be able to send a v1 JSON point", func() {
			datapointSent := datapoint.Datapoint{
				Metric:     "bob",
				Value:      datapoint.NewIntValue(3),
				Dimensions: map[string]string{"sf_source": "name"},
			}
			trySend(`{"metric":"bob", "source": "name", "value": 3}`, "application/json", "/v1/datapoint")
			datapointSeen := sendTo.Next()
			datapointSent.Timestamp = datapointSeen.Timestamp
			So(datapointSent.String(), ShouldEqual, datapointSeen.String())
		})
		Convey("Should ignore missing metric name for v1 json points", func() {
			trySend(`{"source": "name", "value": 3}`, "application/json", "/v1/datapoint")
			So(len(sendTo.PointsChan), ShouldEqual, 0)
		})
		Convey("Should ignore invalid types or value on /v2/datapoint", func() {
			v2Body := `{"gauge": [{"metric":"bob"}], "UNKNOWN":[]}`
			So(len(sendTo.PointsChan), ShouldEqual, 0)
			trySend(v2Body, "application/json", "/v2/datapoint")
			So(len(sendTo.PointsChan), ShouldEqual, 0)
		})
		Convey("Should be able to send a v1 protobuf point", func() {
			datapointSent := datapoint.Datapoint{
				Metric: "metric",
				Value:  datapoint.NewIntValue(3),
			}
			dp := &com_signalfx_metrics_protobuf.DataPoint{
				Metric: pointer.String("metric"),
				Value: &com_signalfx_metrics_protobuf.Datum{
					IntValue: pointer.Int64(3),
				},
			}
			dpBytes, err := proto.Marshal(dp)
			So(err, ShouldBeNil)
			body := bytes.Buffer{}
			body.Write(proto.EncodeVarint(uint64(len(dpBytes))))
			body.Write(dpBytes)
			trySend(body.String(), "application/x-protobuf", "/v1/datapoint")
			datapointSeen := sendTo.Next()
			datapointSent.Timestamp = datapointSeen.Timestamp
			So(datapointSent.String(), ShouldEqual, datapointSeen.String())
		})

		Reset(func() {
			So(listener.Close(), ShouldBeNil)
		})
	})
}
