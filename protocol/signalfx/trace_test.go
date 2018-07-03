package signalfx

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/signalfx/golib/trace"
	. "github.com/smartystreets/goconvey/convey"
)

type fakeSink struct {
	handler func([]*trace.Span)
}

func (s *fakeSink) AddSpans(ctx context.Context, spans []*trace.Span) error {
	s.handler(spans)
	return nil
}

// Benchmark the case where all spans are Zipkin v2.
func BenchmarkTraceDecoder(b *testing.B) {
	spanJSON := `
	 {
       "traceId": "0123456789abcdef",
       "name": "span1",
       "id": "abc1234567890def",
       "kind": "CLIENT",
       "timestamp": 1000,
       "duration": 100,
       "debug": true,
       "shared": true,
       "localEndpoint": {
         "serviceName": "myclient",
         "ipv4": "127.0.0.1"
       },
       "remoteEndpoint": {
         "serviceName": "myserver",
         "ipv4": "127.0.1.1",
         "port": 443
       },
       "annotations": [
         {
           "timestamp": 1001,
           "value": "something happened"
         },
         {
           "timestamp": 1010,
           "value": "something else happened"
         }
       ],
       "tags": {
         "additionalProp1": "string",
         "additionalProp2": "string",
         "additionalProp3": "string"
       }
     }`

	const numSpans = 100
	reqBody := `[`
	for i := 0; i < numSpans; i++ {
		reqBody += spanJSON + ","
	}

	reqBody = strings.TrimSuffix(reqBody, ",") + `]`

	decoder := JSONTraceDecoderV1{
		Sink: &fakeSink{
			handler: func(_ []*trace.Span) {},
		},
	}

	req := http.Request{
		Body: ioutil.NopCloser(bytes.NewBufferString(reqBody)),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := decoder.Read(context.Background(), &req)
		if err != nil {
			fmt.Println(i)
			b.Fatal(err)
		}
		req = http.Request{
			Body: ioutil.NopCloser(bytes.NewBufferString(reqBody)),
		}
	}
	b.StopTimer()
}

func strAddr(s string) *string {
	return &s
}

func float64Addr(u uint64) *float64 {
	f := float64(u)
	return &f
}

func int32Addr(i int32) *int32 {
	return &i
}

func interfaceAddr(i interface{}) *interface{} {
	return &i
}

var falseVar = false

func TestTraceDecoder(t *testing.T) {
	reqBody := ioutil.NopCloser(bytes.NewBufferString(`[
	 {
       "traceId": "0123456789abcdef",
       "name": "span1",
       "id": "abc1234567890def",
       "kind": "CLIENT",
       "timestamp": 1000,
       "duration": 100,
       "debug": true,
       "shared": true,
       "localEndpoint": {
         "serviceName": "myclient",
         "ipv4": "127.0.0.1"
       },
       "remoteEndpoint": {
         "serviceName": "myserver",
         "ipv4": "127.0.1.1",
         "port": 443
       },
       "annotations": [
         {
           "timestamp": 1001,
           "value": "something happened"
         }
       ],
       "tags": {
         "additionalProp1": "string",
         "additionalProp2": "string",
         "additionalProp3": "string"
       }
     },
	 {
       "traceId": "abcdef0123456789",
       "name": "span2",
       "parentId": "0123456789abcdef",
       "id": "abcdef",
       "kind": "SERVER",
       "timestamp": 2000,
       "duration": 200,
       "debug": false,
       "shared": false,
       "tags": {
         "additionalProp1": "string",
         "additionalProp2": "string",
         "additionalProp3": "string"
       }
     },
	 {
       "traceId": "abcdef0123456789",
       "name": "span2",
       "parentId": "0123456789abcdef",
       "id": "badspan-cannot-have-kind-and-binary-annotations",
       "kind": "SERVER",
       "timestamp": 2000,
       "duration": 200,
       "debug": false,
       "shared": false,
	   "binaryAnnotations": [
	     {"key": "a", "value": "v"}
	   ]
     },
	 {
       "traceId": "abcdef0123456789",
       "name": "span2",
       "parentId": "0123456789abcdef",
       "id": "badspan-cannot-have-object-as-binary-annotation-value",
       "timestamp": 2000,
       "duration": 200,
       "debug": false,
       "shared": false,
	   "binaryAnnotations": [
	     {"key": "a", "value": {"k": "v"}}
	   ]
     },
	 {
       "traceId": "abcdef0123456789",
       "name": "span3",
       "parentId": "0123456789abcdef",
       "id": "oldspan",
       "timestamp": 2000,
       "duration": 200,
	   "binaryAnnotations": [
	     {"key": "a", "value": "v"}
	   ]
     }
	]`))

	spans := []*trace.Span{}
	decoder := JSONTraceDecoderV1{
		Sink: &fakeSink{
			handler: func(ss []*trace.Span) {
				spans = append(spans, ss...)
			},
		},
	}

	req := http.Request{
		Body: reqBody,
	}
	err := decoder.Read(context.Background(), &req)

	Convey("Valid spans should be sent even if some error", t, func() {
		So(err.Error(), ShouldContainSubstring, "invalid binary annotation type")

		So(spans, ShouldResemble, []*trace.Span{
			{
				TraceID:  "0123456789abcdef",
				ParentID: nil,
				ID:       "abc1234567890def",
				Name:     strAddr("span1"),
				Kind:     &ClientKind,
				LocalEndpoint: &trace.Endpoint{
					ServiceName: strAddr("myclient"),
					Ipv4:        strAddr("127.0.0.1"),
				},
				RemoteEndpoint: &trace.Endpoint{
					ServiceName: strAddr("myserver"),
					Ipv4:        strAddr("127.0.1.1"),
					Port:        int32Addr(443),
				},
				Timestamp: float64Addr(1000),
				Duration:  float64Addr(100),
				Debug:     &trueVar,
				Shared:    &trueVar,
				Annotations: []*trace.Annotation{
					{Timestamp: float64Addr(1001), Value: strAddr("something happened")},
				},
				Tags: map[string]string{
					"additionalProp1": "string",
					"additionalProp2": "string",
					"additionalProp3": "string",
				},
			},
			{
				TraceID:        "abcdef0123456789",
				ParentID:       strAddr("0123456789abcdef"),
				ID:             "abcdef",
				Name:           strAddr("span2"),
				Kind:           &ServerKind,
				LocalEndpoint:  nil,
				RemoteEndpoint: nil,
				Timestamp:      float64Addr(2000),
				Duration:       float64Addr(200),
				Debug:          &falseVar,
				Shared:         &falseVar,
				Tags: map[string]string{
					"additionalProp1": "string",
					"additionalProp2": "string",
					"additionalProp3": "string",
				},
			},
			{
				TraceID:   "abcdef0123456789",
				ParentID:  strAddr("0123456789abcdef"),
				ID:        "oldspan",
				Name:      strAddr("span3"),
				Timestamp: float64Addr(2000),
				Duration:  float64Addr(200),
				Tags: map[string]string{
					"a": "v",
				},
			},
		})
	})
}

// Tests converted from
// https://github.com/openzipkin/zipkin/blob/2.8.4/zipkin/src/test/java/zipkin/internal/V2SpanConverterTest.java
func TestTraceConversion(t *testing.T) {
	frontend := &trace.Endpoint{
		ServiceName: strAddr("frontend"),
		Ipv4:        strAddr("127.0.0.1"),
	}

	backend := &trace.Endpoint{
		ServiceName: strAddr("backend"),
		Ipv4:        strAddr("192.168.99.101"),
		Port:        int32Addr(9000),
	}

	kafka := &trace.Endpoint{
		ServiceName: strAddr("kafka"),
	}

	Convey("Zipkin v2 spans gets passed through unaltered", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:       strAddr("6b221d5bc9e6496c"),
				ID:             "5b4185666d50f68b",
				Name:           strAddr("get"),
				Kind:           &ClientKind,
				LocalEndpoint:  frontend,
				RemoteEndpoint: backend,
				Timestamp:      float64Addr(1472470996199000),
				Duration:       float64Addr(207000),
				Annotations: []*trace.Annotation{
					{Timestamp: float64Addr(1472470996403000), Value: strAddr("no")},
					{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws")},
					{Timestamp: float64Addr(1472470996403000), Value: strAddr("wr")},
				},
				Tags: map[string]string{
					"http_path":            "/api",
					"clnt/finagle.version": "6.45.0",
				},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span.Span})
	})

	Convey("client", t, func() {
		simpleClient := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("get"),
			Kind:           &ClientKind,
			LocalEndpoint:  frontend,
			RemoteEndpoint: backend,
			Timestamp:      float64Addr(1472470996199000),
			Duration:       float64Addr(207000),
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("no")},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws")},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("wr")},
			},
			Tags: map[string]string{
				"http_path":            "/api",
				"clnt/finagle.version": "6.45.0",
			},
		}

		client := inputSpan{
			Span: trace.Span{TraceID: "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cs"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("wr"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("cr"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("no"), Endpoint: frontend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("http_path"), Value: interfaceAddr("/api"), Endpoint: frontend},
				{Key: strAddr("clnt/finagle.version"), Value: interfaceAddr("6.45.0"), Endpoint: frontend},
				{Key: strAddr("sa"), Value: interfaceAddr(true), Endpoint: backend},
			},
		}

		sp, _ := client.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&simpleClient})
	})

	Convey("client_unfinished", t, func() {
		simpleClient := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("get"),
			Kind:          &ClientKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws")},
			},
			Tags: map[string]string{},
		}

		client := &inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cs"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws"), Endpoint: frontend},
			},
		}

		sp, _ := client.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&simpleClient})
	})

	Convey("client_unstarted", t, func() {
		simpleClient := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("get"),
			Kind:          &ClientKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(100),
			Tags:          map[string]string{},
		}

		client := &inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(100),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199100), Value: strAddr("cr"), Endpoint: frontend},
			},
		}

		sp, _ := client.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&simpleClient})
	})

	Convey("noAnnotationsExceptAddresses", t, func() {
		span2 := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("get"),
			LocalEndpoint:  frontend,
			RemoteEndpoint: backend,
			Timestamp:      float64Addr(1472470996199000),
			Duration:       float64Addr(207000),
			Tags:           map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("ca"), Value: interfaceAddr(true), Endpoint: frontend},
				{Key: strAddr("sa"), Value: interfaceAddr(true), Endpoint: backend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("fromSpan_redundantAddressAnnotations", t, func() {
		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Kind:          &ClientKind,
			Name:          strAddr("get"),
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(207000),
			Tags:          map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cs"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("cr"), Endpoint: frontend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("ca"), Value: interfaceAddr(true), Endpoint: frontend},
				{Key: strAddr("sa"), Value: interfaceAddr(true), Endpoint: frontend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("server", t, func() {
		simpleServer := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ID:             "216a2aea45d08fc9",
			Name:           strAddr("get"),
			Kind:           &ServerKind,
			LocalEndpoint:  backend,
			RemoteEndpoint: frontend,
			Timestamp:      float64Addr(1472470996199000),
			Duration:       float64Addr(207000),
			Tags: map[string]string{
				"http_path":            "/api",
				"clnt/finagle.version": "6.45.0",
			},
		}

		server := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ID:        "216a2aea45d08fc9",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("sr"), Endpoint: backend},
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("ss"), Endpoint: backend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("http_path"), Value: interfaceAddr("/api"), Endpoint: backend},
				{Key: strAddr("clnt/finagle.version"), Value: interfaceAddr("6.45.0"), Endpoint: backend},
				{Key: strAddr("ca"), Value: interfaceAddr(true), Endpoint: frontend},
			},
		}

		sp, _ := server.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&simpleServer})
	})

	Convey("client_missingCs", t, func() {
		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ID:            "216a2aea45d08fc9",
			Name:          strAddr("get"),
			Kind:          &ClientKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(207000),
			Tags:          map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ID:        "216a2aea45d08fc9",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("cs"), Endpoint: frontend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("server_missingSr", t, func() {
		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ID:            "216a2aea45d08fc9",
			Name:          strAddr("get"),
			Kind:          &ServerKind,
			LocalEndpoint: backend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(207000),
			Tags:          map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ID:        "216a2aea45d08fc9",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("ss"), Endpoint: backend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("missingEndpoints", t, func() {
		span2 := trace.Span{
			TraceID:   "1",
			ParentID:  strAddr("1"),
			ID:        "2",
			Name:      strAddr("foo"),
			Timestamp: float64Addr(1472470996199000),
			Duration:  float64Addr(207000),
			Tags:      map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:   "1",
				ParentID:  strAddr("1"),
				ID:        "2",
				Name:      strAddr("foo"),
				Duration:  float64Addr(207000),
				Timestamp: float64Addr(1472470996199000),
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("missingEndpoints_coreAnnotation", t, func() {
		span2 := trace.Span{
			TraceID:   "1",
			ParentID:  strAddr("1"),
			ID:        "2",
			Name:      strAddr("foo"),
			Timestamp: float64Addr(1472470996199000),
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("sr")},
			},
			Tags: map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:   "1",
				ParentID:  strAddr("1"),
				ID:        "2",
				Name:      strAddr("foo"),
				Timestamp: float64Addr(1472470996199000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("sr"), Endpoint: nil},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("incomplete_only_sr", t, func() {
		span2 := trace.Span{
			TraceID:       "1",
			ParentID:      strAddr("1"),
			ID:            "2",
			Name:          strAddr("foo"),
			Kind:          &ServerKind,
			Timestamp:     float64Addr(1472470996199000),
			Shared:        &trueVar,
			LocalEndpoint: backend,
			Tags:          map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:  "1",
				ParentID: strAddr("1"),
				ID:       "2",
				Name:     strAddr("foo"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("sr"), Endpoint: backend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("lateRemoteEndpoint_ss", t, func() {
		span2 := trace.Span{
			TraceID:        "1",
			ParentID:       strAddr("1"),
			ID:             "2",
			Name:           strAddr("foo"),
			Kind:           &ServerKind,
			LocalEndpoint:  backend,
			RemoteEndpoint: frontend,
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("ss")},
			},
			Tags: map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:  "1",
				ParentID: strAddr("1"),
				ID:       "2",
				Name:     strAddr("foo"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("ss"), Endpoint: backend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("ca"), Value: interfaceAddr(true), Endpoint: frontend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	/** Late flushed data on a server span */
	Convey("lateRemoteEndpoint_ca", t, func() {
		span2 := trace.Span{
			TraceID:        "1",
			ParentID:       strAddr("1"),
			ID:             "2",
			Name:           strAddr("foo"),
			Kind:           &ServerKind,
			RemoteEndpoint: frontend,
			Tags:           map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:  "1",
				ParentID: strAddr("1"),
				ID:       "2",
				Name:     strAddr("foo"),
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("ca"), Value: interfaceAddr(true), Endpoint: frontend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("lateRemoteEndpoint_cr", t, func() {
		span2 := trace.Span{
			TraceID:        "1",
			ParentID:       strAddr("1"),
			ID:             "2",
			Name:           strAddr("foo"),
			Kind:           &ClientKind,
			LocalEndpoint:  frontend,
			RemoteEndpoint: backend,
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cr")},
			},
			Tags: map[string]string{},
		}

		span := inputSpan{
			Span: trace.Span{
				TraceID:  "1",
				ParentID: strAddr("1"),
				ID:       "2",
				Name:     strAddr("foo"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cr"), Endpoint: frontend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("sa"), Value: interfaceAddr(true), Endpoint: backend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("lateRemoteEndpoint_sa", t, func() {
		span2 := trace.Span{
			TraceID:        "1",
			ParentID:       strAddr("1"),
			ID:             "2",
			Name:           strAddr("foo"),
			Kind:           &ClientKind,
			RemoteEndpoint: backend,
			Tags:           map[string]string{},
		}

		span := inputSpan{

			Span: trace.Span{
				TraceID:  "1",
				ParentID: strAddr("1"),
				ID:       "2",
				Name:     strAddr("foo"),
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("sa"), Value: interfaceAddr(true), Endpoint: backend},
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("localSpan_emptyComponent", t, func() {
		simpleLocal := trace.Span{
			TraceID:       "1",
			ParentID:      strAddr("1"),
			ID:            "2",
			Name:          strAddr("local"),
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(207000),
			Tags:          map[string]string{},
		}

		local := inputSpan{
			Span: trace.Span{
				TraceID:   "1",
				ParentID:  strAddr("1"),
				ID:        "2",
				Name:      strAddr("local"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},

			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("lc"), Value: interfaceAddr(""), Endpoint: frontend},
			},
		}

		sp, _ := local.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&simpleLocal})
	})

	Convey("clientAndServer", t, func() {
		noNameService := &trace.Endpoint{
			ServiceName: nil,
			Ipv4:        strAddr("127.0.0.1"),
		}
		shared := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cs"), Endpoint: noNameService},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws"), Endpoint: noNameService},
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("sr"), Endpoint: backend},
				{Timestamp: float64Addr(1472470996350000), Value: strAddr("ss"), Endpoint: backend},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("wr"), Endpoint: noNameService},
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("cr"), Endpoint: noNameService},
			},

			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("http_path"), Value: interfaceAddr("/api"), Endpoint: noNameService},
				{Key: strAddr("http_path"), Value: interfaceAddr("/backend"), Endpoint: backend},
				{Key: strAddr("clnt/finagle.version"), Value: interfaceAddr("6.45.0"), Endpoint: noNameService},
				{Key: strAddr("srv/finagle.version"), Value: interfaceAddr("6.44.0"), Endpoint: backend},
				{Key: strAddr("ca"), Value: interfaceAddr(true), Endpoint: noNameService},
				{Key: strAddr("sa"), Value: interfaceAddr(true), Endpoint: backend},
			},
		}

		// the client side owns timestamp and duration
		client := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("get"),
			Kind:           &ClientKind,
			LocalEndpoint:  noNameService,
			RemoteEndpoint: backend,
			Timestamp:      float64Addr(1472470996199000),
			Duration:       float64Addr(207000),
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws")},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("wr")},
			},
			Tags: map[string]string{
				"http_path":            "/api",
				"clnt/finagle.version": "6.45.0",
			},
		}

		// notice server tags are different than the client, and the client's annotations aren't here
		server := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("get"),
			Kind:           &ServerKind,
			Shared:         &trueVar,
			LocalEndpoint:  backend,
			RemoteEndpoint: noNameService,
			Timestamp:      float64Addr(1472470996250000),
			Duration:       float64Addr(100000),
			Tags: map[string]string{
				"http_path":           "/backend",
				"srv/finagle.version": "6.44.0",
			},
		}

		sp, _ := shared.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&client, &server})
	})

	/**
	 * The old span format had no means of saying it is shared or not. This uses lack of timestamp as
	 * a signal
	 */
	Convey("assumesServerWithoutTimestampIsShared", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:  "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID: strAddr("6b221d5bc9e6496c"),
				ID:       "5b4185666d50f68b",
				Name:     strAddr("get"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("sr"), Endpoint: backend},
				{Timestamp: float64Addr(1472470996350000), Value: strAddr("ss"), Endpoint: backend},
			},
		}

		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("get"),
			Kind:          &ServerKind,
			Shared:        &trueVar,
			LocalEndpoint: backend,
			Timestamp:     float64Addr(1472470996250000),
			Duration:      float64Addr(100000),
			Tags:          map[string]string{},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("clientAndServer_loopback", t, func() {
		shared := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("get"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(207000),
			},

			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cs"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("sr"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996350000), Value: strAddr("ss"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("cr"), Endpoint: frontend},
			},
		}

		client := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("get"),
			Kind:          &ClientKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(207000),
			Tags:          map[string]string{},
		}

		server := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("get"),
			Kind:          &ServerKind,
			Shared:        &trueVar,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996250000),
			Duration:      float64Addr(100000),
			Tags:          map[string]string{},
		}

		sp, _ := shared.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&client, &server})
	})

	Convey("oneway_loopback", t, func() {
		shared := inputSpan{
			Span: trace.Span{
				TraceID:  "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID: strAddr("6b221d5bc9e6496c"),
				ID:       "5b4185666d50f68b",
				Name:     strAddr("get"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("cs"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("sr"), Endpoint: frontend},
			},
		}

		client := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("get"),
			Kind:          &ClientKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Tags:          map[string]string{},
		}

		server := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("get"),
			Kind:          &ServerKind,
			Shared:        &trueVar,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996250000),
			Tags:          map[string]string{},
		}

		sp, _ := shared.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&client, &server})
	})

	Convey("producer", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:  "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID: strAddr("6b221d5bc9e6496c"),
				ID:       "5b4185666d50f68b",
				Name:     strAddr("send"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("ms"), Endpoint: frontend},
			},
		}

		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("send"),
			Kind:          &ProducerKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Tags:          map[string]string{},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("producer_remote", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("send"),
				Timestamp: float64Addr(1472470996199000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("ms"), Endpoint: frontend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("ma"), Value: interfaceAddr(true), Endpoint: kafka},
			},
		}

		span2 := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("send"),
			Kind:           &ProducerKind,
			LocalEndpoint:  frontend,
			Timestamp:      float64Addr(1472470996199000),
			RemoteEndpoint: kafka,
			Tags:           map[string]string{},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("producer_duration", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("send"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(51000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("ms"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("ws"), Endpoint: frontend},
			},
		}

		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("send"),
			Kind:          &ProducerKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(51000),
			Tags:          map[string]string{},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("consumer", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("send"),
				Timestamp: float64Addr(1472470996199000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("mr"), Endpoint: frontend},
			},
		}

		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("send"),
			Kind:          &ConsumerKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Tags:          map[string]string{},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("consumer_remote", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("send"),
				Timestamp: float64Addr(1472470996199000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("mr"), Endpoint: frontend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("ma"), Value: interfaceAddr(true), Endpoint: kafka},
			},
		}

		span2 := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("send"),
			Kind:           &ConsumerKind,
			LocalEndpoint:  frontend,
			RemoteEndpoint: kafka,
			Timestamp:      float64Addr(1472470996199000),
			Tags:           map[string]string{},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("consumer_duration", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID:   "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID:  strAddr("6b221d5bc9e6496c"),
				ID:        "5b4185666d50f68b",
				Name:      strAddr("send"),
				Timestamp: float64Addr(1472470996199000),
				Duration:  float64Addr(51000),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("wr"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("mr"), Endpoint: frontend},
			},
		}

		span2 := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("send"),
			Kind:          &ConsumerKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(51000),
			Tags:          map[string]string{},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	/** shared span IDs for messaging spans isn't supported, but shouldn't break */
	Convey("producerAndConsumer", t, func() {
		shared := inputSpan{
			Span: trace.Span{
				TraceID:  "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID: strAddr("6b221d5bc9e6496c"),
				ID:       "5b4185666d50f68b",
				Name:     strAddr("whatev"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("ms"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws"), Endpoint: frontend},
				{Timestamp: nil, Value: strAddr("wr"), Endpoint: backend},
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("mr"), Endpoint: backend},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("ma"), Value: interfaceAddr(true), Endpoint: kafka},
			},
		}

		producer := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("whatev"),
			Kind:           &ProducerKind,
			LocalEndpoint:  frontend,
			RemoteEndpoint: kafka,
			Timestamp:      float64Addr(1472470996199000),
			Duration:       float64Addr(1472470996238000 - 1472470996199000),
			Tags:           map[string]string{},
		}

		consumer := trace.Span{
			TraceID:        "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:       strAddr("6b221d5bc9e6496c"),
			ID:             "5b4185666d50f68b",
			Name:           strAddr("whatev"),
			Kind:           &ConsumerKind,
			Shared:         &trueVar,
			LocalEndpoint:  backend,
			RemoteEndpoint: kafka,
			Timestamp:      float64Addr(1472470996406000),
			Duration:       nil,
			Tags:           map[string]string{},
		}

		sp, _ := shared.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&producer, &consumer})
	})

	/** shared span IDs for messaging spans isn't supported, but shouldn't break */
	Convey("producerAndConsumer_loopback_shared", t, func() {
		shared := inputSpan{
			Span: trace.Span{
				TraceID:  "7180c278b62e8f6a216a2aea45d08fc9",
				ParentID: strAddr("6b221d5bc9e6496c"),
				ID:       "5b4185666d50f68b",
				Name:     strAddr("message"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("ms"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("ws"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("wr"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996406000), Value: strAddr("mr"), Endpoint: frontend},
			},
		}

		producer := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("message"),
			Kind:          &ProducerKind,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996199000),
			Duration:      float64Addr(1472470996238000 - 1472470996199000),
			Tags:          map[string]string{},
		}

		consumer := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ParentID:      strAddr("6b221d5bc9e6496c"),
			ID:            "5b4185666d50f68b",
			Name:          strAddr("message"),
			Kind:          &ConsumerKind,
			Shared:        &trueVar,
			LocalEndpoint: frontend,
			Timestamp:     float64Addr(1472470996403000),
			Duration:      float64Addr(1472470996406000 - 1472470996403000),
			Tags:          map[string]string{},
		}

		sp, _ := shared.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&producer, &consumer})
	})

	Convey("dataMissingEndpointGoesOnFirstSpan", t, func() {
		shared := inputSpan{
			Span: trace.Span{
				TraceID: "7180c278b62e8f6a216a2aea45d08fc9",
				ID:      "5b4185666d50f68b",
				Name:    strAddr("missing"),
			},
			Annotations: []*annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("foo"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("bar"), Endpoint: frontend},
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("baz"), Endpoint: backend},
				{Timestamp: float64Addr(1472470996350000), Value: strAddr("qux"), Endpoint: backend},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("missing"), Endpoint: nil},
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("foo"), Value: interfaceAddr("bar"), Endpoint: frontend},
				{Key: strAddr("baz"), Value: interfaceAddr("qux"), Endpoint: backend},
				{Key: strAddr("missing"), Value: interfaceAddr(""), Endpoint: nil},
			},
		}

		first := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ID:            "5b4185666d50f68b",
			Name:          strAddr("missing"),
			LocalEndpoint: frontend,
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996199000), Value: strAddr("foo")},
				{Timestamp: float64Addr(1472470996238000), Value: strAddr("bar")},
				{Timestamp: float64Addr(1472470996403000), Value: strAddr("missing")},
			},
			Tags: map[string]string{
				"foo":     "bar",
				"missing": "",
			},
		}

		second := trace.Span{
			TraceID:       "7180c278b62e8f6a216a2aea45d08fc9",
			ID:            "5b4185666d50f68b",
			Name:          strAddr("missing"),
			LocalEndpoint: backend,
			Annotations: []*trace.Annotation{
				{Timestamp: float64Addr(1472470996250000), Value: strAddr("baz")},
				{Timestamp: float64Addr(1472470996350000), Value: strAddr("qux")},
			},
			Tags: map[string]string{
				"baz": "qux",
			},
		}

		sp, _ := shared.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&first, &second})
	})

	Convey("convertBinaryAnnotations", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID: "1",
				Name:    strAddr("test"),
				ID:      "2",
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("bool"), Value: interfaceAddr(true), Endpoint: frontend},
				{Key: strAddr("bytes"), Value: interfaceAddr([]byte("hello")), Endpoint: frontend},
				{Key: strAddr("short"), Value: interfaceAddr(uint16(20)), Endpoint: frontend},
				{Key: strAddr("int"), Value: interfaceAddr(int32(32800)), Endpoint: frontend},
				{Key: strAddr("long"), Value: interfaceAddr(int64(2147483700)), Endpoint: frontend},
				{Key: strAddr("double"), Value: interfaceAddr(3.1415), Endpoint: frontend},
				{Key: strAddr("novalue"), Value: nil, Endpoint: frontend},
			},
		}

		span2 := trace.Span{
			TraceID:       "1",
			Name:          strAddr("test"),
			ID:            "2",
			LocalEndpoint: frontend,
			Tags: map[string]string{
				"bool":   "true",
				"bytes":  "hello",
				"short":  "20",
				"int":    "32800",
				"long":   "2147483700",
				"double": "3.1415",
			},
		}

		sp, _ := span.fromZipkinV1()
		So(sp, ShouldResemble, []*trace.Span{&span2})
	})

	Convey("convertBadBinaryAnnotations", t, func() {
		span := inputSpan{
			Span: trace.Span{
				TraceID: "1",
				Name:    strAddr("test"),
				ID:      "2",
			},
			BinaryAnnotations: []*binaryAnnotation{
				{Key: strAddr("badtype"), Value: interfaceAddr([]int{1, 2, 3}), Endpoint: frontend},
			},
		}

		_, err := span.fromZipkinV1()
		So(err.Error(), ShouldContainSubstring, "invalid binary annotation type")
	})
}
