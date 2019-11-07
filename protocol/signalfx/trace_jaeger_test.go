package signalfx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	jThrift "github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/signalfx/golib/v3/pointer"
	"github.com/signalfx/golib/v3/trace"
	. "github.com/smartystreets/goconvey/convey"
)

const jaegerBatchJSON = `
{
  "process": {
    "serviceName": "api",
    "tags": [
      {
        "key": "hostname",
        "vType": "STRING",
        "vStr": "api246-sjc1"
      },
      {
        "key": "ip",
        "vType": "STRING",
        "vStr": "10.53.69.61"
      },
      {
        "key": "jaeger.version",
        "vType": "STRING",
        "vStr": "Python-3.1.0"
      }
    ]
  },
  "spans": [
    {
      "traceIdLow": 5951113872249657919,
      "spanId": 6585752,
      "parentSpanId": 6866147,
      "operationName": "get",
      "startTime": 1485467191639875,
      "duration": 22938,
      "tags": [
        {
          "key": "http.url",
          "vType": "STRING",
          "vStr": "http://127.0.0.1:15598/client_transactions"
        },
        {
          "key": "span.kind",
          "vType": "STRING",
          "vStr": "server"
        },
        {
          "key": "peer.port",
          "vType": "LONG",
          "vLong": 53931
        },
        {
          "key": "someBool",
          "vType": "BOOL",
          "vBool": true
        },
        {
          "key": "someFalseBool",
          "vType": "BOOL",
          "vBool": false
        },
        {
          "key": "someDouble",
          "vType": "DOUBLE",
          "vDouble": 129.8
        },
        {
          "key": "peer.service",
          "vType": "STRING",
          "vStr": "rtapi"
        },
        {
          "key": "peer.ipv4",
          "vType": "LONG",
          "vLong": 3224716605
        }
      ],
      "logs": [
        {
          "timestamp": 1485467191639875,
          "fields": [
            {
              "key": "key1",
              "vType": "STRING",
              "vStr": "value1"
            },
            {
              "key": "key2",
              "vType": "STRING",
              "vStr": "value2"
            }
          ]
        },
        {
          "timestamp": 1485467191639875,
          "fields": [
            {
              "key": "event",
              "vType": "STRING",
              "vStr": "nothing"
            }
          ]
        }
      ]
    },
    {
      "traceIdLow": 5951113872249657919,
      "traceIdHigh": 1,
      "spanId": 27532398882098234,
      "parentSpanId": 6866147,
      "operationName": "post",
      "startTime": 1485467191639875,
      "duration": 22938,
      "tags": [
        {
          "key": "span.kind",
          "vType": "STRING",
          "vStr": "client"
        },
        {
          "key": "peer.port",
          "vType": "STRING",
          "vStr": "53931"
        },
        {
          "key": "peer.ipv4",
          "vType": "STRING",
          "vStr": "10.0.0.1"
        }
      ]
    },
    {
      "traceIdLow": 5951113872249657919,
      "spanId": 27532398882098234,
      "parentSpanId": 6866147,
      "operationName": "post",
      "startTime": 1485467191639875,
	  "flags": "",
      "duration": 22938,
      "tags": [
        {
          "key": "span.kind",
          "vType": "STRING",
          "vStr": "consumer"
        },
        {
          "key": "peer.port",
          "vType": "BOOL",
          "vBool": "true"
        },
        {
          "key": "peer.ipv4",
          "vType": "BOOL",
          "vBool": "false"
        }
      ]
    },
    {
      "traceIdLow": 5951113872249657919,
      "spanId": 27532398882098234,
      "operationName": "post",
      "startTime": 1485467191639875,
	  "flags": 2,
      "duration": 22938,
      "tags": [
        {
          "key": "span.kind",
          "vType": "STRING",
          "vStr": "producer"
        },
        {
          "key": "peer.ipv6",
          "vType": "STRING",
		  "vStr": "::1"
        }
      ],
      "references": [
        {
          "refType": "FOLLOWS_FROM",
          "traceIdLow": 5951113872249657919,
          "spanId": 6866148
        },
        {
          "refType": "CHILD_OF",
          "traceIdLow": 5951113872249657919,
          "spanId": 6866147
        }
      ]
    },
    {
      "traceIdLow": 5951113872249657919,
      "spanId": 27532398882098234,
      "parentSpanId": 6866147,
      "operationName": "post",
      "startTime": 1485467191639875,
	  "flags": 2,
      "duration": 22938,
      "tags": [
        {
          "key": "span.kind",
          "vType": "STRING",
          "vStr": "producer"
        },
        {
          "key": "peer.ipv6",
          "vType": "STRING",
		  "vStr": "::1"
        },
        {
          "key": "elements",
          "vType": "LONG",
		  "vLong": 100
        },
		{
		  "key": "binData",
		  "vType": "BINARY",
		  "vBinary": "abc"
	    },
        {
          "key": "badType",
          "vType": "NOTATYPE"
        }
      ]
    }
  ]

}
`

func TestJaegerTraceDecoder(t *testing.T) {
	var batch jThrift.Batch
	err := json.Unmarshal([]byte(jaegerBatchJSON), &batch)
	batchBytes, err := thrift.NewTSerializer().Write(&batch)
	if err != nil {
		panic("couldn't serialize test batch to thrift")
	}

	reqBody := ioutil.NopCloser(bytes.NewBuffer(batchBytes))

	spans := []*trace.Span{}
	decoder := NewJaegerThriftTraceDecoderV1(nil,
		&fakeSink{
			handler: func(ss []*trace.Span) {
				spans = append(spans, ss...)
			},
		})

	Convey("Bad request body should error out", t, func() {
		req := http.Request{
			Body: ioutil.NopCloser(&errorReader{}),
		}
		err = decoder.Read(context.Background(), &req)

		So(err.Error(), ShouldEqual, "could not read request body")
	})

	Convey("Spans should be decoded properly", t, func() {
		req := http.Request{
			Body: reqBody,
		}
		err = decoder.Read(context.Background(), &req)

		So(err, ShouldBeNil)

		So(spans, ShouldResemble, []*trace.Span{
			{
				TraceID:  "52969a8955571a3f",
				ParentID: pointer.String("000000000068c4e3"),
				ID:       "0000000000647d98",
				Name:     pointer.String("get"),
				Kind:     &ServerKind,
				LocalEndpoint: &trace.Endpoint{
					ServiceName: pointer.String("api"),
					Ipv4:        pointer.String("10.53.69.61"),
				},
				RemoteEndpoint: &trace.Endpoint{
					ServiceName: pointer.String("rtapi"),
					Ipv4:        pointer.String("192.53.69.61"),
					Port:        pointer.Int32(53931),
				},
				Timestamp: pointer.Int64(1485467191639875),
				Duration:  pointer.Int64(22938),
				Debug:     nil,
				Shared:    nil,
				Annotations: []*trace.Annotation{
					{Timestamp: pointer.Int64(1485467191639875), Value: pointer.String("{\"key1\":\"value1\",\"key2\":\"value2\"}")},
					{Timestamp: pointer.Int64(1485467191639875), Value: pointer.String("nothing")},
				},
				Tags: map[string]string{
					"http.url":       "http://127.0.0.1:15598/client_transactions",
					"someBool":       "true",
					"someFalseBool":  "false",
					"someDouble":     "129.8",
					"hostname":       "api246-sjc1",
					"jaeger.version": "Python-3.1.0",
				},
			},
			{
				TraceID:  "000000000000000152969a8955571a3f",
				ParentID: pointer.String("000000000068c4e3"),
				ID:       "0061d092272e8c3a",
				Name:     pointer.String("post"),
				Kind:     &ClientKind,
				LocalEndpoint: &trace.Endpoint{
					ServiceName: pointer.String("api"),
					Ipv4:        pointer.String("10.53.69.61"),
				},
				RemoteEndpoint: &trace.Endpoint{
					Ipv4: pointer.String("10.0.0.1"),
					Port: pointer.Int32(53931),
				},
				Timestamp:   pointer.Int64(1485467191639875),
				Duration:    pointer.Int64(22938),
				Debug:       nil,
				Shared:      nil,
				Annotations: []*trace.Annotation{},
				Tags: map[string]string{
					"hostname":       "api246-sjc1",
					"jaeger.version": "Python-3.1.0",
				},
			},
			{
				TraceID:  "52969a8955571a3f",
				ParentID: pointer.String("000000000068c4e3"),
				ID:       "0061d092272e8c3a",
				Name:     pointer.String("post"),
				Kind:     &ConsumerKind,
				LocalEndpoint: &trace.Endpoint{
					ServiceName: pointer.String("api"),
					Ipv4:        pointer.String("10.53.69.61"),
				},
				RemoteEndpoint: nil,
				Timestamp:      pointer.Int64(1485467191639875),
				Duration:       pointer.Int64(22938),
				Debug:          nil,
				Shared:         nil,
				Annotations:    []*trace.Annotation{},
				Tags: map[string]string{
					"hostname":       "api246-sjc1",
					"jaeger.version": "Python-3.1.0",
				},
			},
			{
				TraceID:  "52969a8955571a3f",
				ParentID: pointer.String("000000000068c4e3"),
				ID:       "0061d092272e8c3a",
				Name:     pointer.String("post"),
				Kind:     &ProducerKind,
				LocalEndpoint: &trace.Endpoint{
					ServiceName: pointer.String("api"),
					Ipv4:        pointer.String("10.53.69.61"),
				},
				RemoteEndpoint: &trace.Endpoint{
					Ipv6: pointer.String("::1"),
				},
				Timestamp:   pointer.Int64(1485467191639875),
				Duration:    pointer.Int64(22938),
				Debug:       pointer.Bool(true),
				Shared:      nil,
				Annotations: []*trace.Annotation{},
				Tags: map[string]string{
					"hostname":       "api246-sjc1",
					"jaeger.version": "Python-3.1.0",
				},
			},
			{
				TraceID:  "52969a8955571a3f",
				ParentID: pointer.String("000000000068c4e3"),
				ID:       "0061d092272e8c3a",
				Name:     pointer.String("post"),
				Kind:     &ProducerKind,
				LocalEndpoint: &trace.Endpoint{
					ServiceName: pointer.String("api"),
					Ipv4:        pointer.String("10.53.69.61"),
				},
				RemoteEndpoint: &trace.Endpoint{
					Ipv6: pointer.String("::1"),
				},
				Timestamp:   pointer.Int64(1485467191639875),
				Duration:    pointer.Int64(22938),
				Debug:       pointer.Bool(true),
				Shared:      nil,
				Annotations: []*trace.Annotation{},
				Tags: map[string]string{
					"elements":       "100",
					"hostname":       "api246-sjc1",
					"jaeger.version": "Python-3.1.0",
				},
			},
		})
	})
}

func BenchmarkJaegerTraceDecoder(b *testing.B) {
	spanJSON := `
        {
          "traceIdLow": 5951113872249657919,
          "spanId": 6585752,
          "parentSpanId": 6866147,
          "operationName": "get",
          "startTime": 1485467191639875,
          "duration": 22938,
          "tags": [
            {
              "key": "http.url",
              "vType": "STRING",
              "vStr": "http://127.0.0.1:15598/client_transactions"
            },
            {
              "key": "span.kind",
              "vType": "STRING",
              "vStr": "server"
            },
            {
              "key": "peer.port",
              "vType": "LONG",
              "vLong": 53931
            },
            {
              "key": "someBool",
              "vType": "BOOL",
              "vBool": true
            },
            {
              "key": "someDouble",
              "vType": "DOUBLE",
              "vDouble": 129.8
            },
            {
              "key": "peer.service",
              "vType": "STRING",
              "vStr": "rtapi"
            },
            {
              "key": "peer.ipv4",
              "vType": "LONG",
              "vLong": 3224716605
            }
          ],
          "logs": [
            {
              "timestamp": 1485467191639875,
              "fields": [
                {
                  "key": "key1",
                  "vType": "STRING",
                  "vStr": "value1"
                },
                {
                  "key": "key2",
                  "vType": "STRING",
                  "vStr": "value2"
                }
              ]
            },
            {
              "timestamp": 1485467191639875,
              "fields": [
                {
                  "key": "event",
                  "vType": "STRING",
                  "vStr": "nothing"
                }
              ]
            }
          ]
        }`

	const numSpans = 100
	reqJSON := `
    {
      "process": {
        "serviceName": "api",
        "tags": [
          {
            "key": "hostname",
            "vType": "STRING",
            "vStr": "api246-sjc1"
          },
          {
            "key": "ip",
            "vType": "STRING",
            "vStr": "10.53.69.61"
          },
          {
            "key": "jaeger.version",
            "vType": "STRING",
            "vStr": "Python-3.1.0"
          }
        ]
      },
      "spans": [`

	for i := 0; i < numSpans; i++ {
		reqJSON += spanJSON + ","
	}

	reqJSON = strings.TrimSuffix(reqJSON, ",") + `]}`

	var batch jThrift.Batch
	err := json.Unmarshal([]byte(reqJSON), &batch)
	if err != nil {
		panic("test json invalid")
	}
	batchBytes, err := thrift.NewTSerializer().Write(&batch)
	if err != nil {
		panic("couldn't serialize test batch to thrift")
	}

	reader := bytes.NewReader(batchBytes)
	reqBody := ioutil.NopCloser(reader)

	decoder := NewJaegerThriftTraceDecoderV1(nil,
		&fakeSink{
			handler: func(_ []*trace.Span) {},
		})

	req := http.Request{
		Body: reqBody,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := decoder.Read(context.Background(), &req)
		if err != nil {
			fmt.Println(i)
			b.Fatal(err)
		}
		b.StopTimer()
		reader.Reset(batchBytes)
		b.StartTimer()
	}
}
