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
