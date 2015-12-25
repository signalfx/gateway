package sfxclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/golang/protobuf/proto"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/errors"
	"golang.org/x/net/context"
)

// ClientVersion is the version of this library and is embedded into the user agent
const ClientVersion = "1.0"

// IngestEndpointV2 is the v2 version of the signalfx ingest endpoint
const IngestEndpointV2 = "https://ingest.signalfx.com/v2/datapoint"

// DefaultUserAgent is the UserAgent string sent to signalfx
var DefaultUserAgent = fmt.Sprintf("golib-sfxclient/%s (gover %s)", ClientVersion, runtime.Version())

// DefaultTimeout is the default time to fail signalfx datapoint requests if they don't succeed
const DefaultTimeout = time.Second * 5

// HTTPDatapointSink will accept signalfx datapoints and forward them to signalfx via HTTP
type HTTPDatapointSink struct {
	AuthToken      string
	UserAgent      string
	Endpoint       string
	Client         http.Client
	protoMarshaler func(pb proto.Message) ([]byte, error)

	stats struct {
		readingBody int64
	}
}

var _ Sink = &HTTPDatapointSink{}

// TokenHeaderName is the header key for the auth token in the HTTP request
const TokenHeaderName = "X-Sf-Token"

// NewHTTPDatapointSink creates a default NewHTTPDatapointSink using package level constants
func NewHTTPDatapointSink() *HTTPDatapointSink {
	return &HTTPDatapointSink{
		UserAgent: DefaultUserAgent,
		Endpoint:  IngestEndpointV2,
		Client: http.Client{
			Timeout:   DefaultTimeout,
			Transport: http.DefaultTransport,
		},
		protoMarshaler: proto.Marshal,
	}
}

// AddDatapoints forwards the datapoints to signalfx
func (h *HTTPDatapointSink) AddDatapoints(ctx context.Context, points []*datapoint.Datapoint) (err error) {
	if len(points) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return errors.Annotate(ctx.Err(), "context already closed")
	}
	body, err := h.encodePostBodyProtobufV2(points)
	if err != nil {
		return errors.Annotate(err, "cannot encode datapoints into protocol buffers")
	}
	req, err := http.NewRequest("POST", h.Endpoint, bytes.NewBuffer(body))
	if err != nil {
		return errors.Annotatef(err, "cannot parse new HTTP request to %s", h.Endpoint)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set(TokenHeaderName, h.AuthToken)
	req.Header.Set("User-Agent", h.UserAgent)
	req.Header.Set("Connection", "Keep-Alive")

	return h.withCancel(ctx, req)
}

func (h *HTTPDatapointSink) handleResponse(resp *http.Response, respErr error) (err error) {
	if respErr != nil {
		return errors.Annotatef(respErr, "failed to send/recieve http request")
	}
	defer func() {
		closeErr := errors.Annotate(resp.Body.Close(), "failed to close response body")
		err = errors.NewMultiErr([]error{err, closeErr})
	}()
	atomic.AddInt64(&h.stats.readingBody, 1)
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Annotate(err, "cannot fully read response body")
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("invalid status code %d", resp.StatusCode)
	}
	var bodyStr string
	err = json.Unmarshal(respBody, &bodyStr)
	if err != nil {
		return errors.Annotatef(err, "cannot unmarshal response body %s", respBody)
	}
	if bodyStr != "OK" {
		return errors.Errorf("invalid response body %s", bodyStr)
	}
	return nil
}

var toMTMap = map[datapoint.MetricType]com_signalfx_metrics_protobuf.MetricType{
	datapoint.Counter:   com_signalfx_metrics_protobuf.MetricType_CUMULATIVE_COUNTER,
	datapoint.Count:     com_signalfx_metrics_protobuf.MetricType_COUNTER,
	datapoint.Enum:      com_signalfx_metrics_protobuf.MetricType_GAUGE,
	datapoint.Gauge:     com_signalfx_metrics_protobuf.MetricType_GAUGE,
	datapoint.Rate:      com_signalfx_metrics_protobuf.MetricType_GAUGE,
	datapoint.Timestamp: com_signalfx_metrics_protobuf.MetricType_GAUGE,
}

func toMT(mt datapoint.MetricType) com_signalfx_metrics_protobuf.MetricType {
	ret, exists := toMTMap[mt]
	if exists {
		return ret
	}
	panic(fmt.Sprintf("Unknown metric type: %d\n", mt))
}

func datumForPoint(pv datapoint.Value) *com_signalfx_metrics_protobuf.Datum {
	switch t := pv.(type) {
	case datapoint.IntValue:
		x := t.Int()
		return &com_signalfx_metrics_protobuf.Datum{IntValue: &x}
	case datapoint.FloatValue:
		x := t.Float()
		return &com_signalfx_metrics_protobuf.Datum{DoubleValue: &x}
	default:
		x := t.String()
		return &com_signalfx_metrics_protobuf.Datum{StrValue: &x}
	}
}

func mapToDimensions(dimensions map[string]string) []*com_signalfx_metrics_protobuf.Dimension {
	ret := make([]*com_signalfx_metrics_protobuf.Dimension, 0, len(dimensions))
	for k, v := range dimensions {
		if k == "" || v == "" {
			continue
		}
		// If someone knows a better way to do this, let me know.  I can't just take the &
		// of k and v because their content changes as the range iterates
		copyOfK := filterSignalfxKey(string([]byte(k)))
		copyOfV := (string([]byte(v)))
		ret = append(ret, (&com_signalfx_metrics_protobuf.Dimension{
			Key:   &copyOfK,
			Value: &copyOfV,
		}))
	}
	return ret
}

func filterSignalfxKey(str string) string {
	return strings.Map(runeFilterMap, str)
}

func runeFilterMap(r rune) rune {
	if unicode.IsDigit(r) || unicode.IsLetter(r) || r == '_' {
		return r
	}
	return '_'
}

func (h *HTTPDatapointSink) coreDatapointToProtobuf(point *datapoint.Datapoint) *com_signalfx_metrics_protobuf.DataPoint {
	m := point.Metric
	var ts int64
	if point.Timestamp.IsZero() {
		ts = 0
	} else {
		ts = point.Timestamp.UnixNano() / time.Millisecond.Nanoseconds()
	}
	mt := toMT(point.MetricType)
	v := &com_signalfx_metrics_protobuf.DataPoint{
		Metric:     &m,
		Timestamp:  &ts,
		Value:      datumForPoint(point.Value),
		MetricType: &mt,
		Dimensions: mapToDimensions(point.Dimensions),
	}
	return v
}

func (h *HTTPDatapointSink) encodePostBodyProtobufV2(datapoints []*datapoint.Datapoint) ([]byte, error) {
	dps := make([]*com_signalfx_metrics_protobuf.DataPoint, 0, len(datapoints))
	for _, dp := range datapoints {
		dps = append(dps, h.coreDatapointToProtobuf(dp))
	}
	msg := &com_signalfx_metrics_protobuf.DataPointUploadMessage{
		Datapoints: dps,
	}
	body, err := h.protoMarshaler(msg)
	if err != nil {
		return nil, errors.Annotate(err, "protobuf marshal failed")
	}
	return body, nil
}
