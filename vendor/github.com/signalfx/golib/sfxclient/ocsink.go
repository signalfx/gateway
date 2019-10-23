package sfxclient

import (
	"context"
	"errors"
	"fmt"
	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/open-telemetry/opentelemetry-collector/config/configmodels"
	"github.com/open-telemetry/opentelemetry-collector/consumer/consumerdata"
	"github.com/open-telemetry/opentelemetry-collector/exporter"
	"github.com/open-telemetry/opentelemetry-collector/exporter/opencensusexporter"
	"github.com/open-telemetry/opentelemetry-collector/translator/trace"
	"github.com/signalfx/golib/trace"
	"github.com/signalfx/ondiskencoding"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync/atomic"
)

// OCSink is an opencensus compatible sink for spans
type OCSink struct {
	exporter exporter.TraceExporter
	err      atomic.Value
	retries  int
}

var (
	errNilZipkinSpan = errors.New("non-nil Zipkin span expected")
	errZeroID        = errors.New("id is zero")
)

var _ trace.Sink = &OCSink{}
var _ exporter.Host = &OCSink{}

func zTraceIDToOCProtoID(traceID string) ([]byte, error) {
	id, err := encoding.GetID(traceID)
	if err != nil {
		return nil, err
	}
	if id.Zero() {
		return nil, errZeroID
	}
	return tracetranslator.UInt64ToByteTraceID(id.High(), id.Low()), nil
}

func zSpanIDToOCProtoID(spanId string) ([]byte, error) {
	id, err := encoding.GetID(spanId)
	if err != nil {
		return nil, err
	}
	if id.Zero() {
		return nil, errZeroID
	}
	return tracetranslator.UInt64ToByteSpanID(id.Low()), nil
}

func zipkinSpanToTraceSpan(zs *trace.Span) (*tracepb.Span, *commonpb.Node, error) {
	if zs == nil {
		return nil, nil, errNilZipkinSpan
	}

	node := nodeFromZipkinEndpoints(zs)
	traceID, err := zTraceIDToOCProtoID(zs.TraceID)
	if err != nil {
		return nil, node, fmt.Errorf("traceID: %v", err)
	}
	spanID, err := zSpanIDToOCProtoID(zs.ID)
	if err != nil {
		return nil, node, fmt.Errorf("spanID: %v", err)
	}
	var parentSpanID []byte
	if zs.ParentID != nil {
		parentSpanID, err = zSpanIDToOCProtoID(*zs.ParentID)
		if err != nil {
			return nil, node, fmt.Errorf("parentSpanID: %v", err)
		}
	}

	pbs := &tracepb.Span{
		TraceId:      traceID,
		SpanId:       spanID,
		ParentSpanId: parentSpanID,
		StartTime:    timeToTimestamp(zs.Timestamp),
		EndTime:      timeToTimestamp(zs.Timestamp, zs.Duration),
		Status:       extractProtoStatus(zs),
		Attributes:   zipkinTagsToTraceAttributes(zs.Tags),
		TimeEvents:   zipkinAnnotationsToProtoTimeEvents(zs.Annotations),
	}
	if zs.Name != nil {
		pbs.Name = &tracepb.TruncatableString{Value: *zs.Name}
	}
	if zs.Kind != nil {
		pbs.Kind = zipkinSpanKindToProtoSpanKind(*zs.Kind)
	}

	return pbs, node, nil
}

// timeToTimestamp converts an *int64 in microseconds to a *timestamp.Timestamp
func timeToTimestamp(ts ...*int64) *timestamp.Timestamp {
	var nanoTime int64
	for _, t := range ts {
		if t != nil && *t > 0 {
			nanoTime += *t * 1000
		}
	}
	if nanoTime == 0 {
		return nil
	}
	return &timestamp.Timestamp{
		Seconds: nanoTime / 1e9,
		Nanos:   int32(nanoTime % 1e9),
	}
}

func nodeFromZipkinEndpoints(zs *trace.Span) *commonpb.Node {
	if zs.LocalEndpoint == nil && zs.RemoteEndpoint == nil {
		return nil
	}

	node := new(commonpb.Node)

	// Retrieve and make use of the local endpoint
	if lep := zs.LocalEndpoint; lep != nil && lep.ServiceName != nil {
		node.ServiceInfo = &commonpb.ServiceInfo{
			Name: *lep.ServiceName,
		}
		node.Attributes = zipkinEndpointIntoAttributes(lep, node.Attributes, isLocalEndpoint)
	}

	// Retrieve and make use of the remote endpoint
	if rep := zs.RemoteEndpoint; rep != nil {
		// For remoteEndpoint, our goal is to prefix its fields with "zipkin.remoteEndpoint."
		// For example becoming:
		// {
		//      "zipkin.remoteEndpoint.ipv4": "192.168.99.101",
		//      "zipkin.remoteEndpoint.port": "9000"
		//      "zipkin.remoteEndpoint.serviceName": "backend",
		// }
		node.Attributes = zipkinEndpointIntoAttributes(rep, node.Attributes, isRemoteEndpoint)
	}
	return node
}

type zipkinDirection bool

const (
	isLocalEndpoint  zipkinDirection = true
	isRemoteEndpoint zipkinDirection = false
)

// nolint: gocyclo
func zipkinEndpointIntoAttributes(ep *trace.Endpoint, into map[string]string, endpointType zipkinDirection) map[string]string {
	if into == nil {
		into = make(map[string]string)
	}

	var ipv4Key, ipv6Key, portKey, serviceNameKey string
	if endpointType == isLocalEndpoint {
		ipv4Key, ipv6Key = "ipv4", "ipv6"
		portKey, serviceNameKey = "port", "serviceName"
	} else {
		ipv4Key, ipv6Key = "zipkin.remoteEndpoint.ipv4", "zipkin.remoteEndpoint.ipv6"
		portKey, serviceNameKey = "zipkin.remoteEndpoint.port", "zipkin.remoteEndpoint.serviceName"
	}
	if ep.Ipv4 != nil && *ep.Ipv4 != "" {
		into[ipv4Key] = *ep.Ipv4
	}
	if ep.Ipv6 != nil && *ep.Ipv6 != "" {
		into[ipv6Key] = *ep.Ipv6
	}
	if ep.Port != nil && *ep.Port > 0 {
		into[portKey] = strconv.Itoa(int(*ep.Port))
	}
	if serviceName := ep.ServiceName; serviceName != nil && *serviceName != "" {
		into[serviceNameKey] = *serviceName
	}
	return into
}

const statusCodeUnknown = 2

func extractProtoStatus(zs *trace.Span) *tracepb.Status {
	// The status is stored with the "error" key
	// See https://github.com/census-instrumentation/opencensus-go/blob/1eb9a13c7dd02141e065a665f6bf5c99a090a16a/exporter/zipkin/zipkin.go#L160-L165
	if zs == nil || len(zs.Tags) == 0 {
		return nil
	}
	canonicalCodeStr := zs.Tags["error"]
	message := zs.Tags["opencensus.status_description"]
	if message == "" && canonicalCodeStr == "" {
		return nil
	}
	code, set := canonicalCodesMap[canonicalCodeStr]
	if !set {
		// If not status code was set, then we should use UNKNOWN
		code = statusCodeUnknown
	}
	return &tracepb.Status{
		Message: message,
		Code:    code,
	}
}

var canonicalCodesMap = map[string]int32{
	// https://github.com/googleapis/googleapis/blob/bee79fbe03254a35db125dc6d2f1e9b752b390fe/google/rpc/code.proto#L33-L186
	"OK":                  0,
	"CANCELLED":           1,
	"UNKNOWN":             2,
	"INVALID_ARGUMENT":    3,
	"DEADLINE_EXCEEDED":   4,
	"NOT_FOUND":           5,
	"ALREADY_EXISTS":      6,
	"PERMISSION_DENIED":   7,
	"RESOURCE_EXHAUSTED":  8,
	"FAILED_PRECONDITION": 9,
	"ABORTED":             10,
	"OUT_OF_RANGE":        11,
	"UNIMPLEMENTED":       12,
	"INTERNAL":            13,
	"UNAVAILABLE":         14,
	"DATA_LOSS":           15,
	"UNAUTHENTICATED":     16,
}

func zipkinSpanKindToProtoSpanKind(skind string) tracepb.Span_SpanKind {
	switch strings.ToUpper(skind) {
	case "CLIENT":
		return tracepb.Span_CLIENT
	case "SERVER":
		return tracepb.Span_SERVER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

func zipkinAnnotationsToProtoTimeEvents(zas []*trace.Annotation) *tracepb.Span_TimeEvents {
	if len(zas) == 0 {
		return nil
	}
	tevs := make([]*tracepb.Span_TimeEvent, 0, len(zas))
	for _, za := range zas {
		if tev := zipkinAnnotationToProtoAnnotation(za); tev != nil {
			tevs = append(tevs, tev)
		}
	}
	if len(tevs) == 0 {
		return nil
	}
	return &tracepb.Span_TimeEvents{
		TimeEvent: tevs,
	}
}

func zipkinAnnotationToProtoAnnotation(zas *trace.Annotation) *tracepb.Span_TimeEvent {
	if zas == nil || zas.Value == nil {
		return nil
	}
	return &tracepb.Span_TimeEvent{
		Time: timeToTimestamp(zas.Timestamp),
		Value: &tracepb.Span_TimeEvent_Annotation_{
			Annotation: &tracepb.Span_TimeEvent_Annotation{
				Description: &tracepb.TruncatableString{Value: *zas.Value},
			},
		},
	}
}

func zipkinTagsToTraceAttributes(tags map[string]string) *tracepb.Span_Attributes {
	if len(tags) == 0 {
		return nil
	}

	amap := make(map[string]*tracepb.AttributeValue, len(tags))
	for key, value := range tags {
		// We did a translation from "boolean" to "string"
		// in OpenCensus-Go's Zipkin exporter as per
		// https://github.com/census-instrumentation/opencensus-go/blob/1eb9a13c7dd02141e065a665f6bf5c99a090a16a/exporter/zipkin/zipkin.go#L138-L155
		switch value {
		case "true", "false":
			amap[key] = &tracepb.AttributeValue{
				Value: &tracepb.AttributeValue_BoolValue{BoolValue: value == "true"},
			}
		default:
			amap[key] = &tracepb.AttributeValue{
				Value: &tracepb.AttributeValue_StringValue{
					StringValue: &tracepb.TruncatableString{Value: value},
				},
			}
		}
	}
	return &tracepb.Span_Attributes{AttributeMap: amap}
}

// v2ToTraceSpans parses Zipkin v2 JSON or Protobuf traces and converts them to OpenCensus Proto spans.
func (oc *OCSink) internalToTraceSpans(zipkinSpans []*trace.Span) (reqs []consumerdata.TraceData, err error) {
	if len(zipkinSpans) > 0 {

		// *commonpb.Node instances have unique addresses hence
		// for grouping within a map, we'll use the .String() value
		byNodeGrouping := make(map[string][]*tracepb.Span)
		uniqueNodes := make([]*commonpb.Node, 0, len(zipkinSpans))
		// Now translate them into tracepb.Span
		for _, zspan := range zipkinSpans {
			span, node, errr := zipkinSpanToTraceSpan(zspan)
			if errr != nil {
				err = errr
			}
			if err == nil && span != nil {
				key := node.String()
				if _, alreadyAdded := byNodeGrouping[key]; !alreadyAdded {
					uniqueNodes = append(uniqueNodes, node)
				}
				byNodeGrouping[key] = append(byNodeGrouping[key], span)
			}
		}

		for _, node := range uniqueNodes {
			key := node.String()
			spans := byNodeGrouping[key]
			if len(spans) == 0 {
				// Should never happen but nonetheless be cautious
				// not to send blank spans.
				continue
			}
			reqs = append(reqs, consumerdata.TraceData{
				Node:  node,
				Spans: spans,
			})
			delete(byNodeGrouping, key)
		}
	}

	return reqs, err
}

// AddSpans forwards the traces to SignalFx.
func (oc *OCSink) AddSpans(ctx context.Context, traces []*trace.Span) (err error) {
	if l := oc.err.Load(); l != nil {
		return l.(error)
	}
	var tds []consumerdata.TraceData
	tds, err = oc.internalToTraceSpans(traces)
	if err == nil {
		for _, td := range tds {
			var terr error
			for i := 0; i < oc.retries; i++ {
				if terr = oc.exporter.ConsumeTraceData(ctx, td); terr == nil {
					break
				}
			}
			if terr != nil {
				err = terr
			}
		}
	}
	return err
}

// Close shuts down the exporter
func (oc *OCSink) Close() error {
	return oc.exporter.Shutdown()
}

// ReportFatalError adheres to the exporter.Host interface
func (oc *OCSink) ReportFatalError(err error) {
	oc.err.Store(err)
}

// NewOCSink gets you a new opencensus sink
func NewOCSink(c configmodels.Exporter, retries int) (*OCSink, error) {
	var ocs *OCSink
	log, err := zap.NewDevelopment()
	if err == nil {

		te, err := (&opencensusexporter.Factory{}).CreateTraceExporter(log, c)
		if err == nil {
			ocs = &OCSink{
				exporter: te,
				retries:  retries,
			}
			err = te.Start(ocs)
		}
	}
	return ocs, err
}
