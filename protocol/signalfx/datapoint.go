package signalfx

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mailru/easyjson"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/gateway/logkey"
	"github.com/signalfx/gateway/protocol/signalfx/format"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
)

// JSONDatapointV1 is an alias
type JSONDatapointV1 signalfxformat.JSONDatapointV1

// JSONDatapointV2 is an alias
type JSONDatapointV2 signalfxformat.JSONDatapointV2

// BodySendFormatV2 is an alias
type BodySendFormatV2 signalfxformat.BodySendFormatV2

// ProtobufDecoderV1 creates datapoints out of the V1 protobuf definition
type ProtobufDecoderV1 struct {
	Sink       dpsink.DSink
	TypeGetter MericTypeGetter
	Logger     log.Logger
}

var errInvalidProtobuf = errors.New("invalid protocol buffer sent")
var errProtobufTooLarge = errors.New("protobuf structure too large")
var errInvalidProtobufVarint = errors.New("invalid protobuf varint")

func (decoder *ProtobufDecoderV1) Read(ctx context.Context, req *http.Request) error {
	body := req.Body
	bufferedBody := bufio.NewReaderSize(body, 32768)
	for {
		_, err := bufferedBody.Peek(1)
		if err == io.EOF {
			return nil
		}
		buf, err := bufferedBody.Peek(4) // should be big enough for any varint

		if err != nil {
			decoder.Logger.Log(log.Err, err, "peek error")
			return err
		}
		num, bytesRead := proto.DecodeVarint(buf)
		if bytesRead == 0 {
			// Invalid varint?
			return errInvalidProtobufVarint
		}
		if num > 32768 {
			// Sanity check
			return errProtobufTooLarge
		}
		// Get the varint out
		buf = make([]byte, bytesRead)
		_, err = io.ReadFull(bufferedBody, buf)
		log.IfErr(decoder.Logger, err)

		// Get the structure out
		buf = make([]byte, num)
		_, err = io.ReadFull(bufferedBody, buf)
		if err != nil {
			return fmt.Errorf("unable to fully read protobuf message: %s", err)
		}
		var msg com_signalfx_metrics_protobuf.DataPoint
		err = proto.Unmarshal(buf, &msg)
		if err != nil {
			return err
		}
		if datapointProtobufIsInvalidForV1(&msg) {
			return errInvalidProtobuf
		}
		mt := decoder.TypeGetter.GetMetricTypeFromMap(msg.GetMetric())
		if dp, err := NewProtobufDataPointWithType(&msg, mt); err == nil {
			log.IfErr(decoder.Logger, decoder.Sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp}))
		}
	}
}

func datapointProtobufIsInvalidForV1(msg *com_signalfx_metrics_protobuf.DataPoint) bool {
	return msg.Metric == nil || msg.Value == nil
}

// JSONDecoderV1 creates datapoints out of the v1 JSON definition
type JSONDecoderV1 struct {
	TypeGetter MericTypeGetter
	Sink       dpsink.DSink
	Logger     log.Logger
}

func (decoder *JSONDecoderV1) Read(ctx context.Context, req *http.Request) error {
	dec := json.NewDecoder(req.Body)
	for {
		var d JSONDatapointV1
		if err := dec.Decode(&d); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			if d.Metric == "" {
				continue
			}
			mt := fromMT(decoder.TypeGetter.GetMetricTypeFromMap(d.Metric))
			dp := datapoint.New(d.Metric, map[string]string{"sf_source": d.Source}, datapoint.NewFloatValue(d.Value), mt, time.Now())
			log.IfErr(decoder.Logger, decoder.Sink.AddDatapoints(ctx, []*datapoint.Datapoint{dp}))
		}
	}
	return nil
}

// ProtobufDecoderV2 decodes protocol buffers in signalfx's v2 format and sends them to Sink
type ProtobufDecoderV2 struct {
	Sink   dpsink.Sink
	Logger log.Logger
}

var buffs = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (decoder *ProtobufDecoderV2) Read(ctx context.Context, req *http.Request) (err error) {
	jeff := buffs.Get().(*bytes.Buffer)
	defer buffs.Put(jeff)
	jeff.Reset()
	if err = readFromRequest(jeff, req, decoder.Logger); err != nil {
		return err
	}
	var msg com_signalfx_metrics_protobuf.DataPointUploadMessage
	if err = proto.Unmarshal(jeff.Bytes(), &msg); err != nil {
		return err
	}
	dps := make([]*datapoint.Datapoint, 0, len(msg.GetDatapoints()))
	for _, protoDb := range msg.GetDatapoints() {
		if dp, err1 := NewProtobufDataPointWithType(protoDb, com_signalfx_metrics_protobuf.MetricType_GAUGE); err1 == nil {
			dps = append(dps, dp)
		}
	}
	if len(dps) > 0 {
		err = decoder.Sink.AddDatapoints(ctx, dps)
	}
	return err
}

// JSONDecoderV2 decodes v2 json data for signalfx and sends it to Sink
type JSONDecoderV2 struct {
	Sink              dpsink.Sink
	Logger            log.Logger
	unknownMetricType int64
	invalidValue      int64
}

var errInvalidJSONFormat = errors.New("invalid JSON format; please see correct format at https://developers.signalfx.com/ingest_data_reference.html")

// Datapoints returns datapoints for json decoder v2
func (decoder *JSONDecoderV2) Datapoints() []*datapoint.Datapoint {
	return []*datapoint.Datapoint{
		sfxclient.Counter("dropped_points", map[string]string{"protocol": "sfx_json_v2", "reason": "unknown_metric_type"}, atomic.LoadInt64(&decoder.unknownMetricType)),
		sfxclient.Counter("dropped_points", map[string]string{"protocol": "sfx_json_v2", "reason": "invalid_value"}, atomic.LoadInt64(&decoder.invalidValue)),
	}
}

func (decoder *JSONDecoderV2) Read(ctx context.Context, req *http.Request) error {
	var d signalfxformat.JSONDatapointV2
	if err := easyjson.UnmarshalFromReader(req.Body, &d); err != nil {
		return errInvalidJSONFormat
	}
	dps := make([]*datapoint.Datapoint, 0, len(d))
	for metricType, datapoints := range d {
		if len(datapoints) > 0 {
			mt, ok := com_signalfx_metrics_protobuf.MetricType_value[strings.ToUpper(metricType)]
			if !ok {
				message := make([]interface{}, 0, 7)
				message = append(message, logkey.MetricType, metricType, logkey.Struct, datapoints[0])
				message = append(message, getTokenLogFormat(req)...)
				message = append(message, "Unknown metric type")
				decoder.Logger.Log(message...)
				atomic.AddInt64(&decoder.unknownMetricType, int64(len(datapoints)))
				continue
			}
			for _, jsonDatapoint := range datapoints {
				v, err := ValueToValue(jsonDatapoint.Value)
				if err != nil {
					message := make([]interface{}, 0, 7)
					message = append(message, logkey.Struct, jsonDatapoint, log.Err, err)
					message = append(message, getTokenLogFormat(req)...)
					message = append(message, "Unable to get value for datapoint")
					decoder.Logger.Log(message...)
					atomic.AddInt64(&decoder.invalidValue, 1)
					continue
				}
				dp := datapoint.New(jsonDatapoint.Metric, jsonDatapoint.Dimensions, v, fromMT(com_signalfx_metrics_protobuf.MetricType(mt)), fromTs(jsonDatapoint.Timestamp))
				dps = append(dps, dp)
			}
		}
	}
	if len(dps) == 0 {
		return nil
	}
	return decoder.Sink.AddDatapoints(ctx, dps)
}

func getTokenLogFormat(req *http.Request) (ret []interface{}) {
	h := sha1.New()
	head := req.Header.Get(TokenHeaderName)
	if _, err := io.WriteString(h, head); err != nil || head == "" {
		return ret
	}
	ret = append(ret, logkey.SHA1, base64.StdEncoding.EncodeToString(h.Sum(nil)))
	length := len(head) / 2
	return append(ret, logkey.Caller, head[:length])
}
