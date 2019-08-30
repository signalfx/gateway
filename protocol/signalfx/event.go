package signalfx

import (
	"bytes"
	"context"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/mailru/easyjson"
	"github.com/signalfx/com_signalfx_metrics_protobuf"
	"github.com/signalfx/gateway/protocol/signalfx/format"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/event"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
)

// ProtobufEventDecoderV2 decodes protocol buffers in signalfx's v2 format and sends them to Sink
type ProtobufEventDecoderV2 struct {
	Sink   dpsink.ESink
	Logger log.Logger
}

func (decoder *ProtobufEventDecoderV2) Read(ctx context.Context, req *http.Request) (err error) {
	jeff := buffs.Get().(*bytes.Buffer)
	defer buffs.Put(jeff)
	jeff.Reset()
	if err = readFromRequest(jeff, req, decoder.Logger); err != nil {
		return err
	}
	var msg com_signalfx_metrics_protobuf.EventUploadMessage
	if err = proto.Unmarshal(jeff.Bytes(), &msg); err != nil {
		return err
	}
	evts := make([]*event.Event, 0, len(msg.GetEvents()))
	for _, protoDb := range msg.GetEvents() {
		if e, err1 := NewProtobufEvent(protoDb); err1 == nil {
			evts = append(evts, e)
		}
	}
	if len(evts) > 0 {
		err = decoder.Sink.AddEvents(ctx, evts)
	}
	return err
}

// JSONEventDecoderV2 decodes v2 json data for signalfx events and sends it to Sink
type JSONEventDecoderV2 struct {
	Sink   dpsink.ESink
	Logger log.Logger
}

func (decoder *JSONEventDecoderV2) Read(ctx context.Context, req *http.Request) error {
	var e signalfxformat.JSONEventV2
	if err := easyjson.UnmarshalFromReader(req.Body, &e); err != nil {
		return err
	}
	evts := make([]*event.Event, 0, len(e))
	for _, jsonEvent := range e {
		if jsonEvent.Category == nil {
			jsonEvent.Category = pointer.String("USER_DEFINED")
		}
		if jsonEvent.Timestamp == nil {
			jsonEvent.Timestamp = pointer.Int64(0)
		}
		cat := event.USERDEFINED
		if pbcat, ok := com_signalfx_metrics_protobuf.EventCategory_value[*jsonEvent.Category]; ok {
			cat = event.Category(pbcat)
		}
		evt := event.NewWithProperties(jsonEvent.EventType, cat, jsonEvent.Dimensions, jsonEvent.Properties, fromTs(*jsonEvent.Timestamp))
		evts = append(evts, evt)
	}
	return decoder.Sink.AddEvents(ctx, evts)
}
