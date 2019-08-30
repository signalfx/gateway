package signalfx

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/signalfx/gateway/protocol/common"
	"github.com/signalfx/golib/datapoint/dpsink"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/sfxclient"
	"github.com/signalfx/golib/web"
)

// setupV1Paths will register paths for all content types
func setupV1Paths(ctx context.Context, r *gin.Engine, sink Sink, typeGetter MericTypeGetter, logger log.Logger, httpChain web.NextConstructor, counter *dpsink.Counter) sfxclient.Collector {

	protobufHandler, st := SetupChain(ctx, sink, "protobuf_v1", func(s Sink) ErrorReader {
		return &ProtobufDecoderV1{Sink: s, TypeGetter: typeGetter, Logger: logger}
	}, httpChain, logger, counter)

	jsonHandler, st2 := SetupChain(ctx, sink, "json_v1", func(s Sink) ErrorReader {
		return &JSONDecoderV1{Sink: s, TypeGetter: typeGetter, Logger: logger}
	}, httpChain, logger, counter)

	SetupV1Paths(r, protobufHandler, jsonHandler)
	return sfxclient.NewMultiCollector(st, st2)
}

// SetupV1Paths will register paths for all given handlers/content-type
func SetupV1Paths(r *gin.Engine, protobufDpHandler, jsonDpHandler http.Handler) {
	r.POST("/datapoint", func(gCtx *gin.Context) {
		if common.IsContentTypeXProtobuf(gCtx.ContentType()) {
			protobufDpHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else if common.IsContentTypeJSON(gCtx.ContentType()) {
			jsonDpHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else {
			web.InvalidContentType(gCtx.Writer, gCtx.Request)
		}
	})

	r.POST("/v1/datapoint", func(gCtx *gin.Context) {
		if common.IsContentTypeXProtobuf(gCtx.ContentType()) {
			protobufDpHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else if common.IsContentTypeJSON(gCtx.ContentType()) {
			jsonDpHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else {
			web.InvalidContentType(gCtx.Writer, gCtx.Request)
		}
	})
}

// setupV2Paths will register paths for all content types
func setupV2Paths(ctx context.Context, r *gin.Engine, sink Sink, logger log.Logger, debugContext *web.HeaderCtxFlag, httpChain web.NextConstructor, counter *dpsink.Counter) sfxclient.Collector {

	var additionalConstructors []web.Constructor
	if debugContext != nil {
		additionalConstructors = append(additionalConstructors, debugContext)
	}
	protobufHandler, st := SetupChain(ctx, sink, "protobuf_v2", func(s Sink) ErrorReader {
		return &ProtobufDecoderV2{Sink: s, Logger: logger}
	}, httpChain, logger, counter, additionalConstructors...)

	var additionalConstructors1 []web.Constructor
	if debugContext != nil {
		additionalConstructors1 = append(additionalConstructors1, debugContext)
	}
	protobufEventHandler, st1 := SetupChain(ctx, sink, "protobuf_event_v2", func(s Sink) ErrorReader {
		return &ProtobufEventDecoderV2{Sink: s, Logger: logger}
	}, httpChain, logger, counter, additionalConstructors1...)

	var additionalConstructors2 []web.Constructor
	if debugContext != nil {
		additionalConstructors2 = append(additionalConstructors2, debugContext)
	}
	var j2 *JSONDecoderV2
	jsonHandler, st2 := SetupChain(ctx, sink, "json_v2", func(s Sink) ErrorReader {
		j2 = &JSONDecoderV2{Sink: s, Logger: logger}
		return j2
	}, httpChain, logger, counter, additionalConstructors2...)

	var additionalConstructors3 []web.Constructor
	if debugContext != nil {
		additionalConstructors3 = append(additionalConstructors3, debugContext)
	}
	jsonEventHandler, st3 := SetupChain(ctx, sink, "json_event_v2", func(s Sink) ErrorReader {
		return &JSONEventDecoderV2{Sink: s, Logger: logger}
	}, httpChain, logger, counter, additionalConstructors3...)

	SetupV2Paths(r, protobufHandler, jsonHandler, protobufEventHandler, jsonEventHandler)

	return sfxclient.NewMultiCollector(st, st1, st2, j2, st3)
}

// SetupV2Paths will register paths for all given handlers/content-type
func SetupV2Paths(r *gin.Engine, protobufDpHandler, jsonDpHandler, protobufEventHandler, jsonEventHandler http.Handler) {
	r.POST("/v2/datapoint", func(gCtx *gin.Context) {
		if common.IsContentTypeXProtobuf(gCtx.ContentType()) {
			protobufDpHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else if common.IsContentTypeJSON(gCtx.ContentType()) {
			jsonDpHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else {
			web.InvalidContentType(gCtx.Writer, gCtx.Request)
		}
	})

	r.POST("/v2/event", func(gCtx *gin.Context) {
		if common.IsContentTypeXProtobuf(gCtx.ContentType()) {
			protobufEventHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else if common.IsContentTypeJSON(gCtx.ContentType()) {
			jsonEventHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else {
			web.InvalidContentType(gCtx.Writer, gCtx.Request)
		}
	})
}

// setupTraceV1Paths will register paths for all content types
func setupTraceV1Paths(ctx context.Context, r *gin.Engine, sink Sink, logger log.Logger, httpChain web.NextConstructor, counter *dpsink.Counter) sfxclient.Collector {

	thriftHandler, st := SetupChain(ctx, sink, JaegerV1, func(s Sink) ErrorReader {
		return NewJaegerThriftTraceDecoderV1(logger, sink)
	}, httpChain, logger, counter)

	jsonTraceHandler, st1 := SetupChain(ctx, sink, ZipkinV1, func(s Sink) ErrorReader {
		return &JSONTraceDecoderV1{Logger: logger, Sink: sink}
	}, httpChain, logger, counter)

	SetupTraceV1Paths(r, thriftHandler, jsonTraceHandler)
	return sfxclient.NewMultiCollector(st, st1)
}

// SetupTraceV1Paths will register paths for all handler/content-types
func SetupTraceV1Paths(r *gin.Engine, thriftHandler, jsonTraceHandler http.Handler) {
	r.POST(DefaultTracePathV1, func(gCtx *gin.Context) {
		if common.IsContentTypeThrift(gCtx.ContentType()) {
			thriftHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else if common.IsContentTypeJSON(gCtx.ContentType()) {
			jsonTraceHandler.ServeHTTP(gCtx.Writer, gCtx.Request)
		} else {
			web.InvalidContentType(gCtx.Writer, gCtx.Request)
		}
	})
}
