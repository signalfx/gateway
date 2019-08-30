package common

import (
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestInitDefaultGin(t *testing.T) {
	tests := []struct {
		name         string
		expectedMode string
		defaultMode  bool
		debugMode    string
	}{
		{
			name:         "gin default mode init",
			expectedMode: gin.DebugMode,
			defaultMode:  true,
			debugMode:    "",
		},
		{
			name:         "gin release mode init",
			expectedMode: gin.ReleaseMode,
			defaultMode:  false,
			debugMode:    gin.ReleaseMode,
		},
	}

	for _, tCase := range tests {
		tCase := tCase
		t.Run(tCase.name, func(t *testing.T) {
			_ = InitDefaultGin(tCase.defaultMode, tCase.debugMode)
			assert.Equal(t, tCase.expectedMode, gin.Mode())
		})
	}
}

func TestAllContentTypes(t *testing.T) {
	tests := []struct {
		name                string
		expectedContentType bool
		inputContentType    string
		jsonType            bool
		protobufType        bool
		thriftType          bool
	}{
		{
			name:                "json content type",
			expectedContentType: true,
			inputContentType:    JSONString,
			jsonType:            true,
		},
		{
			name:                "protobuf content type",
			expectedContentType: true,
			inputContentType:    XProtobuf,
			protobufType:        true,
		},
		{
			name:                "thrift content type",
			expectedContentType: true,
			inputContentType:    XThrift,
			thriftType:          true,
		},
	}
	for _, tCase := range tests {
		tCase := tCase
		t.Run(tCase.name, func(t *testing.T) {
			if tCase.jsonType {
				assert.True(t, tCase.expectedContentType, IsContentTypeJSON(tCase.inputContentType))
				assert.False(t, !tCase.expectedContentType, IsContentTypeXProtobuf(tCase.inputContentType))
				assert.False(t, !tCase.expectedContentType, IsContentTypeThrift(tCase.inputContentType))
			} else if tCase.protobufType {
				assert.False(t, !tCase.expectedContentType, IsContentTypeJSON(tCase.inputContentType))
				assert.True(t, tCase.expectedContentType, IsContentTypeXProtobuf(tCase.inputContentType))
				assert.False(t, !tCase.expectedContentType, IsContentTypeThrift(tCase.inputContentType))
			} else if tCase.thriftType {
				assert.False(t, !tCase.expectedContentType, IsContentTypeJSON(tCase.inputContentType))
				assert.False(t, !tCase.expectedContentType, IsContentTypeXProtobuf(tCase.inputContentType))
				assert.True(t, tCase.expectedContentType, IsContentTypeThrift(tCase.inputContentType))
			}
		})
	}
}
