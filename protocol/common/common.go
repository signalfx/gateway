package common

import (
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	// XProtobuf content type
	XProtobuf = "application/x-protobuf"
	// JSONString content type
	JSONString = "application/json"
	// JSONCharUtf8String content type
	JSONCharUtf8String = "application/json; charset=UTF-8"
	// XThrift content type
	XThrift = "application/x-thrift"
	// VndApacheThrift content type
	VndApacheThrift = "application/vnd.apache.thrift.binary"
	// Invalid content type
	Invalid = "invalid"
)

// InitDefaultGin use to setup default gin engine for all protocols
func InitDefaultGin(defaultMode bool, debugMode string) (r *gin.Engine) {
	if defaultMode {
		return gin.Default()
	}
	gin.SetMode(debugMode)
	r = gin.New()
	r.Use(gin.Recovery())
	return r
}

// IsContentTypeXProtobuf returns if content is of x-protobuf or not
func IsContentTypeXProtobuf(contentType string) bool {
	return strings.Compare(contentType, XProtobuf) == 0
}

// IsContentTypeJSON returns if content is of x-protobuf or not
func IsContentTypeJSON(contentType string) bool {
	return strings.Compare(contentType, JSONString) == 0 || strings.Compare(contentType, JSONCharUtf8String) == 0
}

// IsContentTypeThrift returns if content is of x-protobuf or not
func IsContentTypeThrift(contentType string) bool {
	return strings.Compare(contentType, XThrift) == 0 || strings.Compare(contentType, VndApacheThrift) == 0
}
