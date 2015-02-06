package collectd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkJsonDecodingNative(b *testing.B) {
	benchmarkDecoder(b, &NativeMarshallJSONDecoder{})
}

func benchmarkDecoder(b *testing.B, decoder JSONEngine) {
	barray := []byte(testCollectdBody)
	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(barray)
		val, err := decoder.DecodeJSONWriteBody(buf)
		assert.Equal(b, 3, len(val))
		assert.NoError(b, err)
	}
}

func decoderTest(t *testing.T, decoder JSONEngine) {
	buf := bytes.NewBuffer([]byte(testCollectdBody))
	val, err := decoder.DecodeJSONWriteBody(buf)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(val))
}

func TestJsonDecodingMatches(t *testing.T) {
	decoderTest(t, &NativeMarshallJSONDecoder{})
}

func TestLoad(t *testing.T) {
	_, err := LoadEngine("")
	assert.Nil(t, err)
	_, err = LoadEngine("unknown")
	assert.NotNil(t, err)
}
