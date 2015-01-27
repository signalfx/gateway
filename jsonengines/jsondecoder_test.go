package jsonengines

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testCollectdBody = `[
    {
        "dsnames": [
            "shortterm",
            "midterm",
            "longterm"
        ],
        "dstypes": [
            "gauge",
            "gauge",
            "gauge"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "load",
        "plugin_instance": "",
        "time": 1415062577.4960001,
        "type": "load",
        "type_instance": "",
        "values": [
            0.37,
            0.60999999999999999,
            0.76000000000000001
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "gauge"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "memory",
        "plugin_instance": "",
        "time": 1415062577.4960001,
        "type": "memory",
        "type_instance": "used",
        "values": [
            1524310000.0
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "derive"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "df",
        "plugin_instance": "dev",
        "time": 1415062577.4949999,
        "type": "df_complex",
        "type_instance": "free",
        "values": [
            1962600000.0
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "derive"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "df",
        "plugin_instance": "dev",
        "time": 1415062577.4949999,
        "type": "",
        "type_instance": "free",
        "values": [
            1962600000.0
        ]
    },
    {
        "dsnames": [
            "value"
        ],
        "dstypes": [
            "derive"
        ],
        "host": "i-b13d1e5f",
        "interval": 10.0,
        "plugin": "ignored",
        "plugin_instance": "ignored2",
        "time": 1415062577.4949999,
        "type": "",
        "type_instance": "free",
        "values": [
            5.36202e+09
        ]
    }
]`

func BenchmarkJsonDecodingNative(b *testing.B) {
	benchmarkDecoder(b, &NativeMarshallJSONDecoder{})
}

func benchmarkDecoder(b *testing.B, decoder JSONDecodingEngine) {
	barray := []byte(testCollectdBody)
	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(barray)
		val, err := decoder.DecodeCollectdJSONWriteBody(buf)
		if len(val) != 5 || err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkJsonDecodingMegaJson(b *testing.B) {
	benchmarkDecoder(b, &MegaJSONJSONDecoder{})
}

func decoderTest(t *testing.T, decoder JSONDecodingEngine) {
	buf := bytes.NewBuffer([]byte(testCollectdBody))
	val, err := decoder.DecodeCollectdJSONWriteBody(buf)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(val), "expect 5")
}

func TestJsonDecodingMatches(t *testing.T) {
	decoderTest(t, &MegaJSONJSONDecoder{})
	decoderTest(t, &NativeMarshallJSONDecoder{})
}

func TestLoad(t *testing.T) {
	_, err := Load("")
	assert.Nil(t, err)
	_, err = Load("unknown")
	assert.NotNil(t, err)
}
