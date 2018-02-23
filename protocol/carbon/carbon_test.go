package carbon

import (
	"errors"
	"fmt"
	"testing"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/metricproxy/protocol/carbon/metricdeconstructor"
	"github.com/stretchr/testify/assert"
)

type errorDeconstructor struct{}

func (parser *errorDeconstructor) Parse(originalMetric string) (string, datapoint.MetricType, map[string]string, error) {
	return "", datapoint.Gauge, nil, errors.New("error parsing")
}

var carbonTestCases = []struct {
	in            string
	deconstructor metricdeconstructor.MetricDeconstructor
	name          string
	val           datapoint.Value
	metricType    datapoint.MetricType
	timestamp     int64
	shouldErr     bool
}{
	{
		in:        "hello 3 3",
		name:      "hello",
		val:       datapoint.NewIntValue(3),
		timestamp: 3000000000,
	},
	{
		in:        "hello 3.3 3",
		name:      "hello",
		val:       datapoint.NewFloatValue(3.3),
		timestamp: 3000000000,
	},
	{
		in:        "hello 3 1519398226.544148",
		name:      "hello",
		val:       datapoint.NewIntValue(3),
		timestamp: 1519398226544000000,
	},
	{
		in:        "hello 3.3 1519398226.544148",
		name:      "hello",
		val:       datapoint.NewFloatValue(3.3),
		timestamp: 1519398226544000000,
	},
	{
		in:        "INVALIDLINE",
		shouldErr: true,
	},
	{
		in:        "hello 3 bob",
		shouldErr: true,
	},
	{
		in:        "hello bob 3",
		shouldErr: true,
	},
	{
		in:            "hello 3 3",
		deconstructor: &errorDeconstructor{},
		shouldErr:     true,
	},
}

func TestNewCarbonDatapointTable(t *testing.T) {
	for _, tt := range carbonTestCases {
		identityParser, _ := metricdeconstructor.Load("", "")
		if tt.deconstructor != nil {
			identityParser = tt.deconstructor
		}
		s, err := NewCarbonDatapoint(tt.in, identityParser)
		if err != nil || tt.shouldErr {
			if tt.shouldErr {
				assert.NotEqual(t, nil, err, fmt.Sprintf("input: %s should error", tt.in))
			} else {
				assert.Equal(t, nil, err, fmt.Sprintf("input: '%s' unexpectedly raised the error %v", tt.in, err))
			}
		}
		if err == nil {
			assert.Equal(t, tt.name, s.Metric, fmt.Sprintf("expected metric name '%s' and got '%s' with input %s", tt.name, s.Metric, tt.in))
			assert.Equal(t, tt.val, s.Value, fmt.Sprintf("expected value %v and got %v with input %s", tt.val, s.Value, tt.in))
			assert.Equal(t, tt.metricType, s.MetricType, fmt.Sprintf("expected metric type %v and got %v with input %s", tt.metricType, s.MetricType, tt.in))
			assert.Equal(t, tt.timestamp, s.Timestamp.UnixNano(), fmt.Sprintf("expected timestamp %v and got %v with input %s", tt.timestamp, s.MetricType, tt.in))
		}
	}
}

func TestConvertDatapointToCarbonLine(t *testing.T) {
	identityParser, _ := metricdeconstructor.Load("", "")
	dp, _ := NewCarbonDatapoint("hello 3.3 3", identityParser)
	f := dp.Value.(datapoint.FloatValue).Float()
	assert.Equal(t, 3.3, f, "Should get value back")

	carbonDp, _ := NativeCarbonLine(dp)
	assert.Equal(t, "hello 3.3 3", carbonDp, "Should get the carbon line back")
}
