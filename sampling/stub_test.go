package sampling

import (
	"context"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/trace"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test(t *testing.T) {
	obj := new(SmartSampleConfig)
	n, err := New(obj, log.Discard, nil)
	assert.Nil(t, n)
	assert.Nil(t, err)
	assert.Nil(t, n.AddSpans(context.Background(), []*trace.Span{}, nil))
	assert.Nil(t, n.Close())
	assert.True(t, len(n.Datapoints()) == 0)
}
