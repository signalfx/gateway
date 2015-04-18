package web

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestAddRequestTime(t *testing.T) {
	now := time.Now()
	f := HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		rt := RequestTime(ctx)
		assert.True(t, now.Before(rt))
		assert.True(t, time.Now().After(rt))
	})
	AddRequestTime(context.Background(), nil, nil, f)
}
