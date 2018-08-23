package common

import (
	"context"
	"errors"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/web"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOrgTokenKey_String(t *testing.T) {
	to := &OrgTokenKey{
		OrgID:   1,
		TokenID: 0,
	}
	assert.Equal(t, "[OrgId: AAAAAAAAAAE]", to.String())
	to.TokenID = 1
	assert.Equal(t, "[OrgId: AAAAAAAAAAE TokenID: AAAAAAAAAAE]", to.String())
}

func TestFirstNonNil(t *testing.T) {
	err := errors.New("hi")
	e := FirstNonNil(nil, err)
	assert.Equal(t, err, e)
	assert.Nil(t, FirstNonNil([]error{}...))
}

func TestDebugLogger_GetDebugLogger(t *testing.T) {
	Convey("test debugLogger", t, func() {
		cflag := &web.HeaderCtxFlag{
			HeaderName: "X-Debug",
		}
		cflag.SetFlagStr("debugit")

		dl := DebugLogger{
			CtxFlag: cflag,
			CtxLog:  &log.CtxDimensions{},
			Logger:  log.DefaultLogger,
		}
		l := dl.GetDebugLogger(context.WithValue(context.Background(), cflag, cflag))
		So(l, ShouldNotBeNil)
		l2 := dl.GetDebugLogger(context.Background())
		So(l2, ShouldBeNil)
	})
}
