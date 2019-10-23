package common

import (
	"context"
	"fmt"

	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/web"
	"github.com/signalfx/sfxinternalgo/cmd/sbingest/idtool"
)

// OrgTokenKey is to get rid of every place where we have a package specific identical class
type OrgTokenKey struct {
	OrgID   int64
	TokenID int64
}

func (l *OrgTokenKey) String() string {
	if l.TokenID != 0 {
		return fmt.Sprintf("[OrgId: %s TokenID: %s]", idtool.GetIDAsString(l.OrgID), idtool.GetIDAsString(l.TokenID))
	}
	return fmt.Sprintf("[OrgId: %s]", idtool.GetIDAsString(l.OrgID))
}

// FirstNonNil returns what it says it does
func FirstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// DebugLogger provides a debug logger
type DebugLogger struct {
	CtxFlag *web.HeaderCtxFlag
	CtxLog  *log.CtxDimensions
	Logger  log.Logger
}

// GetDebugLogger returns a logger or not depending the context
func (t *DebugLogger) GetDebugLogger(ctx context.Context) *log.Context {
	if t.CtxFlag.HasFlag(ctx) {
		return t.CtxLog.With(ctx, t.Logger)
	}
	// Note: nil works for log.Context (it is disabled if nil)
	return nil
}
