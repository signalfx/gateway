package web

import (
	"golang.org/x/net/context"
	"net/http"
	"sync"
)

// HeaderCtxFlag sets a debug value in the context if HeaderName is not empty, a flag string has
// been set to non empty, and the header HeaderName or query string HeaderName is equal to the set
// flag string
type HeaderCtxFlag struct {
	HeaderName string

	mu          sync.RWMutex
	expectedVal string
}

// CreateMiddleware creates a handler that calls next as the next in the chain
func (m *HeaderCtxFlag) CreateMiddleware(next ContextHandler) ContextHandler {
	return HandlerFunc(func(ctx context.Context, rw http.ResponseWriter, r *http.Request) {
		m.ServeHTTPC(ctx, rw, r, next)
	})
}

// SetFlagStr enabled flag setting for HeaderName if it's equal to headerVal
func (m *HeaderCtxFlag) SetFlagStr(headerVal string) {
	m.mu.Lock()
	m.expectedVal = headerVal
	m.mu.Unlock()
}

// WithFlag returns a new Context that has the flag for this context set
func (m *HeaderCtxFlag) WithFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, m, struct{}{})
}

// HasFlag returns true if WithFlag has been set for this context
func (m *HeaderCtxFlag) HasFlag(ctx context.Context) bool {
	return ctx.Value(m) != nil
}

// FlagStr returns the currently set flag header
func (m *HeaderCtxFlag) FlagStr() string {
	m.mu.RLock()
	ret := m.expectedVal
	m.mu.RUnlock()
	return ret
}

// ServeHTTPC calls next with a context flagged if the headers match.  Note it checks both headers and query parameters.
func (m *HeaderCtxFlag) ServeHTTPC(ctx context.Context, rw http.ResponseWriter, r *http.Request, next ContextHandler) {
	debugStr := m.FlagStr()
	if debugStr != "" && m.HeaderName != "" {
		if r.Header.Get(m.HeaderName) == debugStr {
			ctx = m.WithFlag(ctx)
		} else if r.URL.Query().Get(m.HeaderName) == debugStr {
			ctx = m.WithFlag(ctx)
		}
	}
	next.ServeHTTPC(ctx, rw, r)
}
