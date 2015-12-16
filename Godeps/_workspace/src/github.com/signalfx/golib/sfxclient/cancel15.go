// +build go1.5

package sfxclient

import (
	"golang.org/x/net/context"
	"net/http"
)

func (h *HTTPDatapointSink) withCancel(ctx context.Context, req *http.Request) (err error) {
	req.Cancel = ctx.Done()
	return h.handleResponse(h.Client.Do(req))
}
