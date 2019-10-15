package httpclient

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/mailru/easyjson"
	"github.com/signalfx/gateway/hub/hubclient"
)

const (
	// endpoints
	registerV2   = "/v2/gatewayhub/register"
	unregisterV2 = "/v2/gatewayhub/unregister"
	//heartbeatV2  = "/v2/gatewayhub/heartbeat"
	//clusterV2    = "/v2/gatewayhub/cluster"

	// Content Types
	contentTypeHeader = "Content-Type"
	applicationJSON   = "application/json"

	// SFX Headers
	authenticationToken = "X-SF-TOKEN"
)

// Client is a client for interacting with the SignalFx Gateway Hub API
type Client interface {
	Register(cluster string, name string, version string, payload []byte, distributor bool) (reg hubclient.RegistrationResponse, etag string, err error)
	Unregister(lease string) error
}

// HTTPClient is an implementation of the Client interface
type HTTPClient struct {
	client    *http.Client
	authToken string
	baseURL   string
}

// request is a method for making http requests against the configured baseURL with the configured authToken
func (h *HTTPClient) request(urlStr string, method string, body []byte, headers map[string]string) (statusCode int, respHeaders http.Header, respBytes []byte, err error) {
	var req *http.Request
	var resp *http.Response

	// create the request
	req, err = http.NewRequest(method, h.baseURL+urlStr, bytes.NewReader(body))

	if err == nil {
		// add headers to the request
		for headerKey, headerVal := range headers {
			req.Header.Set(headerKey, headerVal)
		}

		// make the request
		resp, err = h.client.Do(req)
		if err == nil {
			// read the response body and close it so the backing transport can be re-used.
			// See: https://golang.org/pkg/net/http/#Client.Do
			// and https://forum.golangbridge.org/t/do-i-need-to-read-the-body-before-close-it/5594
			respBytes, err = ioutil.ReadAll(resp.Body)
			_ = resp.Body.Close()

			statusCode = resp.StatusCode

			// save the headers so calling functions can access them
			respHeaders = resp.Header
		}
	}

	return
}

// ErrServerNameConflict is a registration error
var ErrServerNameConflict = errors.New("server name already exists within cluster")

// Register makes a request to register with the gateway hub
func (h *HTTPClient) Register(cluster string, name string, version string, payload []byte, distributor bool) (reg hubclient.RegistrationResponse, etag string, err error) {
	var reqBody []byte

	// marshal registration
	reqBody, err = easyjson.Marshal(
		&hubclient.Registration{
			Cluster:     cluster,
			Name:        name,
			Version:     version,
			Payload:     payload,
			Distributor: distributor,
		})
	if err == nil {
		// get headers
		headers := map[string]string{
			contentTypeHeader:   applicationJSON,
			authenticationToken: h.authToken,
		}

		// make the request
		var respHeaders http.Header
		var statusCode int
		var respBody []byte
		statusCode, respHeaders, respBody, err = h.request(registerV2, http.MethodPost, reqBody, headers)
		if err == nil {
			etag = respHeaders.Get("etag")
			switch statusCode {
			case http.StatusOK:
				// unmarshal payload when the request is successful
				err = easyjson.Unmarshal(respBody, &reg)
				// TODO: decrypt each server payload if err is nil and encryption key is not empty
			case http.StatusConflict:
				// return a named error for status conflicts
				err = ErrServerNameConflict
			default:
				// The request didn't error out, but the server returned an unexpected status
				err = fmt.Errorf(`%s %s %d %s`, http.MethodPost, registerV2, statusCode, string(respBody))
			}
		}
	}

	return
}

// ErrLeaseDoesNotMatch means the lease does not match an existing cluster member
var ErrLeaseDoesNotMatch = errors.New("lease does not match an existing cluster member")

// ErrCannotRemoveInstance means the instance could not be removed durring unregistration
var ErrCannotRemoveInstance = errors.New("can not remove instance")

// Unregister unregisters an instance using its lease
func (h *HTTPClient) Unregister(lease string) (err error) {

	// get headers
	headers := map[string]string{
		contentTypeHeader:   applicationJSON,
		authenticationToken: h.authToken,
	}

	// make the request
	statusCode, _, respBody, err := h.request(path.Join(unregisterV2, lease), http.MethodPost, []byte{}, headers)

	if err == nil {
		switch statusCode {
		case http.StatusNoContent: // successful deregistration (no-op)
		case http.StatusNotModified:
			// return a named error when lease doesn't exist in a cluster
			err = ErrLeaseDoesNotMatch
		case http.StatusInternalServerError:
			// return a named error when hub can't remove the instance
			err = ErrCannotRemoveInstance
		default:
			// The request didn't error out, but the server returned an unexpected status
			err = fmt.Errorf(`%s %s %d %s`, http.MethodPost, unregisterV2, statusCode, string(respBody))
		}
	}

	return err
}

// NewClient returns a new http client for interacting with the SignalFx gateway hub
// TODO: add encryption key to client, this http client will be responsible for encyrpting and decrypting server payloads
func NewClient(hubAddress string, authToken string, timeout time.Duration) (Client, error) {
	var err error
	var cli = &HTTPClient{
		client: &http.Client{
			Timeout: timeout,
		},
		authToken: authToken,
	}

	// validate url
	var parsedURL *url.URL
	if parsedURL, err = url.Parse(hubAddress); err == nil {
		cli.baseURL = parsedURL.String()
	}

	// auth token
	if authToken == "" {
		err = errors.New("auth token can not be empty")
	}

	return cli, err
}
