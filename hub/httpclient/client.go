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
	heartbeatV2  = "/v2/gatewayhub/heartbeat"
	clusterV2    = "/v2/gatewayhub/cluster"
	clustersV2   = "/v2/gatewayhub/clusters"
	configV2     = "/v2/gatewayhub/config"

	// Content Types
	contentTypeHeader = "Content-Type"
	applicationJSON   = "application/json"

	// Connection Header
	connectionHeader = "Connection"
	keepAlive        = "keep-alive"

	// UserAgent Header
	userAgentHeader = "UserAgent"

	// SFX Headers
	authenticationToken = "X-SF-TOKEN"

	eTag = "ETag"
)

// Client is a client for interacting with the SignalFx Gateway Hub API
type Client interface {
	Heartbeat(lease string, etag string) (state *hubclient.HeartbeatResponse, newEtag string, err error)
	Register(cluster string, name string, version string, payload []byte, distributor bool) (reg *hubclient.RegistrationResponse, etag string, err error)
	Unregister(lease string) error
	Cluster(serverName string) (*hubclient.Cluster, error)
	Clusters() (*hubclient.ListClustersResponse, error)
	Config(clusterName string) (*hubclient.Config, error)
}

// handleCommonHTTPErrorCodes handles common http status codes for errors and returns a formatted error
func handleCommonHTTPErrorCodes(statusCode int, method string, endpoint string, body []byte) (err error) {
	switch statusCode {
	case http.StatusUnauthorized:
		err = ErrUnauthorized
	default:
		// return an error for the uncaught error
		err = fmt.Errorf(`%s %s %d %s`, method, endpoint, statusCode, string(body))
	}
	return err
}

// HTTPClient is an implementation of the Client interface
type HTTPClient struct {
	client    *http.Client
	authToken string
	baseURL   string
	userAgent string
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

// ErrUnauthorized is an unauthorized error
var ErrUnauthorized = errors.New("authorization failed (please verify auth token is correct)")

// Register makes a request to register with the gateway hub
func (h *HTTPClient) Register(cluster string, name string, version string, payload []byte, distributor bool) (*hubclient.RegistrationResponse, string, error) {
	var reg *hubclient.RegistrationResponse
	var etag string

	// marshal registration
	reqBody, err := easyjson.Marshal(
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
			userAgentHeader:     h.userAgent,
			connectionHeader:    keepAlive,
			contentTypeHeader:   applicationJSON,
			authenticationToken: h.authToken,
		}

		// make the request
		var respHeaders http.Header
		var statusCode int
		var respBody []byte
		statusCode, respHeaders, respBody, err = h.request(registerV2, http.MethodPost, reqBody, headers)
		if err == nil {
			switch statusCode {
			case http.StatusOK:
				etag = respHeaders.Get(eTag)
				reg = &hubclient.RegistrationResponse{}
				// unmarshal payload when the request is successful
				err = easyjson.Unmarshal(respBody, reg)
				// TODO: decrypt each server payload if err is nil and encryption key is not empty
			case http.StatusConflict:
				// return a named error for status conflicts
				err = ErrServerNameConflict
			default:
				// The request didn't error out, but the server returned an unexpected status
				err = handleCommonHTTPErrorCodes(statusCode, http.MethodPost, registerV2, respBody)
			}
		}
	}

	return reg, etag, err
}

// ErrLeaseDoesNotMatch means the lease does not match an existing cluster member
var ErrLeaseDoesNotMatch = errors.New("lease does not match an existing cluster member")

// ErrCannotRemoveInstance means the instance could not be removed durring unregistration
var ErrCannotRemoveInstance = errors.New("can not remove instance")

// Unregister unregisters an instance using its lease
func (h *HTTPClient) Unregister(lease string) (err error) {

	// get headers
	headers := map[string]string{
		userAgentHeader:     h.userAgent,
		connectionHeader:    keepAlive,
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
			err = handleCommonHTTPErrorCodes(statusCode, http.MethodPost, unregisterV2, respBody)
		}
	}

	return err
}

// ErrExpiredLease is returned when the hub says the client's lease has expired
var ErrExpiredLease = errors.New("lease cannot be found")

// Heartbeat makes a heart beat request to the hub with a lease and etag
func (h *HTTPClient) Heartbeat(lease string, etag string) (*hubclient.HeartbeatResponse, string, error) {
	var state = &hubclient.HeartbeatResponse{}
	var newEtag string

	headers := map[string]string{
		userAgentHeader:     h.userAgent,
		connectionHeader:    keepAlive,
		contentTypeHeader:   applicationJSON,
		authenticationToken: h.authToken,
		eTag:                etag,
	}

	statusCode, respHeaders, respBody, err := h.request(path.Join(heartbeatV2, lease), http.MethodPost, []byte{}, headers)

	if err == nil {
		switch statusCode {
		case http.StatusNoContent: // successful heart beat but no changes to state
		case http.StatusOK: // means that the heart beat was successful but the state changed
			newEtag = respHeaders.Get(eTag)
			err = easyjson.Unmarshal(respBody, state)
		case http.StatusGone:
			err = ErrExpiredLease // means that the lease couldn't be found (this means we must reregister)
		default:
			// The request didn't error out, but the server returned an unexpected status
			err = handleCommonHTTPErrorCodes(statusCode, http.MethodPost, heartbeatV2, respBody)
		}
	}

	return state, newEtag, err
}

// ErrClusterNotFound is returned when a cluster can't be found in the hub
var ErrClusterNotFound = errors.New("could not find cluster")

// Clusters returns all of the clusters under the account
func (h *HTTPClient) Clusters() (*hubclient.ListClustersResponse, error) {
	var resp *hubclient.ListClustersResponse

	// get headers
	headers := map[string]string{
		userAgentHeader:     h.userAgent,
		connectionHeader:    keepAlive,
		contentTypeHeader:   applicationJSON,
		authenticationToken: h.authToken,
	}

	// make the request
	statusCode, _, respBody, err := h.request(clustersV2, http.MethodGet, []byte{}, headers)

	if err == nil {
		switch statusCode {
		case http.StatusOK:
			resp = &hubclient.ListClustersResponse{}
			err = easyjson.Unmarshal(respBody, resp)
		default:
			// The request didn't error out, but the server returned an unexpected status
			err = handleCommonHTTPErrorCodes(statusCode, http.MethodGet, clustersV2, respBody)
		}
	}

	return resp, err
}

// Cluster returns information about a cluster
func (h *HTTPClient) Cluster(clusterName string) (*hubclient.Cluster, error) {
	var resp *hubclient.Cluster

	// get headers
	headers := map[string]string{
		userAgentHeader:     h.userAgent,
		connectionHeader:    keepAlive,
		contentTypeHeader:   applicationJSON,
		authenticationToken: h.authToken,
	}

	// make the request
	statusCode, _, respBody, err := h.request(path.Join(clusterV2, clusterName), http.MethodGet, []byte{}, headers)

	if err == nil {
		switch statusCode {
		case http.StatusOK:
			resp = &hubclient.Cluster{Name: clusterName}
			err = easyjson.Unmarshal(respBody, resp)
		case http.StatusNotFound:
			err = ErrClusterNotFound
		default:
			// The request didn't error out, but the server returned an unexpected status
			err = handleCommonHTTPErrorCodes(statusCode, http.MethodGet, clusterV2, respBody)
		}
	}

	return resp, err
}

// Config returns configuration information
func (h *HTTPClient) Config(clusterName string) (*hubclient.Config, error) {
	var resp *hubclient.Config

	// get headers
	headers := map[string]string{
		userAgentHeader:     h.userAgent,
		connectionHeader:    keepAlive,
		contentTypeHeader:   applicationJSON,
		authenticationToken: h.authToken,
	}

	// make the request
	statusCode, _, respBody, err := h.request(path.Join(configV2, clusterName), http.MethodGet, []byte{}, headers)

	if err == nil {
		switch statusCode {
		case http.StatusOK:
			resp = &hubclient.Config{}
			err = easyjson.Unmarshal(respBody, resp)
		case http.StatusNotFound:
			err = ErrClusterNotFound
		default:
			// The request didn't error out, but the server returned an unexpected status
			err = handleCommonHTTPErrorCodes(statusCode, http.MethodGet, configV2, respBody)
		}
	}

	return resp, err
}

func (h *HTTPClient) validAccessToken() error {
	headers := map[string]string{
		userAgentHeader:     h.userAgent,
		connectionHeader:    keepAlive,
		contentTypeHeader:   applicationJSON,
		authenticationToken: h.authToken,
	}

	// make the request
	statusCode, _, respBody, err := h.request(clustersV2, http.MethodGet, []byte{}, headers)

	if err == nil {
		switch statusCode {
		case http.StatusOK:
		default:
			// The request didn't error out, but the server returned an unexpected status
			err = handleCommonHTTPErrorCodes(statusCode, http.MethodGet, clustersV2, respBody)
		}
	}
	return err
}

// NewClient returns a new http client for interacting with the SignalFx gateway hub
// TODO: add encryption key to client, this http client will be responsible for encyrpting and decrypting server payloads
func NewClient(hubAddress string, authToken string, timeout time.Duration, userAgent string) (Client, error) {
	var cli = &HTTPClient{
		client: &http.Client{
			Timeout: timeout,
		},
		authToken: authToken,
		userAgent: userAgent,
	}

	// validate url
	parsedURL, err := url.ParseRequestURI(hubAddress)
	if err != nil {
		return cli, err
	}
	cli.baseURL = parsedURL.String()

	// validate that the auth token isn't empty
	if authToken == "" {
		return cli, errors.New("auth token can not be empty")
	}

	// validate auth token with a request.  If it is unauthorized we'll throw an error.
	// Otherwise we've vetted about as much as we can.  Maybe the service is totally down?
	// we don't want to prevent the client from being created under those circumstances.
	if err := cli.validAccessToken(); err == ErrUnauthorized {
		return cli, err
	}

	return cli, nil
}
