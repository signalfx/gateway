package httpclient

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/signalfx/gateway/hub/hubclient"
)

func TestHTTPClient_Register(t *testing.T) {
	response := hubclient.RegistrationResponse{
		Lease: "thisIsALease",
		Config: &hubclient.Config{
			Budget: &hubclient.Budget{},
		},
		Cluster: &hubclient.Cluster{
			Name:         "thisIsACluster",
			Servers:      []*hubclient.ServerResponse{},
			Distributors: []*hubclient.ServerResponse{},
		},
	}
	type args struct {
		lease string
	}
	tests := []struct {
		name     string
		args     *hubclient.Registration
		handler  func(w http.ResponseWriter, r *http.Request)
		wantResp hubclient.RegistrationResponse
		wantEtag string
		wantErr  bool
	}{
		{
			name: "successful registration",
			args: &hubclient.Registration{
				Name:    "",
				Version: "",
				Payload: []byte{},
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("etag", "thisIsAnETag")
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusOK)
				w.Write(bts)
			},
			wantResp: response,
			wantEtag: "thisIsAnETag",
			wantErr:  false,
		},
		{
			name: "server name conflict",
			args: &hubclient.Registration{
				Name:    "",
				Version: "",
				Payload: []byte{},
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusConflict)
				w.Write(bts)
			},
			wantResp: hubclient.RegistrationResponse{
				Lease: "",
			},
			wantEtag: "",
			wantErr:  true,
		},
		{
			name: "unauthorized error",
			args: &hubclient.Registration{
				Name:    "",
				Version: "",
				Payload: []byte{},
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantResp: hubclient.RegistrationResponse{
				Lease: "",
			},
			wantEtag: "",
			wantErr:  true,
		},
		{
			name: "unrecognized error",
			args: &hubclient.Registration{
				Name:    "",
				Version: "",
				Payload: []byte{},
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			},
			wantResp: hubclient.RegistrationResponse{
				Lease: "",
			},
			wantEtag: "",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set up a test server
			mux := http.NewServeMux()
			mux.HandleFunc(registerV2, tt.handler)
			server := httptest.NewServer(mux)
			defer server.Close()

			// setup client
			cli, _ := NewClient(server.URL, "", 5*time.Second)

			// execute register
			resp, etag, err := cli.Register(tt.args.Cluster, tt.args.Name, tt.args.Version, tt.args.Payload, tt.args.Distributor)

			// check returned values
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPClient_Register() error = %v, wantErr %v", err, tt.wantErr)
			} else if etag != tt.wantEtag {
				t.Errorf("HTTPClient_Register() etag = %v, wantEtag %v", etag, tt.wantEtag)
			} else if !reflect.DeepEqual(resp, tt.wantResp) {
				t.Errorf("HTTPClient_Register() resp = %v, wantResp %v", resp, tt.wantResp)
			}
		})
	}
}

func TestHTTPClient_UnRegister(t *testing.T) {
	type args struct {
		lease string
	}
	tests := []struct {
		name    string
		args    args
		handler func(w http.ResponseWriter, r *http.Request)
		wantErr bool
	}{
		{
			name: "successful deregistration",
			args: args{
				lease: "hello",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
				w.Write([]byte{})
			},
			wantErr: false,
		},
		{
			name: "lease already removed",
			args: args{
				lease: "hello",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotModified)
				w.Write([]byte{})
			},
			wantErr: true,
		},
		{
			name: "unsuccessful deregistration",
			args: args{
				lease: "hello",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			},
			wantErr: true,
		},
		{
			name: "unsuccessful deregistration",
			args: args{
				lease: "hello",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantErr: true,
		},
		{
			name: "unsuccessful deregistration",
			args: args{
				lease: "hello",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			},
			wantErr: true,
		},
		{
			name: "unsuccessful deregistration random error",
			args: args{
				lease: "hello",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusGatewayTimeout)
				w.Write([]byte{})
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set up a test server
			mux := http.NewServeMux()
			mux.HandleFunc(unregisterV2+"/", tt.handler)
			server := httptest.NewServer(mux)
			defer server.Close()

			// setup client
			cli, _ := NewClient(server.URL, "", 5*time.Second)
			if err := cli.Unregister(tt.args.lease); (err != nil) != tt.wantErr {
				t.Errorf("HTTPClient_Unregister() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHTTPClient_Heartbeat(t *testing.T) {
	response := hubclient.HeartbeatResponse{
		Config: &hubclient.Config{
			Budget: &hubclient.Budget{},
		},
		Cluster: &hubclient.Cluster{
			Name:         "thisIsACluster",
			Servers:      []*hubclient.ServerResponse{},
			Distributors: []*hubclient.ServerResponse{},
		},
	}
	type args struct {
		lease string
		etag  string
	}
	tests := []struct {
		name     string
		args     args
		handler  func(w http.ResponseWriter, r *http.Request)
		wantResp *hubclient.HeartbeatResponse
		wantEtag string
		wantErr  bool
	}{
		{
			name: "successful heartbeat with new content",
			args: args{lease: "thisIsALease", etag: "thisIsAnETag"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("etag", "thisIsAnETag")
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusOK)
				w.Write(bts)
			},
			wantResp: &response,
			wantEtag: "thisIsAnETag",
			wantErr:  false,
		},
		{
			name: "successful heartbeat without new content",
			args: args{lease: "thisIsALease", etag: "thisIsAnETag"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("etag", "thisIsAnETag")
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusNoContent)
				w.Write(bts)
			},
			wantResp: nil,
			wantEtag: "",
			wantErr:  false,
		},
		{
			name: "unauthorized error",
			args: args{lease: "thisIsALease", etag: "thisIsAnETag"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantResp: nil,
			wantEtag: "",
			wantErr:  true,
		},
		{
			name: "unrecognized error",
			args: args{},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			},
			wantResp: nil,
			wantEtag: "",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set up a test server
			mux := http.NewServeMux()
			mux.HandleFunc(heartbeatV2+"/", tt.handler)
			server := httptest.NewServer(mux)
			defer server.Close()

			// setup client
			cli, _ := NewClient(server.URL, "", 5*time.Second)

			// execute register
			state, etag, err := cli.Heartbeat(tt.args.lease, tt.args.etag)

			// check returned values
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPClient_Heartbeat() error = %v, wantErr %v", err, tt.wantErr)
			} else if etag != tt.wantEtag {
				t.Errorf("HTTPClient_Heartbeat() etag = %v, wantEtag %v", etag, tt.wantEtag)
			} else if !reflect.DeepEqual(state, tt.wantResp) {
				t.Errorf("HTTPClient_Heartbeat() state = %v, wantResp %v", state, tt.wantResp)
			}
		})
	}
}
