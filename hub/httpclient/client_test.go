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
	response := &hubclient.RegistrationResponse{
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
		wantResp *hubclient.RegistrationResponse
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
			wantResp: nil,
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
			wantResp: nil,
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
			wantResp: nil,
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
			cli, _ := NewClient(server.URL, "", 5*time.Second, "userAgent")

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
			cli, _ := NewClient(server.URL, "", 5*time.Second, "userAgent")
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
			wantResp: &hubclient.HeartbeatResponse{},
			wantEtag: "",
			wantErr:  false,
		},
		{
			name: "missing from cluster",
			args: args{lease: "thisIsALease", etag: "thisIsAnETag"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusGone)
				w.Write([]byte{})
			},
			wantResp: &hubclient.HeartbeatResponse{},
			wantEtag: "",
			wantErr:  true,
		},
		{
			name: "unauthorized error",
			args: args{lease: "thisIsALease", etag: "thisIsAnETag"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantResp: &hubclient.HeartbeatResponse{},
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
			wantResp: &hubclient.HeartbeatResponse{},
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
			cli, _ := NewClient(server.URL, "", 5*time.Second, "userAgent")

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

func TestHTTPClient_Config(t *testing.T) {
	response := hubclient.Config{
		Budget: &hubclient.Budget{},
	}
	type args struct {
		clusterName string
	}
	tests := []struct {
		name     string
		args     args
		handler  func(w http.ResponseWriter, r *http.Request)
		wantResp *hubclient.Config
		wantErr  bool
	}{
		{
			name: "successfully fetch config",
			args: args{clusterName: "thisIsACluster"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusOK)
				w.Write(bts)
			},
			wantResp: &response,
			wantErr:  false,
		},
		{
			name: "can't find config for cluster name",
			args: args{clusterName: "thisIsACluster"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte{})
			},
			wantResp: nil,
			wantErr:  true,
		},
		{
			name: "unauthorized error",
			args: args{clusterName: "thisIsACluster"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantResp: nil,
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
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set up a test server
			mux := http.NewServeMux()
			mux.HandleFunc(configV2+"/", tt.handler)
			server := httptest.NewServer(mux)
			defer server.Close()

			// setup client
			cli, _ := NewClient(server.URL, "", 5*time.Second, "userAgent")

			// execute register
			state, err := cli.Config(tt.args.clusterName)

			// check returned values
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPClient_Config() error = %v, wantErr %v", err, tt.wantErr)
			} else if !reflect.DeepEqual(state, tt.wantResp) {
				t.Errorf("HTTPClient_Config() state = %v, wantResp %v", state, tt.wantResp)
			}
		})
	}
}

func TestHTTPClient_Cluster(t *testing.T) {
	response := hubclient.Cluster{
		Name:         "thisIsACluster",
		Servers:      []*hubclient.ServerResponse{},
		Distributors: []*hubclient.ServerResponse{},
	}
	type args struct {
		clusterName string
	}
	tests := []struct {
		name     string
		args     args
		handler  func(w http.ResponseWriter, r *http.Request)
		wantResp *hubclient.Cluster
		wantErr  bool
	}{
		{
			name: "successfully fetch cluster state",
			args: args{clusterName: "thisIsACluster"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("etag", "thisIsAnETag")
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusOK)
				w.Write(bts)
			},
			wantResp: &response,
			wantErr:  false,
		},
		{
			name: "cluster not found in hub",
			args: args{clusterName: "thisIsACluster"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("etag", "thisIsAnETag")
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusNotFound)
				w.Write(bts)
			},
			wantResp: nil,
			wantErr:  true,
		},
		{
			name: "unauthorized error",
			args: args{clusterName: "thisIsACluster"},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantResp: nil,
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
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set up a test server
			mux := http.NewServeMux()
			mux.HandleFunc(clusterV2+"/", tt.handler)
			server := httptest.NewServer(mux)
			defer server.Close()

			// setup client
			cli, _ := NewClient(server.URL, "", 5*time.Second, "userAgent")

			// execute register
			state, err := cli.Cluster(tt.args.clusterName)

			// check returned values
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPClient_Cluster() error = %v, wantErr %v", err, tt.wantErr)
			} else if !reflect.DeepEqual(state, tt.wantResp) {
				t.Errorf("HTTPClient_Cluster() state = %v, wantResp %v", state, tt.wantResp)
			}
		})
	}
}

func TestHTTPClient_Clusters(t *testing.T) {
	response := hubclient.ListClustersResponse{}
	tests := []struct {
		name     string
		handler  func(w http.ResponseWriter, r *http.Request)
		wantResp *hubclient.ListClustersResponse
		wantErr  bool
	}{
		{
			name: "successfully fetch cluster state",
			handler: func(w http.ResponseWriter, r *http.Request) {
				bts, _ := response.MarshalJSON()
				w.WriteHeader(http.StatusOK)
				w.Write(bts)
			},
			wantResp: &response,
			wantErr:  false,
		},
		{
			name: "unauthorized error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantResp: nil,
			wantErr:  true,
		},
		{
			name: "unrecognized error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			},
			wantResp: nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set up a test server
			mux := http.NewServeMux()
			mux.HandleFunc(clustersV2+"/", tt.handler)
			server := httptest.NewServer(mux)
			defer server.Close()

			// setup client
			cli, _ := NewClient(server.URL, "", 5*time.Second, "userAgent")

			// execute register
			state, err := cli.Clusters()

			// check returned values
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPClient_Cluster() error = %v, wantErr %v", err, tt.wantErr)
			} else if !reflect.DeepEqual(state, tt.wantResp) {
				t.Errorf("HTTPClient_Cluster() state = %v, wantResp %v", state, tt.wantResp)
			}
		})
	}
}

func TestNewClient(t *testing.T) {
	type args struct {
		authToken  string
		hubAddress string
	}
	tests := []struct {
		name    string
		args    args
		handler func(w http.ResponseWriter, r *http.Request)
		wantErr bool
	}{
		{
			name: "successfully create a client",
			args: args{
				authToken: "thisIsAValidToken",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte{})
			},
			wantErr: false,
		},
		{
			name: "unauthorized error",
			args: args{
				authToken: "thisIsAValidToken",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte{})
			},
			wantErr: true,
		},
		{
			name: "unrecognized error when testing auth token shouldn't return error",
			args: args{
				authToken: "thisIsAValidToken",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			},
			wantErr: false,
		},
		{
			name: "bad hubAddress returns error",
			args: args{
				authToken:  "thisIsAnAuthToken",
				hubAddress: `http_:\\hello/foo`,
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte{})
			},
			wantErr: true,
		},
		{
			name: "bad empty auth token returns error",
			args: args{
				authToken: "",
			},
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte{})
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set up a test server
			mux := http.NewServeMux()
			mux.HandleFunc(clustersV2+"/", tt.handler)
			server := httptest.NewServer(mux)
			defer server.Close()

			var hubAddress = tt.args.hubAddress
			if hubAddress == "" {
				hubAddress = server.URL
			}
			// setup client
			_, err := NewClient(hubAddress, tt.args.authToken, 5*time.Second, "userAgent")

			// check returned values
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
