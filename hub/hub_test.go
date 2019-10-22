package hub

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/gateway/hub/hubclient"
	"github.com/signalfx/golib/log"

	. "github.com/smartystreets/goconvey/convey"
)

type mockHTTPClient struct {
	lock            sync.Mutex
	registerCounter int64
	register        func(cluster string, name string, version string, payload []byte, distributor bool) (reg hubclient.RegistrationResponse, etag string, err error)
	unregister      func(lease string) error
}

// nolint: vet
func (m mockHTTPClient) Register(cluster string, name string, version string, payload []byte, distributor bool) (reg hubclient.RegistrationResponse, etag string, err error) {
	return m.register(cluster, name, version, payload, distributor)
}

func (m *mockHTTPClient) Unregister(lease string) error {
	return m.unregister(lease)
}

func TestHub_Unregister(t *testing.T) {
	Convey("Unregistration with the gateway hub should", t, func() {
		// mock client
		mock := &mockHTTPClient{
			lock:            sync.Mutex{},
			registerCounter: 0,
		}

		h, err := newHub(log.Discard, "http://localhost:8080", "testauth", 10*time.Second)
		So(err, ShouldBeNil)
		So(h, ShouldNotBeNil)

		mock.register = func(cluster string, name string, version string, payload []byte, distributor bool) (reg hubclient.RegistrationResponse, etag string, err error) {
			mock.lock.Lock()
			defer mock.lock.Unlock()
			mock.registerCounter++
			if mock.registerCounter > 1 {
				return hubclient.RegistrationResponse{
					Lease: "testLease",
					Config: &hubclient.Config{
						Heartbeat: 1000,
					},
					Cluster: &hubclient.Cluster{},
				}, "testEtag", nil
			}
			return hubclient.RegistrationResponse{
				Lease:   "testLease",
				Config:  &hubclient.Config{},
				Cluster: &hubclient.Cluster{},
			}, "testEtag", errors.New("we haven't hit register enough times")
		}
		mock.register = func(cluster string, name string, version string, payload []byte, distributor bool) (reg hubclient.RegistrationResponse, etag string, err error) {
			return hubclient.RegistrationResponse{
				Lease: "testLease",
				Config: &hubclient.Config{
					Heartbeat: 1000,
				},
				Cluster: &hubclient.Cluster{},
			}, "testEtag", nil
		}
		mock.unregister = func(lease string) error {
			return nil
		}

		h.client = mock
		startHub(h)

		Convey("error out if the gateway is already unregisterd", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = h.Unregister(ctx)
			So(err, ShouldEqual, ErrAlreadyUnregistered)
		})

		Convey("error out if the gateway is already closed", func() {
			h.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = h.Unregister(ctx)
			So(err, ShouldEqual, ErrAlreadyClosed)
		})
		Convey("error out if the request context has expired", func() {
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			cancel()
			err = h.Unregister(ctx)
			So(err, ShouldEqual, context.Canceled)
		})
		Convey("should successfully unregister", func() {
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = h.Unregister(ctx)
			So(err, ShouldBeNil)
		})
		Reset(func() {

		})
	})
}

func TestHub_Register(t *testing.T) {
	Convey("Registration with the gateway hub should", t, func() {
		// mock client
		mock := &mockHTTPClient{
			lock:            sync.Mutex{},
			registerCounter: 0,
		}

		h, err := newHub(log.Discard, "http://localhost:8080", "testauth", 10*time.Second)
		So(err, ShouldBeNil)
		So(h, ShouldNotBeNil)

		mock.register = func(cluster string, name string, version string, payload []byte, distributor bool) (reg hubclient.RegistrationResponse, etag string, err error) {
			mock.lock.Lock()
			defer mock.lock.Unlock()
			mock.registerCounter++
			if mock.registerCounter > 1 {
				return hubclient.RegistrationResponse{
					Lease: "testLease",
					Config: &hubclient.Config{
						Heartbeat: 1000,
					},
					Cluster: &hubclient.Cluster{},
				}, "testEtag", nil
			}
			return hubclient.RegistrationResponse{
				Lease:   "testLease",
				Config:  &hubclient.Config{},
				Cluster: &hubclient.Cluster{},
			}, "testEtag", errors.New("we haven't hit register enough times")
		}

		h.client = mock
		startHub(h)

		Convey("succeeds after failing the first time", func() {
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)
			runtime.Gosched()
			hb := atomic.LoadInt64(&h.heartbeatInterval)
			for hb != 1000 {
				t.Log("heartbeat is", hb)
				runtime.Gosched()
				time.Sleep(500 * time.Millisecond)
				hb = atomic.LoadInt64(&h.heartbeatInterval)
			}
		})
		Convey("require a server name", func() {
			err = h.Register("", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldEqual, ErrInvalidServerName)
		})
		Convey("require a cluster name", func() {
			err = h.Register("testServer", "", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldEqual, ErrInvalidClusterName)
		})
		Convey("require a version", func() {
			err = h.Register("testServer", "testCluster", "", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldEqual, ErrInvalidVersion)
		})
		Convey("should return an error if invoked a second time with out unregistering first", func() {
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldEqual, ErrAlreadyRegistered)
		})
		Convey("should return an error when the hub is closed", func() {
			h.Close()
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldEqual, ErrAlreadyClosed)
		})

		Reset(func() {
			h.Close()
		})
	})
}

func Test_waitForErrCh(t *testing.T) {
	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()
	nilRespCh := make(chan error, 1)
	nilRespCh <- nil
	type args struct {
		ctx    context.Context
		respCh chan error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "return error when context is expired",
			args: args{
				ctx:    expiredCtx,
				respCh: make(chan error, 1),
			},
			wantErr: true,
		},
		{
			name: "return nil when",
			args: args{
				ctx:    context.Background(),
				respCh: nilRespCh,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := waitForErrCh(tt.args.ctx, tt.args.respCh); (err != nil) != tt.wantErr {
				t.Errorf("waitForErrCh() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewHub(t *testing.T) {
	type args struct {
		logger     log.Logger
		hubAddress string
		authToken  string
		timeout    time.Duration
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "bad auth token should raise an error",
			args: args{
				logger:    log.Discard,
				authToken: "",
				timeout:   time.Second * 5,
			},
			wantErr: true,
		},
		{
			name: "new hub should be returned with out error",
			args: args{
				logger:    log.Discard,
				authToken: "authToken",
				timeout:   time.Second * 5,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewHub(tt.args.logger, tt.args.hubAddress, tt.args.authToken, tt.args.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewHub() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if got.IsOpen() {
					got.Close()
					if got.IsOpen() {
						t.Errorf("failed to close the hub")
					}
				}
			}
		})
	}
}

func Test_signalableErrCh_signalAndWaitForError(t *testing.T) {
	canceldCtx, cancel := context.WithCancel(context.Background())
	cancel()
	successfulSignalableCh := func() chan chan error {
		ch := make(chan chan error, 1)
		go func() {
			respCh := <-ch
			respCh <- nil
		}()
		return ch
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		e       signalableErrCh
		args    args
		wantErr bool
	}{
		{
			name: "canceled context return an error",
			e:    signalableErrCh(make(chan chan error, 1)),
			args: args{
				ctx: canceldCtx,
			},
			wantErr: true,
		},
		{
			name:    "signaled channel returns error",
			e:       successfulSignalableCh(),
			args:    args{context.Background()},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.e.signalAndWaitForError(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("signalAndWaitForError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
