package hub

import (
	"context"
	"errors"
	"fmt"
	"github.com/signalfx/gateway/hub/httpclient"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/signalfx/golib/timekeeper/timekeepertest"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/signalfx/gateway/hub/hubclient"
	"github.com/signalfx/golib/log"

	. "github.com/smartystreets/goconvey/convey"
)

type mockHTTPClient struct {
	lock              sync.Mutex
	registerCounter   int64
	unregisterCounter int64
	heartbeatCounter  int64
	clusterCounter    int64
	clustersCounter   int64
	configCounter     int64
	register          func(cluster string, name string, version string, payload []byte, distributor bool) (reg *hubclient.RegistrationResponse, etag string, err error)
	unregister        func(lease string) error
	heartbeat         func(lease string, etag string) (*hubclient.HeartbeatResponse, string, error)
	cluster           func(clusterName string) (*hubclient.Cluster, error)
	clusters          func() (*hubclient.ListClustersResponse, error)
	config            func(clusterName string) (*hubclient.Config, error)
}

// nolint: vet
func (m *mockHTTPClient) Register(cluster string, name string, version string, payload []byte, distributor bool) (reg *hubclient.RegistrationResponse, etag string, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	atomic.AddInt64(&m.registerCounter, 1)
	if m.register != nil {
		return m.register(cluster, name, version, payload, distributor)
	}
	return &hubclient.RegistrationResponse{Config: &hubclient.Config{Heartbeat: defaultHeartBeatInterval}}, "", nil
}

func (m *mockHTTPClient) Unregister(lease string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	atomic.AddInt64(&m.unregisterCounter, 1)
	if m.unregister != nil {
		return m.unregister(lease)
	}
	return nil
}

func (m *mockHTTPClient) Heartbeat(lease string, etag string) (*hubclient.HeartbeatResponse, string, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	atomic.AddInt64(&m.heartbeatCounter, 1)
	if m.heartbeat != nil {
		return m.heartbeat(lease, etag)
	}
	return &hubclient.HeartbeatResponse{}, "", nil
}

func (m *mockHTTPClient) Cluster(clusterName string) (*hubclient.Cluster, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	atomic.AddInt64(&m.clusterCounter, 1)
	if m.cluster != nil {
		return m.cluster(clusterName)
	}
	return &hubclient.Cluster{}, nil
}

func (m *mockHTTPClient) Clusters() (*hubclient.ListClustersResponse, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	atomic.AddInt64(&m.clustersCounter, 1)
	if m.clusters != nil {
		return m.clusters()
	}
	return &hubclient.ListClustersResponse{}, nil
}

func (m *mockHTTPClient) Config(clusterName string) (*hubclient.Config, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	atomic.AddInt64(&m.configCounter, 1)
	if m.config != nil {
		return m.config(clusterName)
	}
	return &hubclient.Config{}, nil
}

func TestHub_Unregister(t *testing.T) {
	Convey("Unregistration with the gateway hub should", t, func() {
		// mock client
		mock := &mockHTTPClient{
			lock:            sync.Mutex{},
			registerCounter: 0,
		}

		h, err := newHub(log.Discard, "http://localhost:8080", "testauth", 10*time.Second, "userAgent")
		So(err, ShouldBeNil)
		So(h, ShouldNotBeNil)

		mock.register = func(cluster string, name string, version string, payload []byte, distributor bool) (reg *hubclient.RegistrationResponse, etag string, err error) {
			return &hubclient.RegistrationResponse{
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
		mock.heartbeat = func(lease string, etag string) (*hubclient.HeartbeatResponse, string, error) {
			return nil, "", errors.New("not implemented")
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
			h.Close()
		})
	})
}

func getMetric(Datapoints func() []*datapoint.Datapoint, metric string, value int, dims ...string) {
	i := 0
	var last datapoint.Value
	done := make(chan struct{})
	success := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				fmt.Println("looking for ", metric, "with", value, "Last", last)
				success <- struct{}{}
				return
			default:
				runtime.Gosched()
				var dp *datapoint.Datapoint
				dps := Datapoints()
				dpDims := make(map[string]string)
				if len(dims) > 0 {
					for i := 0; i < len(dims); i++ {
						key := dims[i]
						i++
						if i < len(dims) {
							val := dims[i]
							dpDims[key] = val
						}
					}
					dp = dptest.ExactlyOneDims(dps, metric, dpDims)
				} else {
					fmt.Println("metric", metric)
					dp = dptest.ExactlyOne(dps, metric)
				}
				if dp.Value.String() == strconv.Itoa(value) {
					success <- struct{}{}
					return
				}
				last = dp.Value
				i++
			}
		}
	}()
	select {
	case <-success:
		return
	case <-time.After(time.Second * 2):
		close(done)
		<-success
		panic("oops")
	}
}

func TestHub_Register(t *testing.T) {
	Convey("Registration with the gateway hub should", t, func() {
		// mock client
		mock := &mockHTTPClient{
			lock:            sync.Mutex{},
			registerCounter: 0,
		}

		h, err := newHub(log.Discard, "http://localhost:8080", "testauth", 10*time.Second, "userAgent")
		So(err, ShouldBeNil)
		So(h, ShouldNotBeNil)

		mock.register = func(cluster string, name string, version string, payload []byte, distributor bool) (reg *hubclient.RegistrationResponse, etag string, err error) {
			if atomic.LoadInt64(&mock.registerCounter) > 1 {
				return &hubclient.RegistrationResponse{
					Lease: "testLease",
					Config: &hubclient.Config{
						Heartbeat: 1000,
					},
					Cluster: &hubclient.Cluster{},
				}, "testEtag", nil
			}
			return &hubclient.RegistrationResponse{
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
			// ensure that the gateway receives the new config and heartbeat
			getMetric(h.DebugDatapoints, "cluster.heartbeatInterval", 1000)
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

func TestHub_Heartbeat(t *testing.T) {
	Convey("The gateway hub should", t, func() {
		// mock client
		mock := &mockHTTPClient{
			lock:            sync.Mutex{},
			registerCounter: 0,
		}

		h, err := newHub(log.Discard, "http://localhost:8080", "testauth", 10*time.Second, "userAgent")
		So(err, ShouldBeNil)
		So(h, ShouldNotBeNil)

		mock.register = func(cluster string, name string, version string, payload []byte, distributor bool) (reg *hubclient.RegistrationResponse, etag string, err error) {
			return &hubclient.RegistrationResponse{
				Lease: "testLease",
				Config: &hubclient.Config{
					Heartbeat: 1000,
				},
				Cluster: &hubclient.Cluster{},
			}, "testEtag", nil
		}

		mock.cluster = func(clusterName string) (*hubclient.Cluster, error) {
			return &hubclient.Cluster{
				Name:         clusterName,
				Servers:      []*hubclient.ServerResponse{},
				Distributors: []*hubclient.ServerResponse{},
			}, nil
		}

		mock.config = func(clusterName string) (*hubclient.Config, error) {
			return &hubclient.Config{
				Heartbeat: 1000,
			}, nil
		}

		h.client = mock
		stubTimer := timekeepertest.NewStubClock(time.Now())
		h.tk = stubTimer
		startHub(h)

		Convey("successfully beat past the minimum number of successful heartbeats", func() {
			mock.heartbeat = func(lease string, etag string) (*hubclient.HeartbeatResponse, string, error) {
				return &hubclient.HeartbeatResponse{}, "", nil
			}

			// register with the hub
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)
			runtime.Gosched()

			// ensure that the gateway receives the new config and heartbeat
			getMetric(h.DebugDatapoints, "cluster.heartbeatInterval", 1000)

			for atomic.LoadInt64(&mock.heartbeatCounter) < defaultMinSuccessfulHb+1 {
				runtime.Gosched()
				stubTimer.Incr(time.Second)
			}

			So(atomic.LoadInt64(&mock.heartbeatCounter), ShouldBeGreaterThan, 0)
		})

		Convey("fetch cluster state and config when unregistered", func() {
			// register with the hub
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)

			runtime.Gosched()

			err = h.Unregister(context.Background())
			So(err, ShouldBeNil)

			for atomic.LoadInt64(&mock.clusterCounter) == 0 {
				stubTimer.Incr(time.Second)
				runtime.Gosched()
			}

			So(atomic.LoadInt64(&mock.clusterCounter), ShouldBeGreaterThan, 0)
		})

		Convey("re register when lease expires", func() {
			mock.heartbeat = func(lease string, etag string) (*hubclient.HeartbeatResponse, string, error) {
				// after some number of heartbeats send the back the expired lease error
				if atomic.LoadInt64(&mock.heartbeatCounter) > defaultMinSuccessfulHb {
					fmt.Println("sending expired lease")
					return &hubclient.HeartbeatResponse{}, "", httpclient.ErrExpiredLease
				}
				return &hubclient.HeartbeatResponse{}, "", nil
			}

			// register with the hub
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)
			runtime.Gosched()

			// ensure that the gateway receives the new config and heartbeat
			getMetric(h.DebugDatapoints, "cluster.heartbeatInterval", 1000)

			// wait for the registration endpoint to be hit twice
			for atomic.LoadInt64(&mock.registerCounter) < 2 {
				stubTimer.Incr(time.Second)
				runtime.Gosched()
			}

			So(atomic.LoadInt64(&mock.heartbeatCounter), ShouldBeGreaterThan, defaultMinSuccessfulHb)
			So(atomic.LoadInt64(&mock.registerCounter), ShouldBeGreaterThanOrEqualTo, 2)
		})

		Convey("unsuccessful beat resets heartbeat count", func() {
			mock.heartbeat = func(lease string, etag string) (*hubclient.HeartbeatResponse, string, error) {
				return nil, "", errors.New("not implemented")
			}

			// register with the hub
			err = h.Register("testServer", "testCluster", "testVersion", hubclient.ServerPayload(make(map[string][]byte)), false)
			So(err, ShouldBeNil)
			runtime.Gosched()

			// ensure that the gateway receives the new config and heartbeat
			getMetric(h.DebugDatapoints, "cluster.heartbeatInterval", 1000)

			// wait for the heartbeat counter to go up
			for atomic.LoadInt64(&mock.heartbeatCounter) == 0 {
				runtime.Gosched()
				stubTimer.Incr(time.Second)
			}

			So(atomic.LoadInt64(&mock.heartbeatCounter), ShouldBeGreaterThan, 0)
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

func TestHub_DebugDatapoints(t *testing.T) {
	h, err := newHub(log.Discard, "http://localhost:8080", "testauth", 10*time.Second, "userAgent")
	if err != nil || h == nil {
		t.Errorf("failed to create hub")
	}
	h.client = &mockHTTPClient{
		lock:            sync.Mutex{},
		registerCounter: 0,
	}
	startHub(h)
	getMetric(h.DebugDatapoints, "cluster.heartbeatInterval", 0)
	h.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h2 := &Hub{ctx: ctx}
	if len(h2.DebugDatapoints()) > 0 {
		t.Errorf("debug datapoitns should be empty when the context is closed")
	}
}

func TestHub_Datapoints(t *testing.T) {
	h, err := newHub(log.Discard, "http://localhost:8080", "testauth", 10*time.Second, "userAgent")
	if err != nil || h == nil {
		t.Errorf("failed to create hub")
	}
	h.client = &mockHTTPClient{
		lock:            sync.Mutex{},
		registerCounter: 0,
	}
	startHub(h)
	getMetric(h.Datapoints, "hub.clusterSize", 0, "type", "server")
	getMetric(h.Datapoints, "hub.clusterSize", 0, "type", "distributor")
	h.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h2 := &Hub{ctx: ctx}
	if len(h2.Datapoints()) > 0 {
		t.Errorf("datapoitns should be empty when the context is closed")
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
				logger:     log.Discard,
				authToken:  "authToken",
				hubAddress: "http://thisIsAHubAddress",
				timeout:    time.Second * 5,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewHub(tt.args.logger, tt.args.hubAddress, tt.args.authToken, tt.args.timeout, "userAgent")
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

func Test_waitForDatapoints(t *testing.T) {
	closedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	type args struct {
		ctx    context.Context
		respCh chan []*datapoint.Datapoint
	}
	tests := []struct {
		name string
		args args
		want []*datapoint.Datapoint
	}{
		{
			name: "waiting for datapoints with a closed context should return nil",
			args: args{
				ctx:    closedCtx,
				respCh: make(chan []*datapoint.Datapoint, 1),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := waitForDatapoints(tt.args.ctx, tt.args.respCh); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("waitForDatapoints() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_drainRegistrationReqChan(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	bufferedCh := make(chan *registrationReq, 1)
	bufferedCh <- nil
	type args struct {
		ctx context.Context
		ch  chan *registrationReq
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "buffered channel should fire when draining request channel",
			args: args{
				ctx: context.Background(),
				ch:  bufferedCh,
			},
		},
		{
			name: "canceled context should break the wait",
			args: args{
				ctx: canceledCtx,
				ch:  make(chan *registrationReq),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drainRegistrationReqChan(tt.args.ctx, tt.args.ch)
			if len(tt.args.ch) > 0 {
				t.Errorf("the request channel was not drained")
			}
		})
	}
}

func Test_drainConfigCh(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	bufferedCh := make(chan *hubclient.Config, 1)
	bufferedCh <- nil
	type args struct {
		ctx      context.Context
		config   *hubclient.Config
		configCh chan *hubclient.Config
	}
	tests := []struct {
		name string
		args args
		want *hubclient.Config
	}{
		{
			name: "buffered channel should fire when draining config channel",
			args: args{
				ctx:      context.Background(),
				configCh: bufferedCh,
			},
		},
		{
			name: "canceled context should break the select",
			args: args{
				ctx:      canceledCtx,
				configCh: make(chan *hubclient.Config),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := drainConfigCh(tt.args.ctx, tt.args.config, tt.args.configCh); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("drainConfigCh() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_drainClusterCh(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	bufferedCh := make(chan *hubclient.Cluster, 1)
	bufferedCh <- nil
	type args struct {
		ctx       context.Context
		cluster   *hubclient.Cluster
		clusterCh chan *hubclient.Cluster
	}
	tests := []struct {
		name string
		args args
		want *hubclient.Cluster
	}{
		{
			name: "buffered channel should fire when draining cluster channel",
			args: args{
				ctx:       context.Background(),
				clusterCh: bufferedCh,
			},
		},
		{
			name: "canceled context should break the select",
			args: args{
				ctx:       canceledCtx,
				clusterCh: make(chan *hubclient.Cluster),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := drainClusterCh(tt.args.ctx, tt.args.cluster, tt.args.clusterCh); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("drainClusterCh() = %v, want %v", got, tt.want)
			}
		})
	}
}
