package config

import (
	"errors"
	"fmt"
	"github.com/coreos/etcd/embed"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/signalfx/embetcd/embetcd"
	"github.com/signalfx/golib/log"
	"github.com/signalfx/golib/pointer"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestInvalidConfig(t *testing.T) {
	_, err := Load("invalidodesnotexist___SDFSDFSD", log.Discard)
	assert.Error(t, err)
}

func TestStringConv(t *testing.T) {
	name := string("aname")
	lf := ListenFrom{Name: &name}
	assert.Contains(t, lf.String(), "aname")

	ft := ForwardTo{Name: &name}
	assert.Contains(t, ft.String(), "aname")
}

func TestBadDecode(t *testing.T) {
	_, err := decodeConfig([]byte("badjson"))
	assert.Error(t, err)
}

func TestGetDefaultName(t *testing.T) {
	u := getDefaultName(func() (string, error) {
		return "", errors.New("")
	})
	assert.Equal(t, u, "unknown")

}

func TestParseStatsDelay(t *testing.T) {
	config, _ := decodeConfig([]byte(`{"StatsDelay":"3s"}`))
	assert.Equal(t, *config.StatsDelayDuration, time.Second*3)
	_, err := decodeConfig([]byte(`{"StatsDelay":"3r"}`))
	assert.Error(t, err)
}

func TestParseInternalMetricsReportingDelay(t *testing.T) {
	config, _ := decodeConfig([]byte(`{"InternalMetricsReportingDelay":"3s"}`))
	assert.Equal(t, *config.InternalMetricsReportingDelayDuration, time.Second*3)
	_, err := decodeConfig([]byte(`{"InternalMetricsReportingDelay":"3r"}`))
	assert.Error(t, err)
}

func TestParseForwardTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3s"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].TimeoutDuration, time.Second*3)
	config, err = decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3r"}]}`))
	assert.Error(t, err)
}

func TestParseForwardOrdering(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ForwardTo":[{"Timeout":"3s", "DimensionsOrder": ["hi"]}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].TimeoutDuration, time.Second*3)
	assert.Equal(t, config.ForwardTo[0].DimensionsOrder[0], "hi")
	assert.Equal(t, 1, len(config.ForwardTo[0].DimensionsOrder))
	assert.Contains(t, config.Var().String(), "DimensionsOrder")
	assert.Contains(t, config.String(), "<config object>")
}

func TestParseListenFromTimeout(t *testing.T) {
	config, err := decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3s"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ListenFrom[0].TimeoutDuration, time.Second*3, "Shouldn't fail parsing")
	config, err = decodeConfig([]byte(`{"ListenFrom":[{"Timeout":"3r"}]}`))
	assert.Error(t, err)
}

func TestDecodeEnvAuthToken(t *testing.T) {
	prevAccessToken := os.Getenv("SIGNALFX_ACCESS_TOKEN")
	os.Setenv("SIGNALFX_ACCESS_TOKEN", "test-token")
	defer func() { os.Setenv("SIGNALFX_ACCESS_TOKEN", prevAccessToken) }()
	config, err := decodeConfig([]byte(`{"ForwardTo":[{}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].DefaultAuthToken, "test-token")

	config, err = decodeConfig([]byte(`{"ForwardTo":[{"AuthTokenEnvVar":"CUSTOM_VAR"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].DefaultAuthToken, "test-token")

	config, err = decodeConfig([]byte(`{"ForwardTo":[{"DefaultAuthToken":"default-token"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].DefaultAuthToken, "default-token")

	config, err = decodeConfig([]byte(`{"ForwardTo":[{"AuthTokenEnvVar":"CUSTOM_VAR","DefaultAuthToken":"default-token"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].DefaultAuthToken, "default-token")

	os.Setenv("CUSTOM_VAR", "secret-token")
	defer func() { os.Unsetenv("CUSTOM_VAR") }()
	config, err = decodeConfig([]byte(`{"ForwardTo":[{"AuthTokenEnvVar":"CUSTOM_VAR","DefaultAuthToken":"default-token"}]}`))
	assert.Nil(t, err)
	assert.Equal(t, *config.ForwardTo[0].DefaultAuthToken, "secret-token")
}

func TestFileLoading(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()

	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	assert.Nil(t, err)
	defer func() {
		assert.NoError(t, os.Remove(filename))
	}()
	_, err = loadConfig(filename)
	assert.Nil(t, err)

}

func TestLoad(t *testing.T) {
	fileObj, _ := ioutil.TempFile("", "gotest")
	filename := fileObj.Name()
	defer func() {
		assert.NoError(t, os.Remove(filename))
	}()

	err := ioutil.WriteFile(filename, []byte(`{"ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	assert.Nil(t, err)
	_, err = Load(filename, log.Discard)
	assert.Nil(t, err)
	prev := xdgbasedirGetConfigFileLocation
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return "", errors.New("bad") }
	defer func() { xdgbasedirGetConfigFileLocation = prev }()
	_, err = Load(filename, log.Discard)
	assert.Equal(t, "bad", fmt.Sprintf("%s", err), "Expect error when xdg loading fails")
	xdgbasedirGetConfigFileLocation = func(string) (string, error) { return filename, nil }
	_, err = Load(filename, log.Discard)
	assert.Nil(t, err)

	err = ioutil.WriteFile(filename, []byte(`{"ClusterName":"", "ListenFrom":[{"Timeout":"3s"}]}`), os.FileMode(0644))
	assert.Nil(t, err)
	_, err = Load(filename, log.Discard)
	assert.Error(t, err)
}

func TestDecodeConfig(t *testing.T) {
	validConfigs := []string{`{}`, `{"host": "192.168.10.2"}`}
	for _, config := range validConfigs {
		_, err := decodeConfig([]byte(config))
		assert.Nil(t, err)
	}
}

func TestConfigTimeout(t *testing.T) {
	configStr := `{"ForwardTo":[{"Type": "thrift", "Timeout": "3s"}]}`
	config, err := decodeConfig([]byte(configStr))
	assert.Nil(t, err)
	if len(config.ForwardTo) != 1 {
		t.Error("Expected length one for forward to")
	}
	if *config.ForwardTo[0].TimeoutDuration != time.Second*3 {
		t.Error("Expected 3 sec timeout")
	}
}

func TestGracefulDurations(t *testing.T) {
	convey.Convey("set up a valid config with durations", t, func() {
		config, err := decodeConfig([]byte(`{"MaxGracefulWaitTime":"1s","GracefulCheckInterval":"1s","SilentGracefulTime":"1s"}`))
		convey.So(err, convey.ShouldBeNil)
		convey.So(*config.GracefulCheckIntervalDuration, convey.ShouldEqual, time.Second)
		convey.So(*config.MaxGracefulWaitTimeDuration, convey.ShouldEqual, time.Second)
		convey.So(*config.SilentGracefulTimeDuration, convey.ShouldEqual, time.Second)
	})
	convey.Convey("set up an invalid config with bad durations", t, func() {
		_, err := decodeConfig([]byte(`{"MaxGracefulWaitTime":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
		_, err = decodeConfig([]byte(`{"GracefulCheckInterval":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
		_, err = decodeConfig([]byte(`{"SilentGracefulTime":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
		_, err = decodeConfig([]byte(`{"FutureThreshold":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
		_, err = decodeConfig([]byte(`{"LateThreshold":"1z"}`))
		convey.So(err, convey.ShouldNotBeNil)
	})

}

func TestGatewayConfig_ToEtcdConfig(t *testing.T) {
	type fields struct {
		ForwardTo                      []*ForwardTo
		ListenFrom                     []*ListenFrom
		StatsDelay                     *string
		StatsDelayDuration             *time.Duration
		NumProcs                       *int
		LocalDebugServer               *string
		PidFilename                    *string
		LogDir                         *string
		LogMaxSize                     *int
		LogMaxBackups                  *int
		LogFormat                      *string
		PprofAddr                      *string
		DebugFlag                      *string
		MaxGracefulWaitTime            *string
		GracefulCheckInterval          *string
		SilentGracefulTime             *string
		MaxGracefulWaitTimeDuration    *time.Duration
		GracefulCheckIntervalDuration  *time.Duration
		SilentGracefulTimeDuration     *time.Duration
		LateThreshold                  *string
		FutureThreshold                *string
		LateThresholdDuration          *time.Duration
		FutureThresholdDuration        *time.Duration
		AdditionalDimensions           map[string]string
		InternalMetricsListenerAddress *string
		ServerName                     *string
		ClusterName                    *string
		ClusterOperation               *string
		ClusterDataDir                 *string
		TargetClusterAddresses         []string
		AdvertisedPeerAddresses        []string
		AdvertisePeerAddress           *string
		ListenOnPeerAddresses          []string
		ListenOnPeerAddress            *string
		AdvertisedClientAddresses      []string
		AdvertiseClientAddress         *string
		ListenOnClientAddresses        []string
		ListenOnClientAddress          *string
		EtcdListenOnMetricsAddresses   []string
		ETCDMetricsAddress             *string
		UnhealthyMemberTTL             *time.Duration
		RemoveMemberTimeout            *time.Duration
		EtcdDialTimeout                *time.Duration
		EtcdClusterCleanUpInterval     *time.Duration
		EtcdAutoSyncInterval           *time.Duration
		EtcdStartupGracePeriod         *time.Duration
		EtcdHeartBeatInterval          *time.Duration
		EtcdElectionTimeout            *time.Duration
		EtcdSnapCount                  *uint64
		EtcdMaxSnapFiles               *uint
		EtcdMaxWalFiles                *uint
	}
	tests := []struct {
		name   string
		fields fields
		want   *embetcd.Config
	}{
		{
			name: "test config to etcd config",
			fields: fields{
				ClusterName:             pointer.String("testCluster1"),
				ServerName:              pointer.String("testServer1"),
				ClusterDataDir:          pointer.String("./etcd-config"),
				ClusterOperation:        pointer.String("join"),
				AdvertisePeerAddress:    pointer.String("http://127.0.0.1:9990"),
				AdvertisedPeerAddresses: []string{"https://127.0.0.1:9991", "http://127.0.0.1:9992", "127.0.0.1:9993"},
				TargetClusterAddresses:  []string{"https://127.0.0.1:9994"},
				EtcdHeartBeatInterval:   pointer.Duration(time.Millisecond * 200),
				EtcdElectionTimeout:     pointer.Duration(time.Millisecond * 2000),
				EtcdSnapCount:           pointer.Uint64(50),
				EtcdMaxSnapFiles:        pointer.Uint(100),
				EtcdMaxWalFiles:         pointer.Uint(150),
			},
			want: &embetcd.Config{
				ClusterName: "testCluster1",
				Config: &embed.Config{
					Name:         "testServer1",
					Dir:          "./etcd-config",
					ClusterState: "join",
					TickMs:       200,
					ElectionMs:   2000,
					SnapCount:    50,
					MaxSnapFiles: 100,
					MaxWalFiles:  150,
					APUrls:       []url.URL{{Scheme: "http", Host: "127.0.0.1:9990"}, {Scheme: "https", Host: "127.0.0.1:9991"}, {Scheme: "http", Host: "127.0.0.1:9992"}, {Scheme: "http", Host: "127.0.0.1:9993"}},
				},
			},
		},
		{
			name: "test config to etcd config with invalid cluster op",
			fields: fields{
				ClusterName:             pointer.String("testCluster1"),
				ServerName:              pointer.String("testServer1"),
				ClusterDataDir:          pointer.String("./etcd-config"),
				ClusterOperation:        pointer.String("yabadabadoo"),
				AdvertisePeerAddress:    pointer.String("http://127.0.0.1:9990"),
				AdvertisedPeerAddresses: []string{"https://127.0.0.1:9991", "http://127.0.0.1:9992", "127.0.0.1:9993"},
				TargetClusterAddresses:  []string{"https://127.0.0.1:9994"},
			},
			want: &embetcd.Config{
				ClusterName: "testCluster1",
				Config: &embed.Config{
					Name:         "testServer1",
					Dir:          "./etcd-config",
					ClusterState: "yabadabadoo",
					APUrls:       []url.URL{{Scheme: "http", Host: "127.0.0.1:9990"}, {Scheme: "https", Host: "127.0.0.1:9991"}, {Scheme: "http", Host: "127.0.0.1:9992"}, {Scheme: "http", Host: "127.0.0.1:9993"}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GatewayConfig{
				ForwardTo:                      tt.fields.ForwardTo,
				ListenFrom:                     tt.fields.ListenFrom,
				StatsDelay:                     tt.fields.StatsDelay,
				StatsDelayDuration:             tt.fields.StatsDelayDuration,
				NumProcs:                       tt.fields.NumProcs,
				LocalDebugServer:               tt.fields.LocalDebugServer,
				PidFilename:                    tt.fields.PidFilename,
				LogDir:                         tt.fields.LogDir,
				LogMaxSize:                     tt.fields.LogMaxSize,
				LogMaxBackups:                  tt.fields.LogMaxBackups,
				LogFormat:                      tt.fields.LogFormat,
				PprofAddr:                      tt.fields.PprofAddr,
				DebugFlag:                      tt.fields.DebugFlag,
				MaxGracefulWaitTime:            tt.fields.MaxGracefulWaitTime,
				GracefulCheckInterval:          tt.fields.GracefulCheckInterval,
				SilentGracefulTime:             tt.fields.SilentGracefulTime,
				MaxGracefulWaitTimeDuration:    tt.fields.MaxGracefulWaitTimeDuration,
				GracefulCheckIntervalDuration:  tt.fields.GracefulCheckIntervalDuration,
				SilentGracefulTimeDuration:     tt.fields.SilentGracefulTimeDuration,
				LateThreshold:                  tt.fields.LateThreshold,
				FutureThreshold:                tt.fields.FutureThreshold,
				LateThresholdDuration:          tt.fields.LateThresholdDuration,
				FutureThresholdDuration:        tt.fields.FutureThresholdDuration,
				AdditionalDimensions:           tt.fields.AdditionalDimensions,
				InternalMetricsListenerAddress: tt.fields.InternalMetricsListenerAddress,
				ServerName:                     tt.fields.ServerName,
				ClusterName:                    tt.fields.ClusterName,
				ClusterOperation:               tt.fields.ClusterOperation,
				ClusterDataDir:                 tt.fields.ClusterDataDir,
				TargetClusterAddresses:         tt.fields.TargetClusterAddresses,
				AdvertisedPeerAddresses:        tt.fields.AdvertisedPeerAddresses,
				AdvertisePeerAddress:           tt.fields.AdvertisePeerAddress,
				ListenOnPeerAddresses:          tt.fields.ListenOnPeerAddresses,
				ListenOnPeerAddress:            tt.fields.ListenOnPeerAddress,
				AdvertisedClientAddresses:      tt.fields.AdvertisedClientAddresses,
				AdvertiseClientAddress:         tt.fields.AdvertiseClientAddress,
				ListenOnClientAddresses:        tt.fields.ListenOnClientAddresses,
				ListenOnClientAddress:          tt.fields.ListenOnClientAddress,
				EtcdListenOnMetricsAddresses:   tt.fields.EtcdListenOnMetricsAddresses,
				ETCDMetricsAddress:             tt.fields.ETCDMetricsAddress,
				UnhealthyMemberTTL:             tt.fields.UnhealthyMemberTTL,
				RemoveMemberTimeout:            tt.fields.RemoveMemberTimeout,
				EtcdDialTimeout:                tt.fields.EtcdDialTimeout,
				EtcdClusterCleanUpInterval:     tt.fields.EtcdClusterCleanUpInterval,
				EtcdAutoSyncInterval:           tt.fields.EtcdAutoSyncInterval,
				EtcdStartupGracePeriod:         tt.fields.EtcdStartupGracePeriod,
				EtcdHeartBeatInterval:          tt.fields.EtcdHeartBeatInterval,
				EtcdElectionTimeout:            tt.fields.EtcdElectionTimeout,
				EtcdSnapCount:                  tt.fields.EtcdSnapCount,
				EtcdMaxSnapFiles:               tt.fields.EtcdMaxSnapFiles,
				EtcdMaxWalFiles:                tt.fields.EtcdMaxWalFiles,
			}
			got := g.ToEtcdConfig()
			if !reflect.DeepEqual(got.APUrls, tt.want.APUrls) {

				t.Errorf("APUrls %v, want %v", got.APUrls, tt.want.APUrls)
			}
		})
	}
}
