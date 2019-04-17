package embetcd

import (
	"context"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"path"
	"sync"
	"time"

	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/types"
)

// memberHealth is a struct for storing Discovery time and Last time seen healthy
type memberHealth struct {
	// Name is the name of the member in the cluster
	Name string
	// DiscoveredTime indicates the time the member was first discovered
	// this is used to account for member startup time
	Discovered time.Time
	// LastHealthy indicates the last time the member was seen healthy
	LastHealthy time.Time
	// Client for checking member's health
	Client *Client
	// ClientURLs are the client urls to the member
	ClientURLs []string
}

// healthCheckViaMemberHealth checks
func healthCheckViaMemberHealth(timeout context.Context, m *memberHealth) (healthKey bool) {
	if m.ClientURLs != nil && len(m.ClientURLs) > 0 {

		// create a new client
		var err error
		if m.Client == nil {
			m.Client, err = NewClient(cli.Config{Endpoints: m.ClientURLs[:], Context: timeout})
		}

		// set client endpoints
		if err == nil && m.Client != nil {
			m.Client.SetEndpoints(m.ClientURLs[:]...)
			if _, err := m.Client.Get(timeout, "health"); err == nil || err == rpctypes.ErrPermissionDenied {
				healthKey = true
			}
		}
	}
	return healthKey
}

// updateMemberHealthStats fetches the health status of an individual member
func isMemberHealthy(ctx context.Context, m *memberHealth, clusterClient *Client) bool {
	var healthKey bool // whether the member health request succeeded
	var memberKey bool // whether the member key was found in the cluster

	wg := sync.WaitGroup{}

	timeout, cancel := context.WithTimeout(ctx, DefaultDialTimeout)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if clusterClient != nil {
			if resp, nameErr := clusterClient.Get(timeout, path.Join("members", m.Name)); nameErr == nil && len(resp.Kvs) > 0 {
				memberKey = true
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		healthKey = healthCheckViaMemberHealth(timeout, m)
	}()

	wg.Wait()
	return healthKey || memberKey
}

// updateClusterHealthStats checks the health of each member and update times last seen healthy
func updateClusterHealthStats(ctx context.Context, clusterClient *Client, cache map[types.ID]*memberHealth) {
	// create a wait group to wait for health status from each member of the cluster
	wg := sync.WaitGroup{}
	// get the cluster member health concurrently
	for _, m := range cache {
		// fetch the health of the member in a separate go routine
		wg.Add(1)
		go func(ctx context.Context, m *memberHealth, clusterClient *Client) {
			if isMemberHealthy(ctx, m, clusterClient) {
				m.LastHealthy = time.Now()
			}
			wg.Done()
		}(ctx, m, clusterClient)
	}

	wg.Wait()
}
