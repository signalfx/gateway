package embetcd

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

var (
	// ErrNameConflict is an error indicating that the server name is in conflict with an existing member of the cluster
	ErrNameConflict = fmt.Errorf("server name is in conflict with an existing cluster member")
	// ErrAlreadyRunning is an error indicating that the server is already running
	ErrAlreadyRunning = fmt.Errorf("server is already running")
	// ErrAlreadyStopped is an error indicating that the server is already stopped
	ErrAlreadyStopped = fmt.Errorf("server is already stopped")
	// ErrClusterNameConflict is an error indicating that the configured cluster name conflicts with the target cluster
	ErrClusterNameConflict = fmt.Errorf("cluster name either does not exist in the cluster under '/_etcd-cluster/name' or is different from this server's cluster name")
)

// WaitForStructChOrErrCh waits for the struct channel, error channel or context to return a value
func WaitForStructChOrErrCh(ctx context.Context, structCh <-chan struct{}, errCh <-chan error) error {
	// wait for the server to start or error out
	select {
	case <-structCh:
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DurationOrDefault returns the pointed duration or the specified default
func DurationOrDefault(in *time.Duration, def time.Duration) time.Duration {
	if in != nil {
		return *in
	}
	return def
}

// URLSToStringSlice converts urls slices to string slices
func URLSToStringSlice(urls []url.URL) []string {
	strs := make([]string, 0, len(urls))
	for _, u := range urls {
		strs = append(strs, u.String())
	}
	return strs
}
