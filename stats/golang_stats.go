package stats

import (
	"runtime"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/datapoint/dplocal"
)

type golangKeeper struct {
	startTime time.Time
}

// NewGolangKeeper returns a new stats keeper that can return internal golang stats
func NewGolangKeeper() Keeper {
	return &golangKeeper{
		startTime: time.Now(),
	}
}

func point(name string, v int64) *datapoint.Datapoint {
	return dplocal.NewOnHostDatapointDimensions(
		name,
		datapoint.NewIntValue(v),
		datapoint.Gauge,
		map[string]string{"stattype": "golang_sys"})
}

func pointc(name string, v int64) *datapoint.Datapoint {
	return dplocal.NewOnHostDatapointDimensions(
		name,
		datapoint.NewIntValue(v),
		datapoint.Counter,
		map[string]string{"stattype": "golang_sys"})
}

func (statKeeper *golangKeeper) Stats() []*datapoint.Datapoint {
	ret := []*datapoint.Datapoint{}
	ret = append(
		ret,
		point("GOMAXPROCS", int64(runtime.GOMAXPROCS(0))))
	ret = append(
		ret,
		point("process.uptime.ns", time.Now().Sub(statKeeper.startTime).Nanoseconds()))
	ret = append(
		ret,
		point("num_cpu", int64(runtime.NumCPU())))
	ret = append(
		ret,
		pointc("num_cgo_call", int64(runtime.NumCgoCall())))
	ret = append(
		ret,
		point("num_goroutine", int64(runtime.NumGoroutine())))

	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	ret = append(
		ret,
		point("Alloc", int64(m.Alloc)))
	ret = append(
		ret,
		pointc("TotalAlloc", int64(m.TotalAlloc)))
	ret = append(
		ret,
		point("Sys", int64(m.Sys)))
	ret = append(
		ret,
		pointc("Lookups", int64(m.Lookups)))
	ret = append(
		ret,
		pointc("Mallocs", int64(m.Mallocs)))
	ret = append(
		ret,
		pointc("Frees", int64(m.Frees)))
	ret = append(
		ret,
		point("HeapAlloc", int64(m.HeapAlloc)))
	ret = append(
		ret,
		point("HeapSys", int64(m.HeapSys)))
	ret = append(
		ret,
		point("HeapIdle", int64(m.HeapIdle)))
	ret = append(
		ret,
		point("HeapInuse", int64(m.HeapInuse)))
	ret = append(
		ret,
		point("HeapReleased", int64(m.HeapReleased)))
	ret = append(
		ret,
		point("HeapObjects", int64(m.HeapObjects)))
	ret = append(
		ret,
		point("StackInuse", int64(m.StackInuse)))
	ret = append(
		ret,
		point("StackSys", int64(m.StackSys)))
	ret = append(
		ret,
		point("MSpanInuse", int64(m.MSpanInuse)))
	ret = append(
		ret,
		point("MSpanSys", int64(m.MSpanSys)))
	ret = append(
		ret,
		point("MCacheInuse", int64(m.MCacheInuse)))
	ret = append(
		ret,
		point("MCacheSys", int64(m.MCacheSys)))
	ret = append(
		ret,
		point("BuckHashSys", int64(m.BuckHashSys)))
	ret = append(
		ret,
		point("GCSys", int64(m.GCSys)))
	ret = append(
		ret,
		point("OtherSys", int64(m.OtherSys)))
	ret = append(
		ret,
		point("NextGC", int64(m.NextGC)))
	ret = append(
		ret,
		point("LastGC", int64(m.LastGC)))
	ret = append(
		ret,
		pointc("PauseTotalNs", int64(m.PauseTotalNs)))
	ret = append(
		ret,
		pointc("NumGC", int64(m.NumGC)))
	return ret
}
