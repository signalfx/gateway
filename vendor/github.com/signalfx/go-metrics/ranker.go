package metrics

// Ranker is a read only interface for ranking against sample values
type Ranker interface {
	Count() int64
	Max() int64
	Mean() float64
	Min() int64
	Percentile(float64) float64
	Percentiles([]float64) []float64
	Rank(int64) float64
}
