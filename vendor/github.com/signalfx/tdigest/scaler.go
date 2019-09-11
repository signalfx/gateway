package tdigest

import "math"

type scaler interface {
	/**
	 * Computes q as a function of k. This is often faster than finding k as a function of q for some scales.
	 *
	 * @param k          The index value to convert into q scale.
	 * @param normalizer The normalizer value which depends on compression and (possibly) number of points in the
	 *                   digest.
	 * @return The value of q that corresponds to k
	 */
	q(k, normalizer float64) float64
	/**
	 * Converts  a quantile to the k-scale. The normalizer value depends on compression and (possibly) number of points
	 * in the digest. #normalizer(double, double)
	 *
	 * @param q          The quantile
	 * @param normalizer The normalizer value which depends on compression and (possibly) number of points in the
	 *                   digest.
	 * @return The corresponding value of k
	 */
	k(q, normalizer float64) float64
	/**
	 * Computes the normalizer given compression and number of points.
	 */
	normalizer(compression, n float64) float64

	/**
	 * Computes the maximum relative size a cluster can have at quantile q. Note that exactly where within the range
	 * spanned by a cluster that q should be isn't clear. That means that this function usually has to be taken at
	 * multiple points and the smallest value used.
	 * <p>
	 * Note that this is the relative size of a cluster. To get the max number of samples in the cluster, multiply this
	 * value times the total number of samples in the digest.
	 *
	 * @param q          The quantile
	 * @param normalizer The normalizer value which depends on compression and (possibly) number of points in the
	 *                   digest.
	 * @return The maximum number of samples that can be in the cluster
	 */
	max(q, normalizer float64) float64

	processedSize(compression float64) int
	unprocessedSize(compression float64) int
}

func defaultProcessedSize(compression float64) int {
	return int(2 * math.Ceil(compression))
}

func defaultUnprocessedSize(compression float64) int {
	return int(8 * math.Ceil(compression))
}

/**
 * Generates cluster sizes proportional to sqrt(q*(1-q)). This gives constant relative accuracy if accuracy is
 * proportional to squared cluster size. It is expected that K_2 and K_3 will give better practical results.
 */
type K1 struct{}

func (*K1) processedSize(compression float64) int {
	return defaultProcessedSize(compression)
}

func (*K1) unprocessedSize(compression float64) int {
	return defaultUnprocessedSize(compression)
}

var _ scaler = &K1{}

func (*K1) q(k, normalizer float64) float64 {
	return (math.Sin(k/normalizer) + 1) / 2
}

func (*K1) k(q, normalizer float64) float64 {
	return normalizer * math.Asin(2*q-1)
}

func (*K1) normalizer(compression, n float64) float64 {
	return compression / (2 * math.Pi)
}

func (*K1) max(q, normalizer float64) float64 {
	switch {
	case q <= 0:
		fallthrough
	case q >= 1:
		return 0
	default:
		return 2 * math.Sin(0.5/normalizer) * math.Sqrt(q*(1-q))
	}
}

/**
 * Generates cluster sizes proportional to sqrt(q*(1-q)) but avoids computation of asin in the critical path by
 * using an approximate version.
 */
type K1Fast struct{}

var _ scaler = &K1Fast{}

func (*K1Fast) processedSize(compression float64) int {
	return defaultProcessedSize(compression)
}

func (*K1Fast) unprocessedSize(compression float64) int {
	return defaultUnprocessedSize(compression)
}

func (*K1Fast) q(k, normalizer float64) float64 {
	return (math.Sin(k/normalizer) + 1) / 2
}

func (*K1Fast) k(q, normalizer float64) float64 {
	return normalizer * fastAsin(2*q-1)
}

func (*K1Fast) normalizer(compression, n float64) float64 {
	return compression / (2 * math.Pi)
}

func (*K1Fast) max(q, normalizer float64) float64 {
	switch {
	case q <= 0:
		fallthrough
	case q >= 1:
		return 0
	default:
		return 2 * math.Sin(0.5/normalizer) * math.Sqrt(q*(1-q))
	}
}

const splitPoint = 0.5

/**
 * K1Spliced generates cluster sizes proportional to sqrt(1-q) for q >= 1/2, and uniform cluster sizes for q < 1/2 by gluing
 * the graph of the K_1 function to its tangent line at q=1/2. Changing the split point is possible.
 */
type K1Spliced struct{}

var _ scaler = &K1Spliced{}

func (*K1Spliced) processedSize(compression float64) int {
	return defaultProcessedSize(compression)
}

func (*K1Spliced) unprocessedSize(compression float64) int {
	return defaultUnprocessedSize(compression)
}

func (*K1Spliced) q(k, normalizer float64) float64 {
	if k <= normalizer*math.Asin(2*splitPoint-1) {
		ww := k/normalizer - math.Asin(2*splitPoint-1)
		return ww*math.Sqrt(splitPoint*(1-splitPoint)) + splitPoint
	}
	return (math.Sin(k/normalizer) + 1) / 2
}

func (*K1Spliced) k(q, normalizer float64) float64 {
	if q <= splitPoint {
		return normalizer * (math.Asin(2*splitPoint-1) + (q-splitPoint)/(math.Sqrt(splitPoint*(1-splitPoint))))
	}
	return normalizer * math.Asin(2*q-1)
}

func (*K1Spliced) normalizer(compression, n float64) float64 {
	return compression / (2 * math.Pi)
}

func (*K1Spliced) max(q, normalizer float64) float64 {
	switch {
	case q <= 0:
		fallthrough
	case q >= 1:
		return 0
	case q <= splitPoint:
		return math.Sqrt(splitPoint*(1-splitPoint)) / normalizer
	default:
		return 2 * math.Sin(0.5/normalizer) * math.Sqrt(q*(1-q))
	}
}

type K1SplicedFast struct{}

func (*K1SplicedFast) processedSize(compression float64) int {
	return defaultProcessedSize(compression)
}

func (*K1SplicedFast) unprocessedSize(compression float64) int {
	return defaultUnprocessedSize(compression)
}

var _ scaler = &K1SplicedFast{}

func (*K1SplicedFast) q(k, normalizer float64) float64 {
	if k <= normalizer*fastAsin(2*splitPoint-1) {
		ww := k/normalizer - fastAsin(2*splitPoint-1)
		return ww*math.Sqrt(splitPoint*(1-splitPoint)) + splitPoint
	}
	return (math.Sin(k/normalizer) + 1) / 2
}

func (*K1SplicedFast) k(q, normalizer float64) float64 {
	if q <= splitPoint {
		return normalizer * (fastAsin(2*splitPoint-1) + (q-splitPoint)/(math.Sqrt(splitPoint*(1-splitPoint))))
	}
	return normalizer * fastAsin(2*q-1)
}

func (*K1SplicedFast) normalizer(compression, n float64) float64 {
	return compression / (2 * math.Pi)
}

func (*K1SplicedFast) max(q, normalizer float64) float64 {
	switch {
	case q <= 0:
		fallthrough
	case q >= 1:
		return 0
	case q <= splitPoint:
		return math.Sqrt(splitPoint*(1-splitPoint)) / normalizer
	default:
		return 2 * math.Sin(0.5/normalizer) * math.Sqrt(q*(1-q))
	}
}

/**
 * Generates cluster sizes proportional to q*(1-q). This makes tail error bounds tighter than for K_1. The use of a
 * normalizing function results in a strictly bounded number of clusters no matter how many samples.
 */
type K2 struct{}

var _ scaler = &K2{}

func (*K2) processedSize(compression float64) int {
	return defaultProcessedSize(compression)
}

func (*K2) unprocessedSize(compression float64) int {
	return defaultUnprocessedSize(compression)
}

func (*K2) q(k, normalizer float64) float64 {
	w := math.Exp(k / normalizer)
	return w / (1 + w)
}

func (t *K2) k(q, normalizer float64) float64 {
	switch {
	case q < 1e-15:
		// this will return something more extreme than q = 1/n
		return 2 * t.k(1e-15, normalizer)
	case q > 1-1e-15:
		// this will return something more extreme than q = (n-1)/n
		return 2 * t.k(1-1e-15, normalizer)
	default:
		return math.Log(q/(1-q)) * normalizer
	}
}

func (t *K2) normalizer(compression, n float64) float64 {
	return compression / Z24(compression, n)
}

func (*K2) max(q, normalizer float64) float64 {
	return q * (1 - q) / normalizer
}

func Z24(compression, n float64) float64 {
	return 4*math.Log(n/compression) + 24
}

/**
 * Generates cluster sizes proportional to 1-q for q >= 1/2, and uniform cluster sizes for q < 1/2 by gluing
 * the graph of the K_2 function to its tangent line at q=1/2. Changing the split point is possible.
 */
type K2Spliced struct{}

var _ scaler = &K2Spliced{}

func (*K2Spliced) processedSize(compression float64) int {
	// todo fix
	return defaultProcessedSize(compression)
}

func (*K2Spliced) unprocessedSize(compression float64) int {
	// todo fix
	return defaultUnprocessedSize(compression)
}

func (*K2Spliced) q(k, normalizer float64) float64 {
	if k <= math.Log(splitPoint/(1-splitPoint))*normalizer {
		return splitPoint*(1-splitPoint)*(k/normalizer-math.Log(splitPoint/(1-splitPoint))) + splitPoint
	}
	w := math.Exp(k / normalizer)
	return w / (1 + w)
}

func (t *K2Spliced) k(q, normalizer float64) float64 {
	switch {
	case q <= splitPoint:
		return (((q - splitPoint) / splitPoint / (1 - splitPoint)) + math.Log(splitPoint/(1-splitPoint))) * normalizer
	case q > 1-1e-15:
		// this will return something more extreme than q = (n-1)/n
		return 2 * t.k(1-1e-15, normalizer)
	default:
		return math.Log(q/(1-q)) * normalizer
	}
}

func (t *K2Spliced) normalizer(compression, n float64) float64 {
	return compression / Z24(compression, n)
}

func (*K2Spliced) max(q, normalizer float64) float64 {
	if q <= splitPoint {
		return splitPoint * (1 - splitPoint) * normalizer
	}
	return q * (1 - q) / normalizer
}

/**
 * Generates cluster sizes proportional to min(q, 1-q). This makes tail error bounds tighter than for K_1 or K_2.
 * The use of a normalizing function results in a strictly bounded number of clusters no matter how many samples.
 */
type K3 struct{}

var _ scaler = &K3{}

func (*K3) processedSize(compression float64) int {
	return defaultProcessedSize(compression)
}

func (*K3) unprocessedSize(compression float64) int {
	return defaultUnprocessedSize(compression)
}

func (t *K3) q(k, normalizer float64) float64 {
	if k <= 0 {
		return math.Exp(k/normalizer) / 2
	}
	return 1 - t.q(-k, normalizer)
}

func (t *K3) k(q, normalizer float64) float64 {
	if q < 1e-15 {
		return 10 * t.k(1e-15, normalizer)
	} else if q > 1-1e-15 {
		return 10 * t.k(1-1e-15, normalizer)
	}
	if q <= 0.5 {
		return math.Log(2*q) / normalizer
	}
	return -t.k(1-q, normalizer)
}

func (t *K3) normalizer(compression, n float64) float64 {
	return compression / Z21(compression, n)
}

func (*K3) max(q, normalizer float64) float64 {
	return math.Min(q, 1-q) / normalizer
}

func Z21(compression, n float64) float64 {
	return 4*math.Log(n/compression) + 21
}

/**
 * Generates cluster sizes proportional to 1-q for q >= 1/2, and uniform cluster sizes for q < 1/2 by gluing
 * the graph of the K_3 function to its tangent line at q=1/2.
 */
type K3Spliced struct{}

var _ scaler = &K3Spliced{}

func (*K3Spliced) processedSize(compression float64) int {
	return int(math.Ceil(compression * 0.7))
}

func (*K3Spliced) unprocessedSize(compression float64) int {
	return int(5 * math.Ceil(compression))
}

func (t *K3Spliced) q(k, normalizer float64) float64 {
	if k <= 0 {
		return ((k / normalizer) + 1) / 2
	}
	return 1 - (math.Exp(-k/normalizer) / 2)
}

func (t *K3Spliced) k(q, normalizer float64) float64 {
	switch {
	case q <= 0.5:
		return normalizer * (2*q - 1)
	case q > 1-1e-15:
		return 10 * t.k(1-1e-15, normalizer)
	default:
		return -normalizer * math.Log(2*(1-q))
	}
}

func (t *K3Spliced) normalizer(compression, n float64) float64 {
	return compression / Z21(compression, n)
}

func (*K3Spliced) max(q, normalizer float64) float64 {
	if q <= 0.5 {
		return 1.0 / 2 / normalizer
	}
	return (1 - q) / normalizer
}

/**
 * Generates cluster sizes proportional to 1-q for q >= 1/2, and uniform cluster sizes for q < 1/2 by gluing
 * the graph of the K_3 function to its tangent line at q=1/2.
 */
type KQuadratic struct{}

var _ scaler = &KQuadratic{}

func (*KQuadratic) processedSize(compression float64) int {
	return defaultProcessedSize(compression)
}

func (*KQuadratic) unprocessedSize(compression float64) int {
	return defaultUnprocessedSize(compression)
}

func (t *KQuadratic) q(k, normalizer float64) float64 {
	return math.Sqrt(normalizer*(normalizer+3*k))/normalizer - 1
}

func (t *KQuadratic) k(q, normalizer float64) float64 {
	return normalizer * (q*q + 2*q) / 3
}

func (*KQuadratic) normalizer(compression, n float64) float64 {
	return compression / 2
}

func (*KQuadratic) max(q, normalizer float64) float64 {
	return 3.0 / 2 / normalizer / (1 + q)
}
