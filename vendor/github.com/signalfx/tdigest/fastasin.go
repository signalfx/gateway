package tdigest

import "math"

func eval(model, vars []float64) (r float64) {
	for i := 0; i < len(model); i++ {
		r += model[i] * vars[i]
	}
	return r
}

func bound(v float64) float64 {
	switch {
	case v <= 0:
		return 0
	case v >= 1:
		return 1
	default:
		return v
	}
}

const (
	// Cutoffs for models. Note that the ranges overlap. In the
	// overlap we do linear interpolation to guarantee the overall
	// result is "nice"
	c0High = 0.1
	c1High = 0.55
	c2Low  = 0.5
	c2High = 0.8
	c3Low  = 0.75
	c3High = 0.9
	c4Low  = 0.87
)

var (
	// the models
	m0 = []float64{0.2955302411, 1.2221903614, 0.1488583743, 0.2422015816, -0.3688700895, 0.0733398445}
	m1 = []float64{-0.0430991920, 0.9594035750, -0.0362312299, 0.1204623351, 0.0457029620, -0.0026025285}
	m2 = []float64{-0.034873933724, 1.054796752703, -0.194127063385, 0.283963735636, 0.023800124916, -0.000872727381}
	m3 = []float64{-0.37588391875, 2.61991859025, -2.48835406886, 1.48605387425, 0.00857627492, -0.00015802871}
)

func fastAsin(x float64) float64 {
	switch {

	case x < 0:
		return -fastAsin(-x)
	case x > 1:
		return math.NaN()
	case x > c3High:
		return math.Asin(x)
	default:
		// the parameters for all of the models
		vars := []float64{1, x, x * x, x * x * x, 1 / (1 - x), 1 / (1 - x) / (1 - x)}

		// raw grist for interpolation coefficients
		x0 := bound((c0High - x) / c0High)
		x1 := bound((c1High - x) / (c1High - c2Low))
		x2 := bound((c2High - x) / (c2High - c3Low))
		x3 := bound((c3High - x) / (c3High - c4Low))

		// interpolation coefficients
		//noinspection UnnecessaryLocalVariable
		mix0 := x0
		mix1 := (1 - x0) * x1
		mix2 := (1 - x1) * x2
		mix3 := (1 - x2) * x3
		mix4 := 1 - x3

		// now mix all the results together, avoiding extra evaluations
		r := float64(0)
		if mix0 > 0 {
			r += mix0 * eval(m0, vars)
		}
		if mix1 > 0 {
			r += mix1 * eval(m1, vars)
		}
		if mix2 > 0 {
			r += mix2 * eval(m2, vars)
		}
		if mix3 > 0 {
			r += mix3 * eval(m3, vars)
		}
		if mix4 > 0 {
			// model 4 is just the real deal
			r += mix4 * math.Asin(x)
		}
		return r
	}
}
