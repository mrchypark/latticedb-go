// Package benchvector supplies the shared Zig-compatible benchmark workload.
package benchvector

import (
	"math"
	"math/bits"
)

// Generate returns normalized 128-D clustered vectors and 100 queries, seed 42.
// Count must be positive.
func Generate(count int) (vectors, queries [][]float32) {
	if count <= 0 {
		panic("benchmark count must be positive")
	}
	vectors = make([][]float32, count)
	rng := newZigHarnessRNG(42)
	clusters := max(count/1_000, 10)
	centers := make([]float32, clusters*128)
	for cluster := range clusters {
		zigHarnessUnitVector(centers[cluster*128:(cluster+1)*128], &rng)
	}
	for i := 0; i < count; i++ {
		vector := make([]float32, 128)
		center := centers[(i%clusters)*128 : (i%clusters+1)*128]
		for dimension := range vector {
			vector[dimension] = center[dimension] + zigHarnessGaussian(&rng)*0.1
		}
		zigHarnessNormalize(vector)
		vectors[i] = vector
	}

	queries = make([][]float32, 100)
	for i := range queries {
		baseID := rng.lessThan(uint64(count)) + 1
		base := vectors[baseID-1]
		query := make([]float32, 128)
		copy(query, base)
		for dimension := range query {
			query[dimension] += zigHarnessGaussian(&rng) * 0.05
		}
		zigHarnessNormalize(query)
		queries[i] = query
	}
	return vectors, queries
}

// zigHarnessRNG matches Zig 0.16 std.Random.DefaultPrng (Xoshiro256++).
type zigHarnessRNG struct{ state [4]uint64 }

func newZigHarnessRNG(seed uint64) zigHarnessRNG {
	var rng zigHarnessRNG
	for i := range rng.state {
		seed += 0x9e3779b97f4a7c15
		value := seed
		value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
		value = (value ^ value>>27) * 0x94d049bb133111eb
		rng.state[i] = value ^ value>>31
	}
	return rng
}

func (r *zigHarnessRNG) next() uint64 {
	result := bits.RotateLeft64(r.state[0]+r.state[3], 23) + r.state[0]
	t := r.state[1] << 17
	r.state[2] ^= r.state[0]
	r.state[3] ^= r.state[1]
	r.state[1] ^= r.state[2]
	r.state[0] ^= r.state[3]
	r.state[2] ^= t
	r.state[3] = bits.RotateLeft64(r.state[3], 45)
	return result
}

func (r *zigHarnessRNG) unit() float32 {
	random := r.next()
	leading := bits.LeadingZeros64(random)
	if leading >= 41 {
		leading = 41 + bits.LeadingZeros64(r.next())
		if leading == 105 {
			leading += bits.LeadingZeros32(uint32(r.next()) | 0x7ff)
		}
	}
	mantissa := uint32(random) & (1<<23 - 1)
	exponent := uint32(126-leading) << 23
	return math.Float32frombits(exponent|mantissa)*2 - 1
}

func (r *zigHarnessRNG) lessThan(limit uint64) uint64 {
	threshold := -limit % limit
	for {
		high, low := bits.Mul64(r.next(), limit)
		if low >= threshold {
			return high
		}
	}
}

func zigHarnessGaussian(rng *zigHarnessRNG) float32 {
	for {
		u, v := rng.unit(), rng.unit()
		s := u*u + v*v
		if s > 0 && s < 1 {
			return u * float32(math.Sqrt(-2*math.Log(float64(s))/float64(s)))
		}
	}
}

func zigHarnessUnitVector(vector []float32, rng *zigHarnessRNG) {
	for i := range vector {
		vector[i] = rng.unit()
	}
	zigHarnessNormalize(vector)
}

func zigHarnessNormalize(vector []float32) {
	var norm float32
	for _, value := range vector {
		norm += value * value
	}
	norm = float32(math.Sqrt(float64(norm)))
	for i := range vector {
		vector[i] /= norm
	}
}
