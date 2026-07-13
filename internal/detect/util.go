package detect

import "math"

// round4 rounds to 4 decimals so evidence floats serialize cleanly and
// deterministically in the audit log.
func round4(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return math.Round(x*1e4) / 1e4
}
