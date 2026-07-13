package detect

import "time"

// nowNs is the package default clock, indirected so tests can drive time
// deterministically via Engine's WithClock option.
var nowNs = func() int64 { return time.Now().UnixNano() }
