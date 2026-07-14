package okx

import (
	"hash/crc32"
	"sort"
	"strings"

	"github.com/argus-mss/argus/internal/fixedpoint"
)

// stringBook is a lightweight L2 ladder that preserves the venue's original
// price/size strings. It exists solely to validate OKX's CRC32 book checksums,
// which are computed over the *verbatim* strings the exchange sent — rendering
// our fixed-point values back to text could differ by a trailing zero and break
// the CRC, so the raw strings are kept alongside the parsed prices used for
// ordering.
type stringBook struct {
	bids []strLevel // descending by price
	asks []strLevel // ascending by price
}

type strLevel struct {
	px    fixedpoint.Value
	pxStr string
	szStr string
}

func newStringBook() *stringBook { return &stringBook{} }

func (b *stringBook) reset() {
	b.bids = b.bids[:0]
	b.asks = b.asks[:0]
}

// set inserts, replaces, or (szStr=="0") removes a level, keeping ladders
// sorted. Returns an error only for unparseable prices.
func (b *stringBook) set(isBid bool, pxStr, szStr string) error {
	px, err := fixedpoint.Parse(pxStr)
	if err != nil {
		return err
	}
	ladder := &b.asks
	if isBid {
		ladder = &b.bids
	}
	var i int
	if isBid {
		i = sort.Search(len(*ladder), func(i int) bool { return (*ladder)[i].px <= px })
	} else {
		i = sort.Search(len(*ladder), func(i int) bool { return (*ladder)[i].px >= px })
	}
	found := i < len(*ladder) && (*ladder)[i].px == px

	remove := isZero(szStr)
	switch {
	case found && remove:
		*ladder = append((*ladder)[:i], (*ladder)[i+1:]...)
	case found:
		(*ladder)[i].pxStr = pxStr
		(*ladder)[i].szStr = szStr
	case remove:
		// removing an absent level: no-op
	default:
		*ladder = append(*ladder, strLevel{})
		copy((*ladder)[i+1:], (*ladder)[i:])
		(*ladder)[i] = strLevel{px: px, pxStr: pxStr, szStr: szStr}
	}
	return nil
}

// applyRows applies OKX level rows ([px, sz, ...]) to one side.
func (b *stringBook) applyRows(isBid bool, rows [][]string) error {
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		if err := b.set(isBid, row[0], row[1]); err != nil {
			return err
		}
	}
	return nil
}

// checksumPayload builds the OKX checksum string: the top 25 levels of each
// side interleaved as "bidPx:bidSz:askPx:askSz:...". When one side runs out,
// the remaining levels of the other side are appended as "px:sz" pairs.
func (b *stringBook) checksumPayload() string {
	const depth = 25
	nb, na := min(len(b.bids), depth), min(len(b.asks), depth)
	var sb strings.Builder
	n := max(nb, na)
	for i := 0; i < n; i++ {
		if i < nb {
			if sb.Len() > 0 {
				sb.WriteByte(':')
			}
			sb.WriteString(b.bids[i].pxStr)
			sb.WriteByte(':')
			sb.WriteString(b.bids[i].szStr)
		}
		if i < na {
			if sb.Len() > 0 {
				sb.WriteByte(':')
			}
			sb.WriteString(b.asks[i].pxStr)
			sb.WriteByte(':')
			sb.WriteString(b.asks[i].szStr)
		}
	}
	return sb.String()
}

// checksum returns the signed CRC32 (IEEE) OKX compares against.
func (b *stringBook) checksum() int32 {
	return int32(crc32.ChecksumIEEE([]byte(b.checksumPayload())))
}

// isZero reports whether a decimal string equals zero ("0", "0.0", …).
func isZero(s string) bool {
	for _, r := range s {
		if r != '0' && r != '.' {
			return false
		}
	}
	return len(s) > 0
}
