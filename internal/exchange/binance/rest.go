package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/argus-mss/argus/internal/events"
)

// rawSnapshot mirrors GET /api/v3/depth: an absolute book with a lastUpdateId
// anchoring it in the diff-stream sequence.
type rawSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

// fetchSnapshot pulls a depth snapshot and normalizes it into a Depth marked
// IsSnapshot, whose FinalUpdateID is the anchor for diff sequencing.
func (c *Client) fetchSnapshot(ctx context.Context, symbol string) (events.Depth, error) {
	q := url.Values{}
	q.Set("symbol", symbol)
	q.Set("limit", strconv.Itoa(c.cfg.DepthLimit))
	endpoint := c.cfg.RESTBase + "/api/v3/depth?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return events.Depth{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return events.Depth{}, fmt.Errorf("snapshot GET %s: %w", symbol, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return events.Depth{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return events.Depth{}, fmt.Errorf("snapshot %s: status %d: %s", symbol, resp.StatusCode, truncate(body, 200))
	}
	var r rawSnapshot
	if err := json.Unmarshal(body, &r); err != nil {
		return events.Depth{}, fmt.Errorf("decode snapshot %s: %w", symbol, err)
	}
	bids, err := parseLevels(r.Bids)
	if err != nil {
		return events.Depth{}, fmt.Errorf("snapshot bids: %w", err)
	}
	asks, err := parseLevels(r.Asks)
	if err != nil {
		return events.Depth{}, fmt.Errorf("snapshot asks: %w", err)
	}
	return events.Depth{
		Symbol:        symbol,
		FinalUpdateID: r.LastUpdateID,
		Bids:          bids,
		Asks:          asks,
		IngestTsNs:    nowNs(),
		Exchange:      exchangeName,
		IsSnapshot:    true,
	}, nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
