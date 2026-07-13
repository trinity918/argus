package binance

import (
	"testing"

	"github.com/argus-mss/argus/internal/events"
	fp "github.com/argus-mss/argus/internal/fixedpoint"
)

func TestParseDepthMessage(t *testing.T) {
	raw := []byte(`{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":100,"u":105,"b":[["100.50","2.5"],["100.40","0"]],"a":[["100.60","1.0"]]}}`)
	env, err := parseMessage(raw, 42)
	if err != nil {
		t.Fatal(err)
	}
	if env == nil || env.Kind != events.KindDepth {
		t.Fatalf("expected depth envelope, got %+v", env)
	}
	d := env.Depth
	if d.Symbol != "BTCUSDT" || d.FirstUpdateID != 100 || d.FinalUpdateID != 105 {
		t.Fatalf("bad header: %+v", d)
	}
	if d.IngestTsNs != 42 || d.ExchangeTsMs != 1700000000000 {
		t.Fatalf("bad timestamps: ingest=%d exch=%d", d.IngestTsNs, d.ExchangeTsMs)
	}
	if len(d.Bids) != 2 || d.Bids[0].Price != fp.MustParse("100.50") || d.Bids[0].Qty != fp.MustParse("2.5") {
		t.Fatalf("bad bids: %+v", d.Bids)
	}
	if d.Bids[1].Qty != 0 { // qty 0 => removal
		t.Fatalf("expected removal level, got %+v", d.Bids[1])
	}
	if len(d.Asks) != 1 || d.Asks[0].Price != fp.MustParse("100.60") {
		t.Fatalf("bad asks: %+v", d.Asks)
	}
}

func TestParseTradeMessageAggressor(t *testing.T) {
	// m=true => buyer is maker => aggressor is the seller.
	raw := []byte(`{"stream":"btcusdt@trade","data":{"e":"trade","E":1700000000001,"s":"BTCUSDT","t":555,"p":"100.55","q":"0.3","T":1700000000000,"m":true}}`)
	env, err := parseMessage(raw, 7)
	if err != nil {
		t.Fatal(err)
	}
	tr := env.Trade
	if tr == nil || tr.Aggressor != events.Sell {
		t.Fatalf("expected sell aggressor, got %+v", tr)
	}
	if tr.Price != fp.MustParse("100.55") || tr.Qty != fp.MustParse("0.3") || tr.TradeID != 555 {
		t.Fatalf("bad trade fields: %+v", tr)
	}

	// m=false => buyer is taker => aggressor is the buyer.
	raw2 := []byte(`{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","t":556,"p":"100.60","q":"1","T":1,"m":false}}`)
	env2, _ := parseMessage(raw2, 8)
	if env2.Trade.Aggressor != events.Buy {
		t.Fatalf("expected buy aggressor, got %v", env2.Trade.Aggressor)
	}
}

func TestParseUnknownAndControlFrames(t *testing.T) {
	// Unknown inner event type -> skipped (nil,nil).
	env, err := parseMessage([]byte(`{"stream":"x@kline","data":{"e":"kline"}}`), 0)
	if err != nil || env != nil {
		t.Fatalf("unknown type should skip, got env=%v err=%v", env, err)
	}
	// A non-combined control frame (no data) -> skipped.
	env, err = parseMessage([]byte(`{"result":null,"id":1}`), 0)
	if err != nil || env != nil {
		t.Fatalf("control frame should skip, got env=%v err=%v", env, err)
	}
}
