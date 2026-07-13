package transport

import (
	"sync"
	"testing"
)

func TestSubjectMatches(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"md.BTCUSDT", "md.BTCUSDT", true},
		{"md.*", "md.BTCUSDT", true},
		{"md.*", "md.BTCUSDT.extra", false},
		{"alerts.>", "alerts.rule", true},
		{"alerts.>", "alerts.rule.spoofing", true},
		{"alerts.>", "alerts", false},
		{"md.*", "features.BTCUSDT", false},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
	}
	for _, c := range cases {
		if got := subjectMatches(c.pattern, c.subject); got != c.want {
			t.Errorf("subjectMatches(%q,%q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

func TestInProcPubSub(t *testing.T) {
	b := NewInProc()
	defer b.Close()

	var mu sync.Mutex
	var got []string
	_, err := b.Subscribe("md.*", func(subject string, data []byte) {
		mu.Lock()
		got = append(got, subject+":"+string(data))
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := b.Publish(MarketData("btcusdt"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish("features.BTCUSDT", []byte("y")); err != nil { // non-matching
		t.Fatal(err)
	}

	// Delivery is synchronous, so results are ready immediately.
	if len(got) != 1 || got[0] != "md.BTCUSDT:x" {
		t.Fatalf("got %v, want [md.BTCUSDT:x]", got)
	}
}

func TestInProcUnsubscribe(t *testing.T) {
	b := NewInProc()
	count := 0
	sub, _ := b.Subscribe("alerts.>", func(string, []byte) { count++ })
	b.Publish(AlertRule, []byte("1"))
	sub.Unsubscribe()
	b.Publish(AlertRule, []byte("2"))
	if count != 1 {
		t.Fatalf("count = %d, want 1 (second publish after unsubscribe)", count)
	}
}

func TestInProcClosed(t *testing.T) {
	b := NewInProc()
	b.Close()
	if err := b.Publish("x", nil); err != ErrBusClosed {
		t.Fatalf("publish after close = %v, want ErrBusClosed", err)
	}
	if _, err := b.Subscribe("x", func(string, []byte) {}); err != ErrBusClosed {
		t.Fatalf("subscribe after close = %v, want ErrBusClosed", err)
	}
}
