package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempSymbolsYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "symbols.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}
	return path
}

func TestLoadSymbols_Valid(t *testing.T) {
	path := writeTempSymbolsYAML(t, `
symbols:
  BTC-USD:
    coinbase: BTC-USD
    binance: BTCUSDT
  ETH-USD:
    coinbase: ETH-USD
    binance: ETHUSDT
`)

	m, err := LoadSymbols(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(m.Symbols) != 2 {
		t.Errorf("want 2 canonical symbols, got %d", len(m.Symbols))
	}
}

func TestLoadSymbols_FileMissing(t *testing.T) {
	_, err := LoadSymbols("/does/not/exist.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoadSymbols_MalformedYAML(t *testing.T) {
	path := writeTempSymbolsYAML(t, "this is: not: valid: yaml: [")
	_, err := LoadSymbols(path)
	if err == nil {
		t.Fatal("expected error for malformed yaml, got nil")
	}
}

func TestVenueSymbol(t *testing.T) {
	m := &SymbolMap{
		Symbols: map[string]map[string]string{
			"BTC-USD": {"coinbase": "BTC-USD", "binance": "BTCUSDT"},
			"ETH-USD": {"coinbase": "ETH-USD"},
		},
	}

	cases := []struct {
		name      string
		canonical string
		venue     string
		wantSym   string
		wantOk    bool
	}{
		{"btc on binance", "BTC-USD", "binance", "BTCUSDT", true},
		{"btc on coinbase", "BTC-USD", "coinbase", "BTC-USD", true},
		{"eth not on binance", "ETH-USD", "binance", "", false},
		{"unknown canonical", "DOGE-USD", "coinbase", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSym, gotOk := m.VenueSymbol(tc.canonical, tc.venue)
			if gotSym != tc.wantSym || gotOk != tc.wantOk {
				t.Errorf("VenueSymbol(%q, %q) = (%q, %v), want (%q, %v)",
					tc.canonical, tc.venue, gotSym, gotOk, tc.wantSym, tc.wantOk)
			}
		})
	}
}

func TestCanonicalSymbol(t *testing.T) {
	m := &SymbolMap{
		Symbols: map[string]map[string]string{
			"BTC-USD": {"binance": "BTCUSDT"},
			"ETH-USD": {"binance": "ETHUSDT"},
		},
	}

	cases := []struct {
		name          string
		venueSymbol   string
		venue         string
		wantCanonical string
		wantOk        bool
	}{
		{"btcusdt binance", "BTCUSDT", "binance", "BTC-USD", true},
		{"ethusdt binance", "ETHUSDT", "binance", "ETH-USD", true},
		{"unknown venue symbol", "DOGEUSDT", "binance", "", false},
		{"unknown venue", "BTCUSDT", "kraken", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCanonical, gotOk := m.CanonicalSymbol(tc.venueSymbol, tc.venue)
			if gotCanonical != tc.wantCanonical || gotOk != tc.wantOk {
				t.Errorf("CanonicalSymbol(%q, %q) = (%q, %v), want (%q, %v)",
					tc.venueSymbol, tc.venue, gotCanonical, gotOk, tc.wantCanonical, tc.wantOk)
			}
		})
	}
}

func TestCanonicalsForVenue(t *testing.T) {
	m := &SymbolMap{
		Symbols: map[string]map[string]string{
			"BTC-USD": {"coinbase": "BTC-USD", "binance": "BTCUSDT"},
			"ETH-USD": {"coinbase": "ETH-USD"},
			"SOL-USD": {"binance": "SOLUSDT"},
		},
	}

	binance := m.CanonicalsForVenue("binance")
	if len(binance) != 2 {
		t.Errorf("binance: want 2 symbols, got %d (%v)", len(binance), binance)
	}
	if !contains(binance, "BTC-USD") || !contains(binance, "SOL-USD") {
		t.Errorf("binance: want BTC-USD and SOL-USD, got %v", binance)
	}

	kraken := m.CanonicalsForVenue("kraken")
	if len(kraken) != 0 {
		t.Errorf("kraken: want empty, got %v", kraken)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}