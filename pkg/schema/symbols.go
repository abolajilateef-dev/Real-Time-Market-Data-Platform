package schema

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SymbolMap holds canonical → per-venue symbol mappings loaded from YAML.
type SymbolMap struct {
	Symbols map[string]map[string]string `yaml:"symbols"`
}

// LoadSymbols reads and parses a symbols YAML file.
func LoadSymbols(path string) (*SymbolMap, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read symbols file: %w", err)
	}
	var m SymbolMap
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse symbols yaml: %w", err)
	}
	return &m, nil
}

// VenueSymbol returns the exchange-native symbol for a canonical symbol on a venue.
func (m *SymbolMap) VenueSymbol(canonical, venue string) (string, bool) {
	venues, ok := m.Symbols[canonical]
	if !ok {
		return "", false
	}
	s, ok := venues[venue]
	return s, ok
}

// CanonicalSymbol reverse-looks up the canonical symbol for a venue-native one.
// Example: CanonicalSymbol("BTCUSDT", "binance") -> "BTC-USD", true.
func (m *SymbolMap) CanonicalSymbol(venueSymbol, venue string) (string, bool) {
	for canonical, venues := range m.Symbols {
		if venues[venue] == venueSymbol {
			return canonical, true
		}
	}
	return "", false
}

// CanonicalsForVenue lists all canonical symbols that have a mapping on the given venue.
// Used by feed handlers to know which symbols to subscribe to.
func (m *SymbolMap) CanonicalsForVenue(venue string) []string {
	var out []string
	for canonical, venues := range m.Symbols {
		if _, ok := venues[venue]; ok {
			out = append(out, canonical)
		}
	}
	return out
}