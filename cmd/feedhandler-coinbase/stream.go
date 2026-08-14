package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"

	"github.com/abolajilateef-dev/mdp/internal/kafka"
	"github.com/abolajilateef-dev/mdp/pkg/schema"
)

const (
	coinbaseWSURL = "wss://ws-feed.exchange.coinbase.com"

	initialBackoff = 1 * time.Second
	maxBackoff     = 60 * time.Second

	writeTimeout = 10 * time.Second
	readTimeout  = 60 * time.Second
)

// subscribeMessage is what we send to Coinbase right after connecting.
type subscribeMessage struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

// envelope is the minimum shape of any Coinbase message: we peek at the type
// field first, then dispatch to a specific handler for the full parse.
type envelope struct {
	Type string `json:"type"`
}

// stream runs forever: connect, stream, reconnect on failure. Returns only when
// ctx is cancelled.
func stream(
	ctx context.Context,
	log *slog.Logger,
	producer *kafka.Producer,
	symbols *schema.SymbolMap,
) {
	backoff := initialBackoff

	for {
		if ctx.Err() != nil {
			return
		}

		productIDs := coinbaseProductIDs(symbols)
		if len(productIDs) == 0 {
			log.Error("no coinbase symbols configured; nothing to subscribe to")
			return
		}

		err := connectAndStream(ctx, log, producer, symbols, productIDs)
		if ctx.Err() != nil {
			return
		}
		log.Warn("stream ended, will reconnect", "err", err, "backoff", backoff)
		reconnects.WithLabelValues("coinbase").Inc()

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// coinbaseProductIDs returns the venue-native product IDs for every symbol
// configured on Coinbase.
func coinbaseProductIDs(symbols *schema.SymbolMap) []string {
	canonicals := symbols.CanonicalsForVenue("coinbase")
	out := make([]string, 0, len(canonicals))
	for _, c := range canonicals {
		if venueSym, ok := symbols.VenueSymbol(c, "coinbase"); ok {
			out = append(out, venueSym)
		}
	}
	return out
}

// connectAndStream runs one full connection: dial, subscribe, read until error.
// A nil return means the read loop ended cleanly (only happens on ctx cancel).
func connectAndStream(
	ctx context.Context,
	log *slog.Logger,
	producer *kafka.Producer,
	symbols *schema.SymbolMap,
	productIDs []string,
) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}

	conn, _, err := dialer.DialContext(ctx, coinbaseWSURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	log.Info("connected", "url", coinbaseWSURL, "products", productIDs)

	if err := subscribe(conn, productIDs); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Info("subscribed", "channels", []string{"matches", "level2_batch"})

	tracker := newSequenceTracker()
	return readLoop(ctx, log, producer, symbols, tracker, conn)
}

// subscribe sends the initial subscription request to Coinbase.
func subscribe(conn *websocket.Conn, productIDs []string) error {
	msg := subscribeMessage{
		Type:       "subscribe",
		ProductIDs: productIDs,
		Channels: []string{"matches", "level2_batch"},
	}

	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return conn.WriteJSON(msg)
}

// readLoop reads messages until the connection dies or ctx is cancelled.
// Pass 1: just log the message type. Pass 2 replaces this with real handlers.
func readLoop(
	ctx context.Context,
	log *slog.Logger,
	producer *kafka.Producer,
	symbols *schema.SymbolMap,
	tracker *sequenceTracker,
	conn *websocket.Conn,
) error {
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read: %w", err)
		}

		var env envelope
		if err := json.Unmarshal(data, &env); err != nil {
			log.Warn("bad json from coinbase", "err", err)
			continue
		}

		switch env.Type {
		case "subscriptions":
			log.Info("subscription confirmed")
		case "match":
			handleMatch(ctx, log, producer, symbols, data)
		case "snapshot":
			handleSnapshot(ctx, log, producer, symbols, tracker, data)
		case "l2update":
			handleL2Update(ctx, log, producer, symbols, tracker, data)
		case "error":
			log.Error("coinbase error message", "raw", string(data))
		default:
			log.Debug("received message", "type", env.Type)
		}
	}
}