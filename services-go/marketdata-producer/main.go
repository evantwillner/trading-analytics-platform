package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message published to Kafka.
type Tick struct {
	Type     string  `json:"type"`     // always "tick"
	Symbol   string  `json:"symbol"`   // e.g. AAPL
	TsUnixMs int64   `json:"tsUnixMs"` // milliseconds since epoch
	Price    float64 `json:"price"`    // price
	Size     int64   `json:"size"`     // trade size/volume
}

func main() {
	// ---- Configuration ----
	// In real systems these would come from env vars/config files.
	broker := "localhost:9092"
	topic := "ticks.v1"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // spreads messages across partitions (if topic has >1 partition)
	})
	defer writer.Close()

	// Context is Go’s way to pass cancellation/timeouts through calls.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for Ctrl-C (SIGINT) so it can stop cleanly.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fmt.Println("\nCtrl-C received. Shutting down producer...")
		cancel()
	}()

	symbols := []string{"AAPL", "MSFT", "TSLA", "NVDA", "AMZN"}

	fmt.Printf("Go Kafka producer started. Publishing to %s on %s\n", topic, broker)

	// Produce ~20 messages/sec (every 50ms).
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// ctx.Done() is closed when cancel() is called.
			fmt.Println("Producer stopped.")
			return

		case <-ticker.C:
			// Create random tick message
			symbol := symbols[rand.Intn(len(symbols))]

			t := Tick{
				Type:     "tick",
				Symbol:   symbol,
				TsUnixMs: time.Now().UnixMilli(),
				Price:    100 + rand.Float64()*50,
				Size:     int64(1 + rand.Intn(500)),
			}

			// Serialize struct -> JSON bytes
			bytes, err := json.Marshal(t)
			if err != nil {
				fmt.Println("json.Marshal error:", err)
				continue
			}

			// Kafka message. Key is used for partitioning.
			msg := kafka.Message{
				Key:   []byte(t.Symbol),
				Value: bytes,
			}

			// WriteMessages sends one or more messages.
			// ctx here allows cancellation if shutting down.
			if err := writer.WriteMessages(ctx, msg); err != nil {
				fmt.Println("kafka write error:", err)
				continue
			}

			// Optional: log occasionally (not every tick)
		}
	}
}
