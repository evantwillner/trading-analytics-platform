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

type Tick struct {
	Type     string  `json:"type"`
	Symbol   string  `json:"symbol"`
	TsUnixMs int64   `json:"tsUnixMs"`
	Price    float64 `json:"price"`
	Size     int64   `json:"size"`
}

func main() {
	broker := "localhost:9092"
	topic := "ticks.v1"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		fmt.Println("\nCtrl-C received. Shutting down producer...")
		cancel()
	}()

	symbols := []string{"AAPL", "MSFT", "TSLA", "NVDA", "AMZN"}

	lastPrice := map[string]float64{
		"AAPL": 182.0,
		"MSFT": 415.0,
		"TSLA": 245.0,
		"NVDA": 875.0,
		"AMZN": 178.0,
	}

	fmt.Printf("Go Kafka producer started. Publishing to %s on %s\n", topic, broker)

	// Emit one tick per symbol every 200ms — clean synchronized feed
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Producer stopped.")
			return
		case <-ticker.C:
			for _, symbol := range symbols {
				prev := lastPrice[symbol]
				change := prev * (rand.Float64()*0.006 - 0.003)
				newPrice := prev + change
				lastPrice[symbol] = newPrice

				t := Tick{
					Type:     "tick",
					Symbol:   symbol,
					TsUnixMs: time.Now().UnixMilli(),
					Price:    newPrice,
					Size:     int64(1 + rand.Intn(500)),
				}

				bytes, err := json.Marshal(t)
				if err != nil {
					fmt.Println("json.Marshal error:", err)
					continue
				}

				msg := kafka.Message{
					Key:   []byte(t.Symbol),
					Value: bytes,
				}

				if err := writer.WriteMessages(ctx, msg); err != nil {
					fmt.Println("kafka write error:", err)
				}
			}
		}
	}
}
