package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := getenv("KAFKA_BROKER", "kafka:9092")
	topic := getenv("TOPIC", "demo-topic")
	intervalMs := getenvInt("INTERVAL_MS", 300)
	workMs := getenvInt("WORK_MS", 0)

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	ctx := context.Background()
	log.Printf("producer starting: broker=%s topic=%s interval=%d workMs=%d", broker, topic, intervalMs, workMs)

	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	count := 0
	for range ticker.C {
		count++
		message := fmt.Sprintf("dummy-message-%d %s", count, time.Now().UTC().Format(time.RFC3339Nano))
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", count)),
			Value: []byte(message),
		}

		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Printf("failed to write message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if workMs > 0 {
			burnCPU(workMs)
		}

		log.Printf("produced message %d", count)
	}
}

func burnCPU(ms int) {
	deadline := time.Now().Add(time.Duration(ms) * time.Millisecond)
	var x float64
	for time.Now().Before(deadline) {
		x += 3.14159 / 1.61803
	}
	_ = x
}

func getenv(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func getenvInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
