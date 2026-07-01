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
	groupID := getenv("GROUP_ID", "demo-group")
	minBytes := getenvInt("MIN_BYTES", 1)
	maxBytes := getenvInt("MAX_BYTES", 10000000)
	sleepMs := getenvInt("SLEEP_MS", 300)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	ctx := context.Background()
	// logmaker, err := telemetry.InitLogger(ctx, "CONSUMER")
	// if err != nil {
	// 	log.Fatalf("failed to initialize logger: %v", err)
	// }
	// defer logmaker.Shutdown(ctx)

	// plog := logmaker.Logger("processor")
	notice := fmt.Sprintf("consumer starting: broker=%s topic=%s group=%s sleepMs=%d\n", broker, topic, groupID, sleepMs)
	log.Printf(notice)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("read error: %v", err)

			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("consumed partition=%d offset=%d value=%s", msg.Partition, msg.Offset, string(msg.Value))
		if sleepMs > 0 {
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
	}
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
