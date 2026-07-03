package main

import (
	"context"
	"fmt"
	"log"
	"minikube-autoscale/telemetry"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	otellog "go.opentelemetry.io/otel/log"

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

	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Printf("OTEL INTERNAL ERROR: %v", err)
	}))

	fmt.Println("THIS THING IS NOW STARTING UP AND WILL LOG TO OTEL COLLECTOR")

	ctx := context.Background()
	logmaker, err := telemetry.InitLogger(ctx, "CONSUMER")
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
		panic(err)
	}
	defer logmaker.Shutdown(ctx)

	plog := logmaker.Logger("processor")
	notice := fmt.Sprintf("consumer starting: broker=%s topic=%s group=%s sleepMs=%d", broker, topic, groupID, sleepMs)
	startrec := otellog.Record{}
	startrec.SetEventName("consumer_start")
	startrec.SetBody(otellog.StringValue(notice))
	startrec.SetSeverity(otellog.SeverityInfo)
	startrec.AddAttributes(
		otellog.String("config.broker", broker),
		otellog.String("config.topic", topic),
		otellog.String("config.group_id", groupID),
		otellog.Int("config.sleep_ms", sleepMs),
	)
	plog.Emit(ctx, startrec)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			//log.Printf("read error: %v", err)\
			errRec := otellog.Record{}
			errRec.SetBody(otellog.StringValue("Failed to read message from Kafka broker"))
			errRec.SetSeverity(otellog.SeverityError)
			errRec.AddAttributes(otellog.String("error.message", err.Error()))
			plog.Emit(ctx, errRec)

			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("consumed partition=%d offset=%d value=%s", msg.Partition, msg.Offset, string(msg.Value))
		rec := otellog.Record{}
		rec.SetEventName("consumer_processed")
		rec.SetSeverity(otellog.SeverityInfo)
		rec.AddAttributes(
			otellog.KeyValue{
				Key:   "processed_value",
				Value: otellog.StringValue(string(msg.Value)),
			}, otellog.KeyValue{
				Key:   "k8s.pod.name",
				Value: otellog.StringValue(getenv("K8S_POD_NAME", "Consumer")),
			}, otellog.KeyValue{
				Key:   "k8s.namespace.name",
				Value: otellog.StringValue(getenv("K8S_NAMESPACE", "autoscale-demo")),
			},
		)
		rec.SetBody(otellog.StringValue("Successfully processed message block"))
		rec.SetSeverity(otellog.SeverityInfo)
		rec.AddAttributes(
			otellog.Int("kafka.partition", msg.Partition),
			otellog.Int64("kafka.offset", msg.Offset),
			otellog.String("kafka.message_payload", string(msg.Value)),
		)
		plog.Emit(ctx, rec)

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
