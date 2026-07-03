package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func InitLogger(ctx context.Context, serviceName string) (*log.LoggerProvider, error) {
	// 1. Configure the OTLP gRPC exporter directing traffic cross-namespace
	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithInsecure(),
		otlploghttp.WithEndpoint("otel-collector.monitoring.svc.cluster.local:4317"),
	)
	if err != nil {
		return nil, err
	}

	// Inside your InitLogger configuration:
	podName := os.Getenv("K8S_POD_NAME")
	if podName == "" {
		podName = "local-development" // Fallback fallback for running outside minikube
	}

	namespace := os.Getenv("K8S_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			semconv.K8SPodNameKey.String(podName),         // Binds k8s.pod.name to all logs
			semconv.K8SNamespaceNameKey.String(namespace), // Binds k8s.namespace.name to all logs
		),
	)
	if err != nil {
		return nil, err
	}

	// 3. Create a processor that batches records efficiently before pushing
	// processor := log.NewBatchProcessor(
	// 	exporter,
	// 	log.WithExportInterval(1*time.Second), // Correct Option for Logs SDK
	// 	log.WithExportMaxBatchSize(512),
	// )
	processor := log.NewSimpleProcessor(exporter)

	// 4. Instantiate the centralized Provider
	provider := log.NewLoggerProvider(
		log.WithProcessor(processor),
		log.WithResource(res),
	)

	return provider, nil
}
