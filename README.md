# Minikube Autoscale Demo

This repo contains a simple Kafka producer and consumer in Go, plus Kubernetes manifests for a lightweight single-broker Kafka cluster, HPA, and KEDA.

## What is included

- `producer/`: Go app producing dummy Kafka messages
- `consumer/`: Go app consuming messages from Kafka
- `Dockerfile.producer` / `Dockerfile.consumer`
- `k8s/`: Kubernetes resources for Kafka, producer/consumer deployments, HPA, and KEDA

## Prerequisites

- Minikube
- kubectl
- KEDA operator installed in Minikube

Install KEDA:

```bash
kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.11.0/keda-2.11.0.yaml
```

## Build images and load into Minikube

```bash
eval "$(minikube docker-env)"
docker build -t producer:latest -f Dockerfile.producer .
docker build -t consumer:latest -f Dockerfile.consumer .
```

Or use `minikube image load` if you prefer:

```bash
docker build -t producer:latest -f Dockerfile.producer .
docker build -t consumer:latest -f Dockerfile.consumer .
minikube image load producer:latest
minikube image load consumer:latest
```

## Deploy to Minikube

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka-zookeeper.yaml
kubectl apply -f k8s/kafka-broker.yaml
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/consumer-deployment.yaml
kubectl apply -f k8s/producer-hpa.yaml
kubectl apply -f k8s/keda-scaledobject.yaml
```

## Observe scaling

- `producer` has an HPA on CPU usage.
- `consumer` is controlled by KEDA using Kafka lag.

Use:

```bash
kubectl get pods -n autoscale-demo
kubectl get hpa -n autoscale-demo
kubectl get scaledobject -n autoscale-demo
```

## Testing and tuning

The producer can increase CPU pressure using the `WORK_MS` environment variable in `k8s/producer-deployment.yaml`.
Set `WORK_MS` to `100` or higher to drive CPU-based HPA scaling.

The consumer can simulate slower processing using `SLEEP_MS` in `k8s/consumer-deployment.yaml`.
Set `SLEEP_MS` to `300` or higher to increase lag and observe KEDA scaling.

The consumer will scale based on Kafka lag threshold if messages accumulate faster than they are consumed.
# autoscaler-demo
