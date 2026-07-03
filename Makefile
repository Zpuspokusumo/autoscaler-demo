cleanupdeployment:
	@echo "Cleaning up deployment..."
	kubectl delete scaledobject consumer-kafka-scaler -n autoscale-demo --ignore-not-found=true
	kubectl delete job kafka-topic-creator -n autoscale-demo --ignore-not-found=true
	kubectl delete deployment --all -n autoscale-demo --ignore-not-found=true

applydeployment:
	@echo "Cleaning up old transient job tracking records..."
	# Wiping the job history forces Kubernetes to execute it fresh
	-kubectl delete job kafka-topic-creator -n autoscale-demo 2>/dev/null || true
	
	@echo "Applying base telemetry infrastructure..."
	kubectl apply -f k8s/otel-collector.yaml

	@echo "Applying base infrastructure..."
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/kafka-kraft.yaml
	
	@echo "Waiting for Kafka broker to be ready..."
	kubectl wait --for=condition=available --timeout=60s deployment/kafka -n autoscale-demo
	
	@echo "Executing initialization logic..."
	kubectl apply -f k8s/kafka-init-job.yaml

	@echo "BLOCKING: Waiting for init job to finish successfully..."
	kubectl wait --for=condition=complete --timeout=60s job/kafka-topic-creator -n autoscale-demo
	
	@echo "Applying application suite..."
	kubectl apply -f k8s/producer-deployment.yaml
	kubectl apply -f k8s/consumer-deployment.yaml
	kubectl apply -f k8s/consumer-scaledobject.yaml

compileall:
	@echo "Compiling producer and consumer..."
	docker build -t producer:latest -f Dockerfile.producer .
	docker build -t consumer:latest -f Dockerfile.consumer . 

pauseapps:
	@echo "Pausing applications..."
	kubectl scale deployment producer --replicas=0 -n autoscale-demo
	kubectl scale deployment consumer --replicas=0 -n autoscale-demo	
	kubectl scale deployment kafka --replicas=0 -n autoscale-demo

envup:
	@echo "Bringing up environment..."
	eval "$(minikube docker-env)"

envdown:
	@echo "Bringing down environment..."
	eval "$(minikube docker-env -u)"

checkenv:
	echo $MINIKUBE_ACTIVE_DOCKERD

watchhpa:
	@echo "Watching hpa..."
	kubectl get hpa -n autoscale-demo -w

watchpods:
	@echo "Watching pods..."
	kubectl get pods -n autoscale-demo -w

floodkafka:
	@echo "1. Pausing KEDA Autoscaler operator to simulate an orchestrator blindspot..."
	# KEDA usually installs into the 'keda' namespace by default. 
	# If you installed it elsewhere, change the -n flag accordingly.
	kubectl scale deployment keda-operator -n keda --replicas=0

	@echo "2. Initializing complete consumer blackout..."
	kubectl scale deployment consumer -n autoscale-demo --replicas=1

	@echo "3. Scaling up producers to 10 instances to flood the queue..."
	kubectl scale deployment producer -n autoscale-demo --replicas=10

	@echo "Simulating 60 seconds of silent queue accumulation..."
	sleep 60

	@echo "4. Throttling producers back down to 1 baseline instance..."
	kubectl scale deployment producer -n autoscale-demo --replicas=1

	@echo "5. Restoring KEDA Operator. Unleashing the autoscaler..."
	kubectl scale deployment keda-operator -n keda --replicas=1

	@echo "Blackout test sequence completed. Watch your pod count spike!"


checkkafka:
	@echo "Checking Kafka topic status..."
	kubectl exec -it deployment/kafka -n autoscale-demo -- \
	/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
	--describe --group demo-group

checkkedametric:
	kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/autoscale-demo/s0-kafka-demo-topic?labelSelector=scaledobject.keda.sh%2Fname%3Dconsumer-kafka-scaler"

checkscaledobject_kafkascaler:
	kubectl get scaledobject consumer-kafka-scaler -n autoscale-demo -o yaml

watchotel:
	@echo "Streaming raw OpenTelemetry log collector payload..."
	kubectl logs -f deployment/otel-collector -n monitoring

watchlogsconsumer:
	@echo "Streaming consumer logs..."
	kubectl logs -f -l app=consumer -n autoscale-demo 
	
portfwdmonitoring:
	@echo "Port forwarding OpenTelemetry collector to localhost:4317..."
	kubectl port-forward svc/otel-collector 4318:4318 -n monitoring

curlmonitoring:
	@echo "Curling OpenTelemetry collector endpoint..."
	curl -i -X POST http://localhost:4318/v1/logs \
  -H "Content-Type: application/json" \
  -d '{"resourceLogs":[{"resource":{},"scopeLogs":[{"logRecords":[{"body":{"stringValue":"Test message from local machine"}}]}]}]}'