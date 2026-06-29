
cleanupdeployment:
	@echo "Cleaning up deployment..."
kubectl delete deployment kafka -n autoscale-demo --ignore-not-found=true

applydeployment:
	@echo "Applying deployment..."
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/kafka-kraft.yaml
	sleep 8
	@echo "Applying Kafka init job..."
	kubectl apply -f k8s/kafka-init-job.yaml
	kubectl apply -f k8s/consumer-deployment.yaml
	kubectl apply -f k8s/consumer-scaledobject.yaml
	kubectl apply -f k8s/

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

watchpods:
	@echo "Watching pods..."
	kubectl get hpa -n autoscale-demo -w