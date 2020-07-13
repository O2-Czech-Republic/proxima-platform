#Run in minikube

```bash
cd example
minikube -p proxima-example start
eval $(minikube -p proxima-example docker-env)
mvn clean package -Pwith-docker
kubectl apply -f server/deployment/services/
# create commitlog
./server/bin/kafka-topics.sh --create --topic proxima_events
kubectl apply -f server/deployment/
kubectl apply -f tools/deployment/proxima-console.yaml
```