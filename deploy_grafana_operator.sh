#helm repo add grafana https://grafana.github.io/helm-charts
#helm repo update
#helm install grafana-operator grafana/grafana-agent-operator

#kubectl apply -f ./grafana/part1.yaml

kubectl apply -f ./grafana/crds
kubectl apply -f ./grafana/agent.yaml
kubectl apply -f ./grafana/crds2/agent.yaml
kubectl apply -f ./grafana/crds2/metrics-instance.yaml
kubectl apply -f ./grafana/crds2/
