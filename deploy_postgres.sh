kubectl apply -f ./postgresql/secret.yaml
kubectl apply -f ./postgresql/pv.yaml
kubectl apply -f ./postgresql/pv-claim.yaml
kubectl apply -f ./postgresql/postgres-service.yaml
kubectl apply -f ./postgresql/postgres-deployment.yaml
