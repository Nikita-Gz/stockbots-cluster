set -e
version=v0.$(date +%s)
echo "Building with version $version"
docker build . -f Dockerfile_mlflow_server -t data6s/mlflow-server -t data6s/mlflow-server:$version
docker push data6s/mlflow-server:$version
docker push data6s/mlflow-server:latest
kubectl apply -f ./mlflow-tracking/storage.yaml
kubectl apply -f ./mlflow-tracking/mlflow-config.yaml
kubectl apply -f ./mlflow-tracking/service.yaml
kubectl replace -f ./mlflow-tracking/deployment.yaml --force
