set -e
version=v0.$(date +%s)
echo "Building with version $version"
docker build . -f Dockerfile_mlflow_experiment -t data6s/mlflow-experiment:latest -t data6s/mlflow-experiment:$version
docker push data6s/mlflow-experiment:latest
kubectl replace --force -f ./mlflow-experiment/deployment.yaml
