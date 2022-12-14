set -e
version=v0.$(date +%s)
echo "Building with version $version"
docker build . -f Dockerfile_spark_local -t data6s/spark_local:latest -t data6s/spark_local:$version --build-arg root_password=$MONGODB_ROOT_PASSWORD_ENV
docker push data6s/spark_local:latest
kubectl replace --force -f ./spark-local/deployment.yaml
