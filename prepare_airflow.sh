set -e
version=v0.$(date +%s)
echo "Building with version $version"
docker build . -f Dockerfile_airflow_with_spark -t data6s/airflow_with_spark -t data6s/airflow:latest --build-arg root_password=$MONGODB_ROOT_PASSWORD_ENV
docker push data6s/airflow_with_spark:latest
