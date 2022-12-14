set -e
version=v0.$(date +%s)
echo "Building with version $version"
docker build . -f Dockerfile_airflow_with_requirements -t data6s/airflow -t data6s/airflow:$version --build-arg root_password=$MONGODB_ROOT_PASSWORD_ENV
docker push data6s/airflow:$version
kubectl apply -f ./dags/svc.yaml
helm upgrade -n airflow --install airflow apache-airflow/airflow --set images.airflow.repository=data6s/airflow --set images.airflow.tag=$version
