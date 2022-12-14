set -e
version=v0.$(date +%s)
echo "Building with version $version"
docker build . -f Dockerfile_rt_price_fetcher -t data6s/realtime-price-fetcher:latest -t data6s/realtime-price-fetcher:$version --build-arg root_password=$MONGODB_ROOT_PASSWORD_ENV
docker push data6s/realtime-price-fetcher:latest
kubectl replace --force -f ./realtime-price-fetcher/deployment.yaml
