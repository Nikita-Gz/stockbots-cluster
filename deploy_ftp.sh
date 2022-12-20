kubectl apply -f ./ftp-server/pv.yaml
kubectl apply -f ./ftp-server/pvc.yaml
kubectl apply -f ./ftp-server/deployment.yaml
kubectl apply -f ./ftp-server/service.yaml
kubectl apply -f ./ftp-server/ingress.yaml
