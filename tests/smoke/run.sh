kubectl delete job omni-runtime-echo-smoke --ignore-not-found
kubectl apply -f echo-smoke-job.yaml
kubectl wait --for=condition=complete job/omni-runtime-echo-smoke --timeout=120s
kubectl logs -l job-name=omni-runtime-echo-smoke
