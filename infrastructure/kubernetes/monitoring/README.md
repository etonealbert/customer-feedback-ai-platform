# Namespace-local monitoring

After deploying the base, run:

```sh
python infrastructure/kubernetes/monitoring/deploy.py
kubectl -n feedback-ai rollout status deployment/prometheus
kubectl -n feedback-ai rollout status deployment/grafana
kubectl -n feedback-ai port-forward service/grafana 3000:3000
```

Log in as `admin` with `GRAFANA_PASSWORD` from your local `.env`. The script constructs ConfigMaps from the canonical `monitoring/` files so dashboards cannot drift between Compose and Kubernetes. Run it again after editing those files, then restart the Prometheus deployment to reload its configuration.

This lightweight deployment uses `emptyDir` storage: metrics history and Grafana UI edits are lost when pods are replaced. The provisioned dashboard is retained in source control. Use PVCs and a cluster monitoring operator for persistent shared environments.

Prometheus discovers and scrapes each annotated application pod directly, so scaling replicas does not mix unrelated counters behind a Service. Its ServiceAccount has read-only pod discovery permissions limited to the `feedback-ai` namespace. Compose uses Docker DNS discovery for the same reason. A cluster Prometheus can use the standalone discovery example in `monitoring/prometheus/kubernetes-scrape.yml`.
