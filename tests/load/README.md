# Reproducible load experiments

Start the Compose stack and run the same benchmark twice with different worker counts:

```sh
python scripts/benchmark.py --count 300 --concurrency 20 --output docs/evidence/load-1worker
docker compose up -d --scale worker=3
python scripts/benchmark.py --count 300 --concurrency 20 --output docs/evidence/load-3workers
```

The script writes JSON containing the raw per-request observations, ingestion latency
percentiles, accepted throughput, end-to-end completion times, inference times, failures,
and sampled Docker CPU/memory usage. It also renders a standalone SVG from those observations.
All generated text is derived from the completed run. A nonzero exit code means an HTTP
failure, processing failure, or unfinished item was observed; keep that evidence.

Ingestion latency measures the API response, not the asynchronous pipeline. End-to-end
latency includes client queueing after the concurrency gate and polling interval, so it is
an upper-bound observation of completion. The script reports successful ingestion rate
and completed pipeline rate separately. CPU percent is Docker's sampled cgroup metric;
it can exceed 100% across cores. The script also records worker replicas and host details.

The benchmark creates real feedback. It never deletes records or resets volumes. Use a
fresh Compose project if you need an isolated dataset. Use the same dataset size,
concurrency, host, image, database history, and warmup policy for a defensible comparison.
Latency samples are not independent and this small synthetic corpus is not a production
capacity estimate. Inspect Prometheus/Grafana and the captured CPU/memory time series
before identifying a bottleneck.
