# Performance evidence

Performance numbers must come from a completed run, with its raw observations retained.
The repository includes a live benchmark runner and an SVG renderer; this document does
not substitute synthetic estimates for measurements. Until an evidence file is committed,
that scenario is **not measured**.

## Run and preserve the evidence

From the repository root with the Compose stack healthy:

```sh
python scripts/benchmark.py --count 300 --concurrency 20 --warmup 5 --output docs/evidence/load-1worker
docker compose up -d --scale worker=3
python scripts/benchmark.py --count 300 --concurrency 20 --warmup 5 --output docs/evidence/load-3workers
docker compose up -d --scale worker=1
```

Both runs write JSON and a standalone SVG. The JSON records the Git commit, dirty working
tree flag, host and Python details, observed container images and replicas, workload,
warmup, all request outcomes, HTTP latency, completion latency, model inference duration,
and Docker CPU/memory samples. Commands never delete previous feedback or database volumes.
Run the same load against an otherwise idle machine and disclose any simultaneous tests.

## Interpret the measurements

| Observation | Definition and limits |
| --- | --- |
| Accepted requests/s | HTTP 202 responses divided by the ingestion window; measures acceptance into PostgreSQL/outbox, not completed ML work. |
| Completed items/s | Completed items divided by the full run window; includes ingestion and completion polling. |
| Ingestion p50/p95/p99 | Successful request latency after the concurrency gate; failed requests remain in raw evidence and error counts. |
| End-to-end p50/p95/p99 | Time from HTTP send until the client first observes a completed prediction. Polling makes this an upper bound. |
| Inference p50/p95/p99 | Worker-recorded model duration returned by the inference service; does not include Kafka waiting or HTTP transport. |
| CPU / memory | Raw `docker stats --no-stream` samples approximately every two seconds, including collector overhead. Docker CPU can exceed 100% across cores. |
| Error rate | Failed ingestion plus processing failures and unfinished items are reported separately. A runner failure must not be discarded. |

Use the adjacent SVG to compare the observed latency distributions. For real-time
throughput, p95 HTTP latency, errors, outbox depth, Kafka consumer lag, and processing
latency, open the provisioned Grafana dashboard. Export an actual dashboard screenshot
after a run if presenting it; do not illustrate success with invented metrics.

## Bottlenecks and improvements

The architecture has a single-broker development Kafka cluster, one PostgreSQL instance,
a small CPU model, and an intentionally simple outbox publisher. Its capacity is not a
production sizing claim. Candidate bottlenecks are database connections, inference CPU,
Kafka partition count, worker count, and outbox publish rate. These are hypotheses until
the load observations, CPU profiles, or traces support them.

A useful experiment changes only one factor, repeats the baseline, and compares both
throughput and tails. For example, use the commands above to compare one versus three
workers while keeping the inference replica count unchanged. Extra workers may produce
no improvement if inference is saturated. Record that result rather than promising
linear scaling. Commit an evidence summary with the exact run filenames, measured
numbers, workload differences, errors, and limitations.

## Reliability evidence

```sh
python scripts/reliability.py --output docs/evidence/reliability-local.json
```

This separately measures worker termination, Kafka outage, database outage, an unhealthy
inference deployment and rollback, and model-version rollback. The runner records the
actual commands, elapsed times, HTTP observations, feedback IDs, final database counts,
and duplicate-prediction audit. An unexecuted scenario remains **not tested**. See the
[incident runbook](incident-runbook.md) for operating limits and recovery commands.
