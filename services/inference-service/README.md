# Inference service

Run `uvicorn feedback_platform.inference:app --host 0.0.0.0 --port 8000` in the
service container. The Compose configuration selects the appropriate host port.

`POST /predict` accepts `{"text":"The mobile app keeps crashing","model_version":"v1"}`.
It returns sentiment, probability, a topic list, classifier version, the
128-dimensional embedding, embedding version, and measured inference time.
`model_version` can be omitted to use the deployment's `MODEL_VERSION`.

`POST /embed` accepts `{"text":"mobile application complaints"}` and returns
an embedding in the same space as stored predictions. `GET /models` lists the
available versions, active default, corpus hash, and limitations. All three
routes require the internal `X-Service-Token` header. End-user tenant API keys
are not accepted here. Do not expose this service publicly.

`/health/live` checks process liveness; `/health/ready` requires a valid default
model. Startup fails if the default model or corpus is invalid. Prometheus
scrapes `/metrics`, including `feedback_inference_duration_seconds` by version.

The optional KServe-compatible V2 routes include `/v2/health/live`,
`/v2/health/ready`, `/v2/models/v1/ready`, and `POST /v2/models/v1/infer`:

```json
{
  "id": "example-1",
  "inputs": [{"name": "text", "shape": [1], "datatype": "BYTES",
              "data": ["The mobile app keeps crashing"]}]
}
```

V2 inference accepts up to 32 strings and returns sentiment, confidence, and
embedding output tensors. It uses the same internal token. This is a custom
predictor protocol adapter, not a claim that a KServe cluster was deployed.

For rollout, deploy the new image with `MODEL_VERSION=v2`, check readiness,
compare held-out predictions and latency, then direct new events to that
version. Keep `v1` available for events already pinned to it. A rollback changes
the default/new-event version; it does not rewrite historical predictions.
