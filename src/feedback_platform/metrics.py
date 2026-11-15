import time

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

HTTP_REQUESTS = Counter(
    "feedback_http_requests_total", "HTTP requests", ["service", "method", "route", "status"]
)
HTTP_DURATION = Histogram(
    "feedback_http_request_duration_seconds", "HTTP duration", ["service", "method", "route"]
)
PROCESSING = Histogram("feedback_processing_seconds", "Worker processing time")
PROCESSING_LAG = Histogram(
    "feedback_processing_lag_seconds",
    "Accepted-to-completed latency",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
)
WORKER_RESULTS = Counter("feedback_worker_results_total", "Worker results", ["outcome"])
OUTBOX_PENDING = Gauge("feedback_outbox_pending", "Unpublished durable outbox events")
CONSUMER_LAG = Gauge(
    "feedback_consumer_lag", "Broker high watermark minus committed offset", ["partition"]
)


def install_metrics(app: FastAPI, service: str):
    @app.middleware("http")
    async def measure(request, call_next):
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")
            if not path.startswith(("/metrics", "/health")):
                HTTP_REQUESTS.labels(service, request.method, path, str(status)).inc()
                HTTP_DURATION.labels(service, request.method, path).observe(
                    time.perf_counter() - start
                )

    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
