"""Authenticated HTTP inference and optional KServe V2 protocol endpoints."""

from __future__ import annotations

import secrets
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException
from prometheus_client import Histogram
from pydantic import BaseModel, Field, field_validator

from feedback_platform.metrics import install_metrics
from feedback_platform.ml import EMBEDDING_DIM, EMBEDDING_VERSION, get_registry
from feedback_platform.settings import get_settings

INFERENCE_DURATION = Histogram(
    "feedback_inference_duration_seconds",
    "Time spent on classification and local embedding inference.",
    ["model_version"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)


def require_service_token(x_service_token: Annotated[str | None, Header()] = None) -> None:
    expected = get_settings().internal_api_key
    if not expected or not secrets.compare_digest(x_service_token or "", expected):
        raise HTTPException(status_code=401, detail="Invalid service token")


class TextRequest(BaseModel):
    text: str = Field(min_length=1, max_length=10000)

    @field_validator("text")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Text must contain non-whitespace characters")
        return value.strip()


class PredictionRequest(TextRequest):
    model_version: str | None = Field(default=None, max_length=64)


class PredictionResponse(BaseModel):
    sentiment: Literal["positive", "neutral", "negative"]
    confidence: float = Field(ge=0, le=1)
    topics: list[str]
    model_version: str
    embedding: list[float] = Field(min_length=EMBEDDING_DIM, max_length=EMBEDDING_DIM)
    embedding_version: str
    inference_ms: float = Field(ge=0)


@asynccontextmanager
async def lifespan(application: FastAPI):
    # A missing/corrupt corpus or an unknown default version fails startup,
    # allowing Kubernetes to keep the previous healthy replicas serving.
    registry = get_registry()
    registry.require_version(get_settings().model_version)
    application.state.registry = registry
    yield


app = FastAPI(title="Feedback inference service", version="1.0.0", lifespan=lifespan)
install_metrics(app, "inference")
secured = [Depends(require_service_token)]


def predict_text(text: str, version: str | None) -> dict:
    version = version or get_settings().model_version
    registry = get_registry()
    try:
        registry.require_version(version)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    with INFERENCE_DURATION.labels(version).time():
        return registry.predict(text, version)


@app.get("/health/live")
def live() -> dict:
    return {"status": "alive"}


@app.get("/health/ready")
def ready() -> dict:
    try:
        get_registry().require_version(get_settings().model_version)
    except (ValueError, OSError) as exc:
        raise HTTPException(status_code=503, detail="Model unavailable") from exc
    return {"status": "ready", "model_version": get_settings().model_version}


@app.post("/predict", response_model=PredictionResponse, dependencies=secured)
def predict(body: PredictionRequest) -> dict:
    return predict_text(body.text, body.model_version)


@app.post("/embed", dependencies=secured)
def embed(body: TextRequest) -> dict:
    return {"embedding": get_registry().embed(body.text), "embedding_version": EMBEDDING_VERSION}


@app.get("/models", dependencies=secured)
def models() -> dict:
    return {"active_version": get_settings().model_version, **get_registry().metadata()}


class V2Input(BaseModel):
    name: Literal["text"]
    shape: list[int]
    datatype: Literal["BYTES"]
    data: list[str] = Field(min_length=1, max_length=32)

    @field_validator("data")
    @classmethod
    def valid_texts(cls, values: list[str]) -> list[str]:
        for value in values:
            TextRequest(text=value)
        return values


class V2Request(BaseModel):
    id: str | None = Field(default=None, max_length=128)
    inputs: list[V2Input] = Field(min_length=1, max_length=1)


@app.get("/v2/health/live")
def v2_live() -> dict:
    return live()


@app.get("/v2/health/ready")
def v2_ready() -> dict:
    return ready()


@app.get("/v2/models/{model_name}/ready")
def v2_model_ready(model_name: str) -> dict:
    if model_name not in get_registry().models:
        raise HTTPException(status_code=404, detail="Unknown model")
    return {"ready": True}


@app.post("/v2/models/{model_name}/infer", dependencies=secured)
def v2_infer(model_name: str, body: V2Request) -> dict:
    tensor = body.inputs[0]
    if tensor.shape != [len(tensor.data)]:
        raise HTTPException(status_code=422, detail="Expected text shape [batch_size]")
    results = [predict_text(text, model_name) for text in tensor.data]
    output = {
        "model_name": model_name,
        "model_version": model_name,
        "outputs": [
            {
                "name": "sentiment",
                "shape": [len(results)],
                "datatype": "BYTES",
                "data": [row["sentiment"] for row in results],
            },
            {
                "name": "confidence",
                "shape": [len(results)],
                "datatype": "FP64",
                "data": [row["confidence"] for row in results],
            },
            {
                "name": "embedding",
                "shape": [len(results), EMBEDDING_DIM],
                "datatype": "FP64",
                "data": [value for row in results for value in row["embedding"]],
            },
        ],
    }
    if body.id is not None:
        output["id"] = body.id
    return output
