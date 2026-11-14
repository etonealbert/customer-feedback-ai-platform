import json
import math
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FeedbackIn(StrictModel):
    text: str = Field(min_length=1, max_length=10000)
    source: str = Field(default="api", min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9_.-]+$")
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("text")
    @classmethod
    def nonblank(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("Text must not be blank")
        return value

    @field_validator("metadata")
    @classmethod
    def small_metadata(cls, value):
        if len(json.dumps(value, allow_nan=False).encode()) > 16384:
            raise ValueError("Metadata exceeds 16 KiB")
        return value


class Event(StrictModel):
    event_id: UUID
    feedback_id: UUID
    tenant_id: UUID
    model_version: Literal["v1", "v2"]


class Prediction(StrictModel):
    sentiment: Literal["positive", "negative", "neutral"]
    confidence: float = Field(ge=0, le=1)
    topics: list[str] = Field(min_length=1, max_length=10)
    model_version: Literal["v1", "v2"]
    embedding: list[float] = Field(min_length=128, max_length=128)
    embedding_version: str
    inference_ms: float = Field(ge=0)

    @field_validator("embedding")
    @classmethod
    def finite(cls, value):
        if not all(math.isfinite(x) for x in value):
            raise ValueError("Embedding must contain finite numbers")
        return value


class SearchIn(StrictModel):
    query: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=5, ge=1, le=20)


class AskIn(StrictModel):
    question: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=5, ge=1, le=20)


class ReprocessIn(StrictModel):
    model_version: Literal["v1", "v2"]
