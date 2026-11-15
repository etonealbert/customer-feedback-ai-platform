"""Small, reproducible CPU models with a separately versioned embedding space.

These are trained classifiers, not keyword rules. They are deliberately small
demonstration baselines. No model artifact is downloaded or unpickled.
"""

from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import normalize

EMBEDDING_DIM = 128
EMBEDDING_VERSION = "lsa-v1"
MODEL_VERSIONS = ("v1", "v2")


def corpus_path() -> Path:
    configured = os.getenv("MODEL_CORPUS_PATH")
    if configured:
        return Path(configured)
    module = Path(__file__).resolve()
    candidates = [
        module.parents[2] / "models" / "training" / "feedback.jsonl",  # source checkout
        module.parents[1] / "models" / "training" / "feedback.jsonl",  # installed wheel
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "Training corpus missing; set MODEL_CORPUS_PATH or reinstall the package"
    )


def load_corpus() -> list[dict[str, str]]:
    with corpus_path().open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


class ModelRegistry:
    """Train immutable model versions and one stable embedding model once."""

    def __init__(self) -> None:
        corpus = load_corpus()
        training = [row for row in corpus if row["split"] == "train"]
        self.training_count = len(training)
        self.evaluation_count = sum(row["split"] == "test" for row in corpus)
        # Text mode canonicalizes line endings, so checkouts have the same hash
        # on Windows and Linux even when Git's autocrlf setting differs.
        self.corpus_sha256 = hashlib.sha256(
            corpus_path().read_text(encoding="utf-8").encode("utf-8")
        ).hexdigest()
        texts = [row["text"] for row in training]
        self.models: dict[str, dict[str, Pipeline]] = {}
        for version in MODEL_VERSIONS:
            self.models[version] = {}
            for target in ("sentiment", "topic"):
                pipeline = Pipeline(
                    [
                        (
                            "tfidf",
                            TfidfVectorizer(
                                ngram_range=(1, 1) if version == "v1" else (1, 2),
                                sublinear_tf=True,
                                strip_accents="unicode",
                            ),
                        ),
                        (
                            "classifier",
                            LogisticRegression(
                                C=4.0 if version == "v1" else 8.0,
                                class_weight="balanced",
                                random_state=42,
                                max_iter=1000,
                            ),
                        ),
                    ]
                )
                pipeline.fit(texts, [row[target] for row in training])
                self.models[version][target] = pipeline

        # Do not couple the retrieval space to a classifier rollout. The same
        # encoder embeds historical rows and queries for both model versions.
        self.vectorizer = TfidfVectorizer(
            ngram_range=(1, 2), sublinear_tf=True, strip_accents="unicode"
        )
        features = self.vectorizer.fit_transform(texts)
        dimensions = min(64, features.shape[0] - 1, features.shape[1] - 1)
        self.reducer = TruncatedSVD(n_components=dimensions, n_iter=7, random_state=42)
        self.reducer.fit(features)
        self.semantic_dimensions = dimensions

    def require_version(self, version: str) -> None:
        if version not in self.models:
            raise ValueError(
                f"Unknown model version: {version}. Available: {', '.join(MODEL_VERSIONS)}"
            )

    def embed(self, text: str) -> list[float]:
        vector = self.reducer.transform(self.vectorizer.transform([text]))
        vector = normalize(vector, norm="l2")[0]
        padded = np.zeros(EMBEDDING_DIM, dtype=np.float64)
        padded[: len(vector)] = vector
        return padded.tolist()

    def predict(self, text: str, version: str = "v1") -> dict[str, Any]:
        self.require_version(version)
        started = perf_counter()
        sentiment = self.models[version]["sentiment"]
        probability = sentiment.predict_proba([text])[0]
        label_index = int(probability.argmax())
        topic = self.models[version]["topic"].predict([text])[0]
        return {
            "sentiment": str(sentiment.classes_[label_index]),
            "confidence": float(probability[label_index]),
            "topics": [str(topic)],
            "model_version": version,
            "embedding": self.embed(text),
            "embedding_version": EMBEDDING_VERSION,
            "inference_ms": (perf_counter() - started) * 1000,
        }

    def metadata(self) -> dict[str, Any]:
        return {
            "versions": [
                {
                    "version": version,
                    "algorithm": "TF-IDF + logistic regression",
                    "ngram_range": [1, 1 if version == "v1" else 2],
                    "regularization_C": 4.0 if version == "v1" else 8.0,
                }
                for version in MODEL_VERSIONS
            ],
            "embedding_version": EMBEDDING_VERSION,
            "embedding_dim": EMBEDDING_DIM,
            "semantic_dimensions": self.semantic_dimensions,
            "embedding_algorithm": "TF-IDF + truncated SVD (LSA); padded to 128 dimensions",
            "training_rows": self.training_count,
            "heldout_rows": self.evaluation_count,
            "corpus_sha256": self.corpus_sha256,
            "dataset_kind": "small hand-authored synthetic demonstration",
            "limitations": [
                "English only; not validated for production or demographic subgroups.",
                "Confidence is an uncalibrated classifier probability.",
                "Topics are single-label predictions returned as a list.",
                "LSA is a local semantic baseline, not a pretrained neural embedding model.",
                "Completely unknown vocabulary maps to a zero vector; retrieval must abstain.",
            ],
        }


@lru_cache(maxsize=1)
def get_registry() -> ModelRegistry:
    return ModelRegistry()
