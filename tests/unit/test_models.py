import numpy as np
import pytest
from fastapi.testclient import TestClient

from feedback_platform.ml import EMBEDDING_DIM, EMBEDDING_VERSION, get_registry, load_corpus


@pytest.fixture(scope="module")
def registry():
    return get_registry()


def test_corpus_holdout_does_not_overlap_training():
    rows = load_corpus()
    train = {row["text"].casefold() for row in rows if row["split"] == "train"}
    test = {row["text"].casefold() for row in rows if row["split"] == "test"}
    assert train and test
    assert train.isdisjoint(test)
    assert len({row["id"] for row in rows}) == len(rows)


def test_real_classifiers_identify_clear_feedback(registry):
    complaint = registry.predict("The mobile app crashes every time I open it.")
    praise = registry.predict("The support team was helpful and solved my problem quickly.")
    assert complaint["sentiment"] == "negative"
    assert complaint["topics"] == ["reliability"]
    assert praise["sentiment"] == "positive"
    assert praise["topics"] == ["support"]
    assert 0 <= complaint["confidence"] <= 1


def test_embedding_space_is_stable_across_classifier_rollouts(registry):
    text = "The mobile app crashes on my phone"
    old = registry.predict(text, "v1")
    new = registry.predict(text, "v2")
    assert old["model_version"] == "v1"
    assert new["model_version"] == "v2"
    assert old["embedding_version"] == new["embedding_version"] == EMBEDDING_VERSION
    np.testing.assert_array_equal(old["embedding"], new["embedding"])
    assert len(old["embedding"]) == EMBEDDING_DIM
    assert np.isfinite(old["embedding"]).all()
    assert np.linalg.norm(old["embedding"]) == pytest.approx(1)


def test_semantic_projection_retrieves_relevant_topic(registry):
    query = np.array(registry.embed("mobile app crashes"))
    relevant = np.array(registry.embed("The application crashes and freezes"))
    unrelated = np.array(registry.embed("Monthly subscription pricing and invoice"))
    assert query @ relevant > query @ unrelated


def test_unknown_vocabulary_is_explicit_zero_vector(registry):
    assert np.linalg.norm(registry.embed("zxqvww qqqxxxzz")) == 0


def test_unknown_version_fails(registry):
    with pytest.raises(ValueError, match="Unknown model version"):
        registry.predict("A test comment", "missing-model")


@pytest.fixture
def inference_client(monkeypatch):
    from feedback_platform.inference import app
    from feedback_platform.settings import get_settings

    monkeypatch.setenv("INTERNAL_API_KEY", "unit-test-service-token")
    monkeypatch.setenv("MODEL_VERSION", "v1")
    get_settings.cache_clear()
    with TestClient(app) as client:
        yield client
    get_settings.cache_clear()


def test_inference_requires_internal_token(inference_client):
    assert inference_client.post("/predict", json={"text": "The app is great"}).status_code == 401
    assert inference_client.get("/health/ready").status_code == 200


def test_inference_validates_input_and_version(inference_client):
    headers = {"X-Service-Token": "unit-test-service-token"}
    assert inference_client.post("/predict", headers=headers, json={"text": "   "}).status_code == 422
    assert inference_client.post(
        "/predict", headers=headers, json={"text": "Great app", "model_version": "bad"}
    ).status_code == 422
    result = inference_client.post(
        "/predict", headers=headers, json={"text": "Great app", "model_version": "v2"}
    )
    assert result.status_code == 200
    assert result.json()["model_version"] == "v2"
    assert len(result.json()["embedding"]) == EMBEDDING_DIM


def test_kserve_v2_batch_and_shape_validation(inference_client):
    headers = {"X-Service-Token": "unit-test-service-token"}
    body = {
        "id": "batch-1",
        "inputs": [{"name": "text", "shape": [2], "datatype": "BYTES", "data": ["Great app", "Bad app"]}],
    }
    response = inference_client.post("/v2/models/v1/infer", headers=headers, json=body)
    assert response.status_code == 200
    assert response.json()["id"] == "batch-1"
    assert response.json()["outputs"][2]["shape"] == [2, EMBEDDING_DIM]
    body["inputs"][0]["shape"] = [1]
    assert inference_client.post("/v2/models/v1/infer", headers=headers, json=body).status_code == 422


def test_unhealthy_default_model_fails_startup(monkeypatch):
    from feedback_platform.inference import app
    from feedback_platform.settings import get_settings

    monkeypatch.setenv("MODEL_VERSION", "unhealthy-version")
    get_settings.cache_clear()
    try:
        with pytest.raises(ValueError, match="Unknown model version"), TestClient(app):
            pass
    finally:
        get_settings.cache_clear()
