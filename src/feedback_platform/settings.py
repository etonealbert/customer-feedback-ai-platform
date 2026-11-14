from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql://feedback_app:feedback@localhost:5432/feedback"
    admin_database_url: str = ""
    app_db_password: str = ""
    internal_api_key: str = ""
    demo_tenant_a_key: str = ""
    demo_tenant_b_key: str = ""
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "feedback.v1"
    kafka_dlq_topic: str = "feedback.dlq.v1"
    kafka_group_id: str = "feedback-workers"
    inference_url: str = "http://localhost:8001"
    model_version: str = "v1"
    embedding_dim: int = 128
    embedding_version: str = "lsa-v1"
    max_attempts: int = Field(default=4, ge=1, le=20)
    retry_base_seconds: float = Field(default=0.5, ge=0.01, le=30)
    db_pool_size: int = Field(default=10, ge=1, le=100)
    llm_provider: str = "extractive"
    openai_api_key: str = ""
    openai_model: str = ""
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
