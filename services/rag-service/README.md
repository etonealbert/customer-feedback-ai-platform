# RAG service

Entrypoint: `uvicorn feedback_platform.rag:app --host 0.0.0.0 --port 8000`.

Host endpoint: `http://localhost:8002`. The service authenticates the caller, obtains an embedding from the internal inference service, and retrieves matching feedback under the caller's tenant context. Optional LLM synthesis uses the configured API key; the local mode returns cited evidence without a paid model call.

Internal inference requests require `INTERNAL_API_KEY`. Credentials are supplied through `.env` in Compose or a Kubernetes Secret. The repository contains no live credentials. Liveness, readiness, and metrics endpoints follow the other HTTP services.
