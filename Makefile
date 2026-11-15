.PHONY: setup up down logs test integration lint load validate scale-worker

setup:
	uv sync --extra dev
	uv run python scripts/setup_env.py

up:
	docker compose up --build -d --wait

down:
	docker compose down

logs:
	docker compose logs --tail 100 -f ingestion worker outbox inference rag

test:
	uv run pytest tests/unit

integration:
	uv run pytest tests/integration -m integration

lint:
	uv run ruff check .

load:
	uv run python scripts/benchmark.py

validate:
	docker compose config --quiet
	kubectl kustomize infrastructure/kubernetes/base > /dev/null

scale-worker:
	docker compose up -d --scale worker=3
