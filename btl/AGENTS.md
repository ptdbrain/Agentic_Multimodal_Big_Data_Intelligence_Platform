# Repository Guidelines

## Project Structure & Module Organization

SentinelAI is a Python big data platform for e-commerce ingestion and analytics. `ingestion/` collects files, API responses, and crawler data; `kafka/` manages producers, consumers, topics, and dead letters. `spark/` handles batch and streaming processing, while `storage/` implements the Bronze, Silver, and Gold lake layers. Analytics and serving live in `analytics/`, `database/`, and `dashboard/`. Use `orchestration/` for Airflow DAGs and `monitoring/` for observability. Data contracts live in `data/schemas/`, sample fixtures in `data/sample/`, tests in `tests/`, and operational entry points in `scripts/`.

## Build, Test, and Development Commands

Run commands from the repository root:

- `pip install -r requirements.txt`: install Python dependencies.
- `make up` / `make down`: start or stop Docker Compose infrastructure.
- `make health`: check service connectivity.
- `make demo`: execute the end-to-end data pipeline.
- `streamlit run dashboard/app.py`: launch the dashboard.
- `make test-unit`: run isolated tests without external services.
- `make test`: run the full pytest suite.
- `make lint`: compile Python sources and run Ruff checks.
- `make benchmark`: run the volume, velocity, and variety benchmarks.

## Coding Style & Naming Conventions

Use four-space indentation, `snake_case` for modules and functions, and `PascalCase` for classes. Follow neighboring code and use type hints for new interfaces. Ruff targets Python 3.11 with a configured line length of 120; its enabled checks focus on syntax and critical errors. Keep schema changes consistent with ingestion validation and downstream consumers.

## Testing Guidelines

Use pytest with `test_*.py` files, `Test*` classes, and `test_*` functions. Mark isolated tests `unit` and infrastructure-dependent tests `integration`. Run `pytest -m integration -v` with the required services available. Cover changed behavior, including malformed records and pipeline accounting where relevant. Keep temporary data isolated and clean it up. No numeric coverage threshold is configured.

## Commit & Pull Request Guidelines

Follow the scoped Conventional Commit style used in history, such as `feat(etl): ...` or `fix(spark): ...`. Keep commits focused. Pull requests should describe the behavior change, link related issues, and report validation commands and results. Include screenshots for dashboard changes and explain schema or configuration impacts.

## Security & Configuration

Use `.env.example` as the configuration reference. Keep credentials and local `.env` values out of commits. Distinguish host endpoints from Docker service endpoints. Treat `make reset` as destructive: it removes local lake data.
