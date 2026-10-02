.PHONY: help up down demo test benchmark health dag metrics reset lint

help:
	@echo "SentinelAI Platform Management Commands:"
	@echo "  make up         - Start all Docker infrastructure services"
	@echo "  make down       - Stop all Docker infrastructure services"
	@echo "  make demo       - Execute complete end-to-end data pipeline"
	@echo "  make test       - Run full pytest verification suite"
	@echo "  make benchmark  - Run 3V Big Data benchmark suite"
	@echo "  make health     - Run infrastructure connectivity health checks"
	@echo "  make dag        - Run Airflow pipeline DAG standalone"
	@echo "  make metrics    - Export Prometheus observability metrics"
	@echo "  make reset      - Clean local data lake and reset database"

up:
	docker compose up -d

down:
	docker compose down

demo:
	python scripts/run_e2e_pipeline.py

test:
	pytest -v

benchmark:
	python scripts/benchmark.py

health:
	python scripts/health_check.py

dag:
	python orchestration/dags/sentinel_daily_pipeline.py

metrics:
	python monitoring/metrics_exporter.py

reset:
	python -c "import shutil, pathlib; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('storage/datalake').glob('*') if p.is_dir() and p.name != 'quarantine']; print('Data Lake tiers reset.')"
