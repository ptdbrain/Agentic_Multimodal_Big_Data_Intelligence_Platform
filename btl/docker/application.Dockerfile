FROM apache/spark:3.5.0 AS sparkdist
FROM python:3.11-slim-bookworm
ENV PYTHONPATH=/app JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/opt/project-venv/bin/python PYSPARK_DRIVER_PYTHON=/opt/project-venv/bin/python \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 RUNNING_IN_DOCKER=true
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl tini && rm -rf /var/lib/apt/lists/*
COPY --from=sparkdist /opt/spark /opt/spark
WORKDIR /app
COPY requirements.txt requirements-airflow.txt ./
RUN python -m venv /opt/project-venv && /opt/project-venv/bin/pip install --no-cache-dir -r requirements.txt
RUN python -m venv /opt/airflow-venv && /opt/airflow-venv/bin/pip install --no-cache-dir -r requirements-airflow.txt psycopg2-binary
# Airflow keeps its own dependency versions; missing project imports resolve
# from the project environment, as in the verified native dual-venv runtime.
RUN /opt/airflow-venv/bin/python -c 'import pathlib,site; (pathlib.Path(site.getsitepackages()[0]) / "sentinel-project-runtime.pth").write_text("/opt/project-venv/lib/python3.11/site-packages\n")'
ENV PATH=/opt/project-venv/bin:/opt/spark/bin:/usr/local/bin:/usr/bin:/bin
COPY . /app
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8502", "--browser.gatherUsageStats=false"]
