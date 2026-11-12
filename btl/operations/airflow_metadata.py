"""Airflow-only metadata boundary, usable in a separate scheduler environment."""

import json
import os
from pathlib import Path
import subprocess
import sys

PREFIX = "SENTINEL_OWNER_EVIDENCE "


def query_terminal_run(owner):
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session

    with create_session() as session:
        run = (
            session.query(DagRun)
            .filter(DagRun.dag_id == owner["dag_id"], DagRun.run_id == owner["dag_run_id"])
            .one_or_none()
        )
        if run is None or run.state not in {"success", "failed"}:
            raise RuntimeError("Airflow backend does not confirm a terminal DagRun")
        states = [
            row[0]
            for row in session.query(TaskInstance.state)
            .filter(TaskInstance.dag_id == owner["dag_id"], TaskInstance.run_id == owner["dag_run_id"])
            .all()
        ]
        terminal = {"success", "failed", "skipped", "upstream_failed", "removed"}
        if not states or any(state not in terminal for state in states):
            raise RuntimeError("Airflow backend has nonterminal task instances")
        url = session.get_bind().url
        backend = {key: str(getattr(url, key, None)) for key in ("drivername", "host", "port", "database", "username")}
        return {
            "kind": "airflow",
            "metadata_backend": backend,
            "dag_id": owner["dag_id"],
            "dag_run_id": owner["dag_run_id"],
            "dag_state": run.state,
            "task_states": states,
        }


def verify_terminal_run(owner):
    python = os.environ.get("AIRFLOW_METADATA_PYTHON")
    if not python or Path(python).absolute() == Path(sys.executable).absolute():
        return query_terminal_run(owner)
    child = subprocess.run(
        [python, str(Path(__file__).resolve()), json.dumps(owner)], capture_output=True, text=True, timeout=45
    )
    if child.returncode:
        raise RuntimeError("Airflow metadata subprocess could not verify a terminal run; backend details withheld")
    payloads = [line[len(PREFIX) :] for line in child.stdout.splitlines() if line.startswith(PREFIX)]
    if len(payloads) != 1:
        raise RuntimeError("Airflow metadata boundary returned no unique evidence")
    result = json.loads(payloads[0])
    if (
        result.get("dag_id") != owner["dag_id"]
        or result.get("dag_run_id") != owner["dag_run_id"]
        or not result.get("metadata_backend")
    ):
        raise ValueError("Airflow metadata boundary identity differs")
    return result


if __name__ == "__main__":
    try:
        evidence = query_terminal_run(json.loads(sys.argv[1]))
        print(PREFIX + json.dumps(evidence))
    except Exception:
        print("Airflow metadata verification refused", file=sys.stderr)
        raise SystemExit(1)
