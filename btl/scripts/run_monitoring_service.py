"""Foreground monitoring processes for explicit systemd ownership, using private paths."""

import argparse, json, os, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_scheduled_backup import environment


def command(config, name):
    work = Path(config["work"]).resolve()
    vendor = Path(config["tools"]).resolve()
    python = config["python"]
    if name in ("metrics", "dependencies", "operations"):
        module, port = {
            "metrics": ("metrics_exporter", 8000),
            "dependencies": ("services_exporter", 8001),
            "operations": ("operations_exporter", 8002),
        }[name]
        return [
            python,
            str(ROOT / "monitoring" / f"{module}.py"),
            "--serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ], {}
    if name == "receiver":
        return [
            python,
            str(ROOT / "scripts/verify_operations_stack.py"),
            "--action",
            "receiver",
            "--output",
            str(work / "notifications.jsonl"),
        ], {}
    if name == "prometheus":
        return [
            str(vendor / "prometheus-2.54.1.linux-amd64/prometheus"),
            "--config.file=" + str(work / "prometheus.yml"),
            "--storage.tsdb.path=" + str(work / "prometheus-data"),
            "--web.listen-address=127.0.0.1:9090",
        ], {}
    if name == "alertmanager":
        return [
            str(vendor / "alertmanager-0.27.0.linux-amd64/alertmanager"),
            "--config.file=" + str(ROOT / "monitoring/alertmanager.yml"),
            "--storage.path=" + str(work / "alertmanager-data"),
            "--web.listen-address=127.0.0.1:9093",
            "--cluster.listen-address=",
        ], {}
    if name == "grafana":
        home = work / "grafana-v11.2.0"
        env = dict(
            GF_SERVER_HTTP_ADDR="127.0.0.1",
            GF_SERVER_HTTP_PORT="3000",
            GF_PATHS_DATA=str(work / "data"),
            GF_PATHS_LOGS=str(work / "logs"),
            GF_PATHS_PROVISIONING=str(work / "provisioning"),
            GF_SECURITY_ADMIN_USER="admin",
            GF_SECURITY_ADMIN_PASSWORD=(work / "admin-password").read_text().strip(),
            GF_SECURITY_SECRET_KEY=(work / "secret-key").read_text().strip(),
            GF_USERS_ALLOW_SIGN_UP="false",
            GF_AUTH_ANONYMOUS_ENABLED="false",
            GF_ANALYTICS_REPORTING_ENABLED="false",
            GF_ANALYTICS_CHECK_FOR_UPDATES="false",
        )
        return [str(home / "bin/grafana"), "server", "--homepath", str(home)], env
    raise ValueError("Unknown owned monitoring service")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--service", required=True)
    args = parser.parse_args()
    if args.config.stat().st_mode & 0o077:
        raise ValueError("Private monitoring configuration required")
    config = json.loads(args.config.read_text())
    env = dict(os.environ, **environment(config["environment_file"]))
    env.update(OPS_SOURCE_JOBS_CONFIG=config["source_jobs"], OPS_BACKUP_STATUS_FILE=config["backup_status_file"])
    args, extra = command(config, args.service)
    env.update(extra)
    os.execvpe(args[0], args, env)


if __name__ == "__main__":
    main()
