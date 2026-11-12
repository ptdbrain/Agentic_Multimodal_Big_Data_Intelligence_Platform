"""Exercise checked-in rules through real Prometheus, Alertmanager and HTTP delivery.

All fault measurements belong to an isolated labelled test namespace. Only
scrape/evaluation/group intervals are shortened; production alert expressions
and their pending timers remain unchanged.
"""

import argparse, json, socket, subprocess, sys, threading, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tools", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    work = args.work.resolve()
    work.mkdir(parents=True, exist_ok=False, mode=0o700)
    import yaml
    from config.settings import settings
    from operations.status import OperationStatus
    from monitoring.metrics_exporter import create_metrics_server
    from scripts.verify_operations_stack import webhook_server

    settings.set_runtime_mode("test")
    settings.database.sqlite_path = work / "status.sqlite"
    receiver = webhook_server(work / "notifications.jsonl", port=0)

    def collect():
        with OperationStatus(mode="test") as status:
            return status.metrics()

    exporter = create_metrics_server("127.0.0.1", 0, metrics_provider=collect)
    servers = (receiver, exporter)
    for server in servers:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    # Explicit reserved listeners: never replace existing monitoring.
    for port in (19590, 19593):
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    am = yaml.safe_load((ROOT / "monitoring/alertmanager.yml").read_text())
    am["route"].update(group_wait="1s", group_interval="1s", repeat_interval="1m")
    am["receivers"][0]["webhook_configs"][0]["url"] = f"http://127.0.0.1:{receiver.server_port}/alerts"
    (work / "alertmanager.yml").write_text(yaml.safe_dump(am))
    prom = {
        "global": {"scrape_interval": "1s", "evaluation_interval": "1s"},
        "rule_files": [str(ROOT / "monitoring/alerts.yml")],
        "alerting": {"alertmanagers": [{"static_configs": [{"targets": ["127.0.0.1:19593"]}]}]},
        "scrape_configs": [
            {"job_name": "sentinel-operations", "static_configs": [{"targets": [f"127.0.0.1:{exporter.server_port}"]}]}
        ],
    }
    (work / "prometheus.yml").write_text(yaml.safe_dump(prom))

    def api(path, data=None):
        request = Request(
            "http://127.0.0.1:19593" + path,
            data=json.dumps(data).encode() if data is not None else None,
            headers={"Content-Type": "application/json"},
        )
        with urlopen(request, timeout=5) as response:
            return json.load(response)

    def rows():
        path = work / "notifications.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def delivered(name, state):
        return any(
            alert["labels"]["alertname"] == name and alert["status"] == state
            for row in rows()
            for alert in row["notification"]["alerts"]
        )

    def wait(check, description, timeout=50):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if check():
                    return
            except (OSError, ValueError):
                pass
            time.sleep(0.4)
        raise RuntimeError("Not verified: " + description)

    children = []
    try:
        for name, binary, flags in [
            (
                "alertmanager",
                args.tools / "alertmanager-0.27.0.linux-amd64/alertmanager",
                [
                    "--config.file=" + str(work / "alertmanager.yml"),
                    "--storage.path=" + str(work / "am-data"),
                    "--web.listen-address=127.0.0.1:19593",
                    "--cluster.listen-address=",
                ],
            ),
            (
                "prometheus",
                args.tools / "prometheus-2.54.1.linux-amd64/prometheus",
                [
                    "--config.file=" + str(work / "prometheus.yml"),
                    "--storage.tsdb.path=" + str(work / "prom-data"),
                    "--web.listen-address=127.0.0.1:19590",
                ],
            ),
        ]:
            with (work / (name + ".log")).open("x") as log:
                children.append(subprocess.Popen([str(binary), *flags], stdout=log, stderr=subprocess.STDOUT))
        wait(lambda: api("/-/ready") if False else api("/api/v2/status"), "Alertmanager ready")
        with OperationStatus(mode="test") as status:
            status.start("injected-failure", "commerce", "simulation")
            status.fail("injected-failure")
            status.start("injected-shortage", "articles", "web_content", expected_min=5)
            status.finish("injected-shortage", 1, 1, 0)
            status.set_gauge("checkpoint_errors", 1)
            status.set_gauge("backup_last_complete_timestamp", time.time() - 100000)
            status.stream_error()
        names = [
            "SentinelPipelineFailed",
            "SentinelInputShortage",
            "SentinelCheckpointFailure",
            "SentinelBackupStale",
            "SentinelStreamFailed",
        ]
        for name in names:
            wait(lambda name=name: delivered(name, "firing"), name + " firing")
        with OperationStatus(mode="test") as status:
            status.start("recovered-commerce", "commerce", "simulation")
            status.finish("recovered-commerce", 5, 5, 0)
            status.start("recovered-articles", "articles", "web_content", expected_min=5)
            status.finish("recovered-articles", 5, 5, 0)
            status.set_gauge("checkpoint_errors", 0)
            status.set_gauge("backup_last_complete_timestamp", time.time())
            # Dedicated SQL fault drill: clearing this test gauge tests rule delivery,
            # actual committed Spark clearing is verified in stream progress tests.
            status.set_gauge("stream_errors", 0, pipeline="streaming")
        for name in names:
            wait(lambda name=name: delivered(name, "resolved"), name + " resolved")
        now = datetime.now(timezone.utc)
        silence = api(
            "/api/v2/silences",
            {
                "matchers": [{"name": "alertname", "value": "SentinelCheckpointFailure", "isRegex": False}],
                "startsAt": now.isoformat(),
                "endsAt": (now + timedelta(minutes=5)).isoformat(),
                "createdBy": "isolated-drill",
                "comment": "Labelled maintenance test",
            },
        )["silenceID"]
        before = sum(
            alert["status"] == "firing" and alert["labels"]["alertname"] == "SentinelCheckpointFailure"
            for row in rows()
            for alert in row["notification"]["alerts"]
        )
        with OperationStatus(mode="test") as status:
            status.set_gauge("checkpoint_errors", 1)

        def suppressed():
            return any(
                a["labels"]["alertname"] == "SentinelCheckpointFailure" and silence in a["status"]["silencedBy"]
                for a in api("/api/v2/alerts")
            )

        wait(suppressed, "actual maintenance silence attached to firing alert")
        time.sleep(4)  # several real test group intervals while silence is active
        after = sum(
            alert["status"] == "firing" and alert["labels"]["alertname"] == "SentinelCheckpointFailure"
            for row in rows()
            for alert in row["notification"]["alerts"]
        )
        if before != after:
            raise RuntimeError("Silenced fault was delivered")
        with OperationStatus(mode="test") as status:
            status.set_gauge("checkpoint_errors", 0)
        report = {
            "passed": True,
            "firing_and_resolved": names,
            "maintenance_silence_verified": True,
            "rules_count": sum(
                len(g["rules"]) for g in yaml.safe_load((ROOT / "monitoring/alerts.yml").read_text())["groups"]
            ),
            "boundary": "Real isolated native Prometheus/Alertmanager/webhook with explicit injected SQL fault measurements; production rule expressions/pending timers unchanged. Long pending/freshness/unknown cases also require promtool matrix.",
        }
        (work / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))
    finally:
        for child in children:
            child.terminate()
        for child in children:
            child.wait(timeout=15)
        for server in servers:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    main()
