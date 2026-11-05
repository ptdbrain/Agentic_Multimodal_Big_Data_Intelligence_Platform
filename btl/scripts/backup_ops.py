"""Fenced backup/restore CLI. Configuration contains references, never passwords."""

from pathlib import Path
import argparse
from contextlib import contextmanager
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from operations.backup import (
    create_backup,
    verify_backup,
    list_backups,
    retention_plan,
    restore_plan,
    restore_apply,
    restore_verify,
    retention_apply,
    maintenance_guard,
    kafka_drain_state,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["backup", "restore"])
    parser.add_argument(
        "action", choices=["create", "list", "verify", "plan", "apply", "retention-plan", "retention-apply"]
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    started = time.monotonic()
    config = json.loads(args.config.read_text())
    mode = config.get("mode", "integration")

    @contextmanager
    def guarded():
        with maintenance_guard(mode) as state:
            if args.operation == "backup" and args.action == "create" and mode in ("integration", "production"):
                kafka = config.get("kafka")
                if not kafka:
                    raise ValueError("Integration backup requires explicit Kafka drain scope")
                state["offsets"] = kafka_drain_state(kafka["bootstrap_servers"], kafka["groups"], kafka["topics"])
            yield state

    if args.operation == "backup":
        if args.action == "create":
            result = {
                "path": str(
                    create_backup(config["backup_target"], config["components"], guarded, config.get("inventory"))
                )
            }
        elif args.action == "list":
            result = list_backups(config["backup_target"])
        elif args.action == "verify":
            result = verify_backup(args.backup)
        elif args.action == "retention-plan":
            result = {
                "remove": retention_plan(list_backups(config["backup_target"])),
                "backup_target": str(Path(config["backup_target"]).resolve()),
                "inventory": list_backups(config["backup_target"]),
                "policy": "seven daily / four weekly, preserve latest; no automatic deletion",
            }
        elif args.action == "retention-apply":
            if not args.plan:
                parser.error("Retention apply requires --plan")
            result = retention_apply(config["backup_target"], json.loads(args.plan.read_text()))
        else:
            parser.error("Unsupported backup action")
    else:
        if args.action == "plan":
            result = restore_plan(args.backup, config["restore_targets"])
        elif args.action == "apply":
            if not args.plan:
                parser.error("Restore apply requires --plan")
            result = restore_apply(args.backup, json.loads(args.plan.read_text()), guarded)
        elif args.action == "verify":
            result = restore_verify(args.backup, config["restore_targets"])
        else:
            parser.error("Unsupported restore action")
    if (args.operation, args.action) in {("backup", "create"), ("restore", "apply")}:
        from operations.status import OperationStatus

        with OperationStatus(mode=mode) as status:
            name = "backup_last_complete_timestamp" if args.operation == "backup" else "restore_last_success_timestamp"
            status.set_gauge(name, time.time())
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        with args.output.open("x") as stream:
            stream.write(text + "\n")
    else:
        print(text)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Third-party exceptions may contain credentials/connection strings.
        print(f"Operation failed: {type(error).__name__}; no success claimed.", file=sys.stderr)
        sys.exit(1)
