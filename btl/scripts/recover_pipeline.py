"""Inspect a durable run or apply an explicitly reviewed recovery plan."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inspect", "plan-recovery", "resume", "abort"])
    parser.add_argument("--mode", choices=["test", "local", "integration", "production"], required=True)
    parser.add_argument("--manifest", help="Lake-relative runs/<hash>/manifest.json")
    parser.add_argument("--output", type=Path, help="New immutable plan file")
    parser.add_argument("--recovery-action", choices=["resume", "abort"], default="resume")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--plan-hash", help="Reviewed SHA-256 plan hash")
    parser.add_argument(
        "--airflow-metadata",
        action="store_true",
        help="Query actual configured Airflow metadata backend for stopped evidence",
    )
    parser.add_argument("--executor", choices=["commerce-dag"], default="commerce-dag")
    args = parser.parse_args(argv)
    from config.settings import settings
    from storage.storage_manager import storage
    from operations.recovery import inspect_run, plan_recovery, apply_recovery, AirflowMetadataAdapter

    settings.set_runtime_mode(args.mode)
    storage.set_mode(args.mode)
    adapter = AirflowMetadataAdapter() if args.airflow_metadata else None
    if args.action == "inspect":
        if not args.manifest:
            parser.error("inspect requires --manifest")
        result = inspect_run(args.manifest)
    elif args.action == "plan-recovery":
        if not args.manifest or not args.output:
            parser.error("plan-recovery requires --manifest and --output")
        result = plan_recovery(args.manifest, args.output, action=args.recovery_action, airflow_adapter=adapter)
    else:
        if not args.plan or not args.plan_hash:
            parser.error("resume/abort requires --plan and --plan-hash")
        plan = json.loads(args.plan.read_text())
        if plan.get("action") != args.action or plan.get("mode") != args.mode:
            parser.error("Plan action/mode differs from command")
        executor = None
        if args.action == "resume":
            from orchestration.dags.sentinel_daily_pipeline import recovery_stage_executor

            executor = recovery_stage_executor
        result = apply_recovery(args.plan, args.plan_hash, executor=executor, airflow_adapter=adapter)
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError) as error:
        print(f"Recovery refused: {error}", file=sys.stderr)
        raise SystemExit(1)
