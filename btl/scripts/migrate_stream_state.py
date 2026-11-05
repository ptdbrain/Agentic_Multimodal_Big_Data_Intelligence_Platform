"""Inspect, replay, reconcile and switch isolated streaming namespaces."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from operations import migration


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--checkpoint", required=True)
    plan = sub.add_parser("plan")
    plan.add_argument("--old-checkpoint", required=True)
    plan.add_argument("--destination", required=True)
    plan.add_argument("--input", action="append", required=True)
    plan.add_argument("--writer-pid", action="append", type=int, required=True)
    plan.add_argument("--engine", choices=["simulator", "spark"], default="spark")
    plan.add_argument("--mode", choices=["local", "test", "integration", "production"], default="integration")
    plan.add_argument("--old-output")
    plan.add_argument("--old-quarantine")
    plan.add_argument("--old-input-manifest")
    plan.add_argument("--output", required=True)
    for name in ["replay", "verify", "promote", "rollback"]:
        command = sub.add_parser(name)
        command.add_argument("--plan", required=True)
        if name in ["promote", "rollback"]:
            command.add_argument("--pointer", required=True)
    args = parser.parse_args()
    if args.command == "inspect":
        result = migration.inspect_checkpoint(args.checkpoint)
    elif args.command == "plan":
        result = migration.make_plan(
            args.old_checkpoint,
            args.destination,
            args.input,
            writer_pids=args.writer_pid,
            engine=args.engine,
            mode=args.mode,
            old_output=args.old_output,
            old_quarantine=args.old_quarantine,
            old_input_manifest=args.old_input_manifest,
        )
        with Path(args.output).open("x") as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
    else:
        plan = json.loads(Path(args.plan).read_text())
        if args.command == "replay":
            result = migration.replay(plan)
        elif args.command == "verify":
            result = migration.verify(plan)
        elif args.command == "promote":
            result = migration.promote(plan, args.pointer)
        else:
            result = migration.rollback(plan, args.pointer)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
