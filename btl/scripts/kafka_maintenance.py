"""Run-topic registry inventory and explicit cleanup planning/application."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operations.kafka_lifecycle import TopicRegistry, KafkaMaintenance, cleanup_plan, apply_cleanup, verify_bronze


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inventory", "plan", "apply", "archive-failure"])
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--topic")
    parser.add_argument("--proofs", type=Path)
    parser.add_argument("--review")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--mode", choices=["integration", "local", "test"], default="integration")
    args = parser.parse_args()
    registry = TopicRegistry(args.mode)
    broker = None
    try:
        if args.action == "inventory":
            result = {"version": 1, "topics": registry.entries()}
        elif args.action == "archive-failure":
            if not all([args.topic, args.proofs, args.review]):
                parser.error("--topic --proofs --review required")
            registry.archive_failure(args.topic, json.loads(args.proofs.read_text()), args.review, verify_bronze)
            result = {"archived": args.topic}
        else:
            if args.mode in ("local", "test"):
                parser.error("Broker maintenance requires integration mode")
            broker = KafkaMaintenance()
            if args.action == "plan":
                result = cleanup_plan(registry, broker, verify_bronze)
            else:
                if not args.plan:
                    parser.error("--plan required")
                result = {"deleted": apply_cleanup(registry, broker, verify_bronze, json.loads(args.plan.read_text()))}
        content = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
        if args.output:
            with args.output.open("x") as stream:
                stream.write(content)
        else:
            print(content)
    finally:
        registry.close()
        if broker:
            broker.close()


if __name__ == "__main__":
    main()
