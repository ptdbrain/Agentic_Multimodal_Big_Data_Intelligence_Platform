"""Apply verified age/ownership/offset/archive cleanup to managed web run topics."""

import argparse, json, os, subprocess, sys, uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--job", help=argparse.SUPPRESS)
    args = parser.parse_args()
    import yaml
    from orchestration.source_jobs import validate_config
    from scripts.run_scheduled_backup import environment

    config = validate_config(yaml.safe_load(args.config.read_text()))
    jobs = [job for job in config["jobs"] if job["adapter"] == "web"]
    if args.job:
        job = next(job for job in jobs if job["id"] == args.job)
        os.environ.update(environment(job["environment_file"]))
        if os.environ.get("SOURCE_JOB_NAMESPACE") != job["namespace"]:
            raise ValueError("Source namespace mismatch")
        from orchestration.source_jobs import preflight_target
        from operations.kafka_lifecycle import (
            TopicRegistry,
            KafkaMaintenance,
            cleanup_plan,
            apply_cleanup,
            verify_bronze,
        )

        preflight_target(config, job)
        folder = args.output_root / job["id"] / uuid.uuid4().hex
        folder.mkdir(parents=True, exist_ok=False, mode=0o700)
        registry = TopicRegistry("integration")
        broker = None
        try:
            broker = KafkaMaintenance()
            plan = cleanup_plan(registry, broker, verify_bronze)
            (folder / "plan.json").write_text(json.dumps(plan, indent=2))
            deleted = apply_cleanup(registry, broker, verify_bronze, plan)
            (folder / "result.json").write_text(
                json.dumps({"deleted": deleted, "eligible": len(plan["candidates"])}, indent=2)
            )
        finally:
            registry.close()
            if broker:
                broker.close()
        print(json.dumps({"job": job["id"], "deleted": deleted, "report": str(folder)}))
    else:
        for job in jobs:
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--config",
                    str(args.config),
                    "--output-root",
                    str(args.output_root),
                    "--job",
                    job["id"],
                ],
                check=True,
            )


if __name__ == "__main__":
    main()
