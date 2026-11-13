"""Execute one configured source in its own environment/process."""

from pathlib import Path
import argparse, hashlib, json, os, shlex, sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["run", "capture", "process", "status"])
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--scheduler", choices=["airflow", "systemd"], default="airflow")
    args = parser.parse_args(argv)
    import yaml
    from orchestration.source_jobs import validate_config, execute_job

    config = validate_config(yaml.safe_load(args.config.read_text()))
    job = next(job for job in config["jobs"] if job["id"] == args.job)
    if args.action != "status":
        env = Path(job["environment_file"])
        if not env.is_file() or env.stat().st_mode & 0o077:
            raise ValueError("Private source environment file required (0600)")
        for line in env.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            key, value = line.split("=", 1)
            if not key.strip().replace("_", "").isalnum():
                raise ValueError("Invalid environment key")
            parsed = shlex.split(value)
            os.environ[key.strip()] = parsed[0] if parsed else ""
        if os.environ.get("SOURCE_JOB_NAMESPACE") != job["namespace"]:
            raise ValueError("Source environment namespace mismatch")
        os.environ["SERVING_DATASET_ID"] = job["id"]
        from orchestration.source_jobs import preflight_target
    run_id = args.job + "-" + hashlib.sha256(args.run_id.encode()).hexdigest()[:32]
    result = execute_job(
        config,
        args.job,
        run_id,
        action=args.action,
        scheduler=args.scheduler,
        preflight=preflight_target if args.action != "status" else None,
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    main()
