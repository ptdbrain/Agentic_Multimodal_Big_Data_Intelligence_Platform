"""Durable capture/process handoff for explicitly configured source jobs."""

from pathlib import Path
import copy, fcntl, hashlib, json, os, re, tempfile

GROUPS = {"tiki": "marketplace", "web": "web_content", "simulation": "simulation"}


def validate_config(config):
    value = copy.deepcopy(config)
    if value.get("version") != 1 or not value.get("root") or not value.get("jobs"):
        raise ValueError("Source config version/root/jobs required")
    ids = set()
    namespaces = {}
    for job in value["jobs"]:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        job.setdefault("timezone", "Asia/Ho_Chi_Minh")
        try:
            ZoneInfo(job["timezone"])
        except (ZoneInfoNotFoundError, TypeError) as error:
            raise ValueError("Invalid source scheduler timezone") from error
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,40}", job.get("id", "")) or job["id"] in ids:
            raise ValueError("Unique safe source job ID required")
        ids.add(job["id"])
        if GROUPS.get(job.get("adapter")) != job.get("source_group"):
            raise ValueError("Adapter/source provenance mismatch")
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,60}", job.get("namespace", "")):
            raise ValueError("Safe isolated namespace required")
        if job.get("owner") not in ("airflow", "systemd") or not job.get("schedule"):
            raise ValueError("Explicit scheduler owner/schedule required")
        if type(job.get("minimum_records")) is not int or job["minimum_records"] < 0:
            raise ValueError("Minimum input count invalid")
        previous = namespaces.setdefault(job["namespace"], job["source_group"])
        if previous != job["source_group"]:
            raise ValueError("Different source classes cannot share a namespace")
    return value


def write_atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=".stage-")
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, ensure_ascii=False, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def execute_job(
    config, job_id, run_id, *, action="run", scheduler="airflow", capture=None, process=None, preflight=None
):
    config = validate_config(config)
    job = next((job for job in config["jobs"] if job["id"] == job_id), None)
    if not job or scheduler != job["owner"]:
        raise ValueError("Source scheduler ownership mismatch")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", run_id):
        raise ValueError("Safe source run ID required")
    if action not in ("capture", "process", "run", "status"):
        raise ValueError("Unsupported source action")
    folder = Path(config["root"]) / job_id / run_id
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (folder / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Source run already active") from None
        path = folder / "manifest.json"
        payload = folder / "capture.json"
        identity = hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()
        from orchestration.run_manifest import utc_now

        manifest = (
            json.loads(path.read_text())
            if path.exists()
            else {
                "version": 1,
                "run_id": run_id,
                "job_id": job_id,
                "namespace": job["namespace"],
                "source_group": job["source_group"],
                "config_sha256": identity,
                "state": "new",
                "started_at": utc_now(),
                "scheduler": scheduler,
                "mode": os.environ.get("RUNTIME_MODE", "integration"),
            }
        )
        if manifest["config_sha256"] != identity:
            raise ValueError("Run source config changed; create a new run")
        if action == "status":
            return manifest
        if manifest["state"] == "complete" or manifest.get("processing_state") == "complete":
            if not payload.is_file() or hashlib.sha256(payload.read_bytes()).hexdigest() != manifest.get(
                "capture_sha256"
            ):
                raise ValueError("Capture hash mismatch; completed receipt invalid")
            return manifest
        try:
            write_atomic(path, manifest)
            if preflight:
                manifest["physical_target"] = preflight(config, job)
            receipt_path = folder / "capture-receipt.json"
            if not payload.exists() or (not manifest.get("capture_sha256") and receipt_path.exists()):
                if receipt_path.exists():
                    receipt = json.loads(receipt_path.read_text())
                    expected = {
                        "version": 1,
                        "job_id": job_id,
                        "run_id": run_id,
                        "namespace": job["namespace"],
                        "config_sha256": identity,
                    }
                    if any(receipt.get(key) != value for key, value in expected.items()):
                        raise ValueError("Capture receipt identity mismatch")
                    result = receipt["capture"]
                    digest = hashlib.sha256(
                        json.dumps(result, ensure_ascii=False, allow_nan=False).encode()
                    ).hexdigest()
                    if digest != receipt.get("capture_sha256") or result.get("status") not in ("complete", "partial"):
                        raise ValueError("Capture receipt hash/status mismatch")
                else:
                    if action == "process":
                        raise ValueError("Capture missing; processing cannot recrawl")
                    result = (capture or capture_source)(job, folder)
                    if result.get("status") not in ("complete", "partial"):
                        raise RuntimeError("Source capture not usable: " + str(result.get("status")))
                    digest = hashlib.sha256(
                        json.dumps(result, ensure_ascii=False, allow_nan=False).encode()
                    ).hexdigest()
                    receipt = {
                        "version": 1,
                        "job_id": job_id,
                        "run_id": run_id,
                        "namespace": job["namespace"],
                        "config_sha256": identity,
                        "capture_sha256": digest,
                        "handed_off_at": utc_now(),
                        "capture": result,
                    }
                    # This one atomic object contains both the data and its identity.
                    # A later crash cannot orphan a payload from its hash/manifest.
                    write_atomic(receipt_path, receipt)
                if not payload.exists():
                    write_atomic(payload, result)
                if hashlib.sha256(payload.read_bytes()).hexdigest() != digest:
                    raise ValueError("Capture hash mismatch against durable receipt")
                records = result.get("records", result.get("datasets", {}).get("products", []))
                manifest.update(
                    state="captured",
                    capture_sha256=digest,
                    captured_at=result.get("finished_at"),
                    handed_off_at=receipt["handed_off_at"],
                    capture_status=result["status"],
                    input_records=len(records),
                )
                write_atomic(path, manifest)
            if hashlib.sha256(payload.read_bytes()).hexdigest() != manifest.get("capture_sha256"):
                raise ValueError("Capture hash mismatch; refused replay")
            if action == "capture":
                return manifest
            result = json.loads(payload.read_text())
            manifest["state"] = "processing"
            write_atomic(path, manifest)
            output = (process or process_source)(job, result, run_id)
            if output is False or (isinstance(output, dict) and output.get("completed") is False):
                raise RuntimeError("Processing did not complete; captured input retained")
            manifest.update(
                state="complete" if result["status"] == "complete" else "partial",
                processing_state="complete",
                capture_status=result["status"],
                process_result=output,
                processed_at=utc_now(),
            )
            manifest.pop("error_type", None)
            write_atomic(path, manifest)
            return manifest
        except Exception as error:
            manifest.update(state="failed", error_type=type(error).__name__, failed_at=utc_now())
            write_atomic(path, manifest)
            raise


def bind_target(config, job, actual):
    """Pin resolved physical resources under one shared registry before collection.

    Names alone cannot separate simulation from observed data. Any shared lake
    bucket or SQL schema is refused across configured namespaces.
    """
    required = {"lake_endpoint", "bucket", "db_host", "db_port", "db_name", "db_schema"}
    if set(actual) != required or any(value is None or str(value) == "" for value in actual.values()):
        raise ValueError("Complete physical target identity required")
    root = Path(config["root"])
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".targets.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = root / "targets.json"
        registry = json.loads(path.read_text()) if path.exists() else {}
        previous = registry.get(job["namespace"])
        entry = {"source_group": job["source_group"], "target": actual}
        if previous and previous != entry:
            raise ValueError("Namespace physical target changed; explicit migration required")
        for namespace, registered in registry.items():
            if namespace == job["namespace"]:
                continue
            target = registered["target"]
            same_lake = all(actual[key] == target[key] for key in ("lake_endpoint", "bucket"))
            same_sql = all(actual[key] == target[key] for key in ("db_host", "db_port", "db_name", "db_schema"))
            if same_lake or same_sql:
                raise ValueError("Physical target shared across source namespaces")
        registry[job["namespace"]] = entry
        write_atomic(path, registry)
    return entry


def preflight_target(config, job):
    """Resolve the real SQL search path; never trust a namespace environment label."""
    from config.settings import settings
    from database.warehouse import WarehouseManager

    if settings.runtime_mode not in ("integration", "production"):
        raise ValueError("Scheduled source jobs require real services")
    expected = job.get("target", {})
    if not expected.get("bucket") or not expected.get("postgres_schema"):
        raise ValueError("Explicit bucket and postgres_schema target required")
    if expected["bucket"] != settings.storage.data_lake_bucket:
        raise ValueError("Configured lake target mismatch")
    warehouse = WarehouseManager(mode=settings.runtime_mode)
    try:
        cursor = warehouse._conn.cursor()
        cursor.execute("SELECT current_schema()")
        schema = cursor.fetchone()[0]
        if schema != expected["postgres_schema"]:
            raise ValueError("Configured SQL schema target mismatch")
    finally:
        warehouse.close()
    return bind_target(
        config,
        job,
        {
            "lake_endpoint": settings.storage.endpoint,
            "bucket": settings.storage.data_lake_bucket,
            "db_host": settings.database.host,
            "db_port": settings.database.port,
            "db_name": settings.database.database,
            "db_schema": schema,
        },
    )


def capture_source(job, folder):
    parameters = job.get("parameters", {})
    if job["adapter"] == "tiki":
        from datetime import date
        from ingestion.daily_capture import capture_day
        from scripts.collect_tiki_daily import tiki_crawler_factory

        result = capture_day(
            Path(parameters.get("capture_root", folder.parent / "capture-state")),
            date.today(),
            tiki_crawler_factory(parameters.get("delay_seconds", 8)),
            query=parameters.get("query", "iphone"),
            max_products=parameters.get("max_products", 20),
            reviews_per_product=parameters.get("reviews_per_product", 20),
            incremental_reviews=True,
            review_max_pages=parameters.get("review_max_pages", 10),
        )
        attempt = Path(result["attempt_dir"])
        if result["status"] == "skipped":
            metadata = json.loads((attempt / "manifest.json").read_text())
            result.update(metadata)
        if result["status"] not in ("complete", "partial"):
            return result
        raw = (attempt / result["capture_file"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != result["capture_sha256"]:
            raise ValueError("Tiki capture changed")
        records = json.loads(raw)
        if len(records["products"]) < job["minimum_records"]:
            raise RuntimeError("Source input shortage")
        return {
            "status": result["status"],
            "datasets": records,
            "capture_manifest": str(attempt / "manifest.json"),
            "finished_at": result["finished_at"],
        }
    if job["adapter"] == "web":
        from ingestion.web_capture import capture_web

        result = capture_web(
            folder.parent / "capture-state",
            parameters.get("seed_urls", ["https://tinhte.vn/"]),
            limit=parameters.get("limit", 50),
            max_pages=parameters.get("max_pages", 150),
            request_interval=parameters.get("delay_seconds", 1),
        )
        if len(result.get("records", [])) < job["minimum_records"]:
            raise RuntimeError("Source input shortage")
        return result
    from data.generator import generate_products, generate_reviews

    seed = parameters.get("seed", 42)
    catalogue = generate_products(seed=seed)
    count = parameters.get("products", 10)
    if type(count) is not int or not 1 <= count <= len(catalogue):
        raise ValueError("Requested products exceed the generator catalogue")
    products = catalogue[:count]
    generator = {
        "base_reviews": parameters.get("base_reviews", parameters.get("reviews", 100)),
        "inject_anomalies": parameters.get("inject_anomalies", False),
        "burst_count": parameters.get("burst_count", 60),
    }
    if (
        type(generator["base_reviews"]) is not int
        or generator["base_reviews"] < 0
        or type(generator["burst_count"]) is not int
        or generator["burst_count"] < 0
        or type(generator["inject_anomalies"]) is not bool
    ):
        raise ValueError("Explicit nonnegative simulation sizes and boolean anomaly policy required")
    reviews = generate_reviews(
        products,
        count=generator["base_reviews"],
        seed=seed,
        inject_anomalies=generator["inject_anomalies"],
        burst_count=generator["burst_count"],
    )
    from orchestration.run_manifest import utc_now

    return {
        "status": "complete",
        "datasets": {"products": products, "reviews": reviews, "prices": []},
        "generator_seed": seed,
        "generator_parameters": generator,
        "finished_at": utc_now(),
    }


def commerce_input(job, captured):
    """Keep an empty new-review delta honest while providing explicit old context.

    This is an opt-in snapshot replay of previously accepted observations, with
    original observation/collection clocks. It never fills an empty first crawl
    with samples, and capture metrics still report zero newly observed reviews.
    """
    datasets = copy.deepcopy(captured["datasets"])
    if datasets["reviews"]:
        return datasets, 0
    if not job.get("parameters", {}).get("allow_review_context_replay"):
        raise ValueError("Explicit review context policy required for an empty delta")
    from storage.storage_manager import storage
    from ingestion.provenance import classify_source_group

    frame = storage.read_silver_parquet("reviews")
    if frame.empty:
        raise ValueError("No accepted review context exists; source remains pending")
    if classify_source_group(frame) != job["source_group"]:
        raise ValueError("Review context source group differs from source job")
    records = json.loads(frame.to_json(orient="records", date_format="iso"))
    for row in records:
        row["original_acquisition_method"] = row.get("original_acquisition_method") or row.get("acquisition_method")
        row["acquisition_method"] = "snapshot_replay"
    datasets["reviews"] = records
    return datasets, len(records)


def process_source(job, captured, run_id):
    from config.settings import settings

    if os.environ.get("SOURCE_JOB_NAMESPACE") != job["namespace"]:
        raise ValueError("Runtime environment not bound to the configured namespace")
    if settings.runtime_mode not in ("integration", "production"):
        raise ValueError("Scheduled source jobs require real services")
    if job["adapter"] == "web":
        from orchestration.article_pipeline import run_article_pipeline

        records = captured["records"]
        return run_article_pipeline(records, mode=settings.runtime_mode, run_id=run_id)
    from scripts.run_e2e_pipeline import run_end_to_end

    datasets, replayed = commerce_input(job, captured)
    with tempfile.TemporaryDirectory(prefix="source-job-") as temporary:
        paths = {}
        for dataset, rows in datasets.items():
            file = Path(temporary) / (dataset + ".json")
            file.write_text(json.dumps(rows, ensure_ascii=False))
            paths[dataset + "_path"] = str(file)
        return {
            "completed": bool(run_end_to_end(mode=settings.runtime_mode, run_id=run_id, **paths)),
            "context_reviews_replayed": replayed,
            "new_reviews_observed": len(captured["datasets"]["reviews"]),
        }
