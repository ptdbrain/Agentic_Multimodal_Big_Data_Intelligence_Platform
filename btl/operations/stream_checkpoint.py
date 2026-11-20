"""Read committed Spark Kafka offsets for a quiesced single-source review deployment."""

import json

from serving.lake import DatasetLake


def committed_frontiers(
    lake: DatasetLake, prefix: str, sinks: list[str], broker_end: dict[str, dict[str, int]], sql_batch: int | None
) -> dict:
    """Require every sink at broker end and the SQL frontier at the committed window batch."""
    if not broker_end or any(not partitions for partitions in broker_end.values()):
        raise ValueError("Kafka end offsets must cover the registered topic and partitions")
    if any(type(end) is not int or end < 0 for offsets in broker_end.values() for end in offsets.values()):
        raise ValueError("Invalid Kafka offsets")
    if "reviews_window" not in sinks or "reviews_quarantine" not in sinks or len(set(sinks)) != len(sinks):
        raise ValueError("All registered stream sinks are required")
    result = {}
    for sink in sinks:
        root = f"{prefix}/{sink}"
        latest = {}
        for kind in ("commits", "offsets"):
            folder = f"{root}/{kind}/"
            batches = [int(key[len(folder):]) for key, _ in lake.objects(folder) if key[len(folder):].isdecimal()]
            latest[kind] = max(batches, default=None)
        batch = latest["commits"]
        if batch != latest["offsets"]:
            raise ValueError(f"{sink} has an incomplete Spark batch")
        if batch is None:
            if sql_batch is not None or any(end for partitions in broker_end.values() for end in partitions.values()):
                raise ValueError(f"{sink} is missing committed checkpoints")
        else:
            commit = lake.read_bytes(f"{root}/commits/{batch}").decode().splitlines()
            offsets = lake.read_bytes(f"{root}/offsets/{batch}").decode().splitlines()
            if len(commit) != 2 or len(offsets) != 3 or commit[0] != "v1" or offsets[0] != "v1":
                raise ValueError(f"{sink} has an unsupported Spark checkpoint format")
            if not isinstance(json.loads(commit[1]), dict) or not isinstance(json.loads(offsets[1]), dict):
                raise ValueError(f"{sink} has malformed checkpoint metadata")
            actual = json.loads(offsets[2])
            if json.dumps(actual, sort_keys=True) != json.dumps(broker_end, sort_keys=True):
                raise ValueError(f"{sink} has not committed the Kafka end offsets")
        if sink == "reviews_window" and batch != sql_batch:
            raise ValueError("SQL has not confirmed the latest Spark window batch")
        result[sink] = {"batch_id": batch, "end_offsets": broker_end}
    return result
