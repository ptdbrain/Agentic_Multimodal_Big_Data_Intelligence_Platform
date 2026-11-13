"""Require completed sink offsets before advancing SQL stream serving heartbeat."""

import json


def _offsets(progress):
    result = {}
    for source in progress.get("sources", []):
        value = source.get("endOffset")
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except ValueError:
                return None
        if not isinstance(value, dict):
            return None

        def flatten(value, prefix=""):
            for key, item in value.items():
                name = prefix + "/" + str(key)
                if isinstance(item, dict):
                    yield from flatten(item, name)
                elif type(item) is int:
                    yield name, item
                else:
                    raise ValueError("Unknown source offset shape")

        result.update(flatten(value, source.get("description", "source")))
    return result or None


def committed_serving_progress(window: dict, other_sinks: list[dict]) -> dict | None:
    ends = _offsets(window)
    if not ends or not other_sinks:
        return None
    for sink in other_sinks:
        if not sink:
            return None
        offsets = _offsets(sink)
        if not offsets or set(offsets) != set(ends) or any(offsets[key] < value for key, value in ends.items()):
            return None
    return window
