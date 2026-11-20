from datetime import datetime
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit


def test_heartbeat_waits_for_each_current_run_and_ignores_reordered_events():
    from spark.streaming.liveness import QueryHeartbeats
    from pyspark.sql.streaming.listener import QueryIdleEvent

    listener = QueryHeartbeats(["window-run", "quarantine-run"])
    def idle(run, seconds):
        listener.onQueryIdle(QueryIdleEvent("persistent-id", run, f"2026-11-20T00:00:{seconds:02d}Z"))

    idle("previous-run", 50)
    idle("window-run", 30)
    assert listener.frontier() is None
    listener.onQueryProgress(SimpleNamespace(progress=SimpleNamespace(runId="quarantine-run", timestamp="2026-11-20T00:00:20Z")))
    expected = datetime.fromisoformat("2026-11-20T00:00:20+00:00").timestamp()
    assert listener.frontier() == expected
    idle("quarantine-run", 10)
    assert listener.frontier() == expected
    idle("quarantine-run", 40)
    assert listener.frontier() == expected + 10
    listener.onQueryTerminated(SimpleNamespace(runId="quarantine-run"))
    assert listener.frontier() is None
