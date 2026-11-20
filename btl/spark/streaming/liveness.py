"""Track Spark trigger activity independently of input rows and SQL publication."""

from collections.abc import Iterable
from datetime import datetime
from threading import Lock

from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent


class QueryHeartbeats(StreamingQueryListener):
    def __init__(self, run_ids: Iterable[str]) -> None:
        self.observed: dict[str, float | None] = {run_id: None for run_id in run_ids}
        if not self.observed:
            raise ValueError("At least one active Spark query is required")
        self.lock = Lock()
        self.terminated = False

    def _record(self, run_id: str, timestamp: str) -> None:
        clock = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if clock.tzinfo is None:
            raise ValueError("Spark trigger timestamp must include its timezone")
        with self.lock:
            if run_id in self.observed:
                previous = self.observed[run_id]
                self.observed[run_id] = max(previous, clock.timestamp()) if previous is not None else clock.timestamp()

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        pass

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        self._record(str(event.progress.runId), event.progress.timestamp)

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        self._record(str(event.runId), event.timestamp)

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        with self.lock:
            if str(event.runId) in self.observed:
                self.terminated = True

    def frontier(self) -> float | None:
        with self.lock:
            if self.terminated or None in self.observed.values():
                return None
            return min(value for value in self.observed.values() if value is not None)
