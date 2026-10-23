import sys
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from typing import List, Tuple, Dict, Any, Optional
from config.settings import settings


class TopicManager:
    """Unified Kafka Topic Provisioning and Management.
    Ensures all canonical ingestion topics and the Dead Letter Queue topic
    are provisioned with defined partition counts and replication factors.
    """

    @classmethod
    def get_topic_definitions(cls) -> List[Tuple[str, int]]:
        """Returns canonical list of (topic_name, num_partitions)."""
        return [
            (settings.kafka.topic_products, 2),
            (settings.kafka.topic_reviews, 3),
            (settings.kafka.topic_prices, 2),
            (settings.kafka.topic_events, 3),
            ("dead-letter", 1),
        ]

    @classmethod
    def ensure_topics(
        cls,
        bootstrap_servers: Optional[str] = None,
        timeout_ms: int = 5000,
        mode: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Provisions missing Kafka topics idempotently.
        In integration mode: connects to Kafka broker via KafkaAdminClient and creates topics.
        In local/test mode: registers topics in simulated in-memory registry if broker is offline.
        """
        servers = bootstrap_servers or settings.kafka.bootstrap_servers
        resolved_mode = mode or settings.runtime_mode
        topic_configs = cls.get_topic_definitions()

        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError

            admin_client = KafkaAdminClient(
                bootstrap_servers=servers,
                client_id="sentinel-admin",
                request_timeout_ms=timeout_ms,
            )

            existing_topics = set(admin_client.list_topics())
            to_create = []

            for topic_name, partitions in topic_configs:
                if topic_name not in existing_topics:
                    to_create.append(
                        NewTopic(name=topic_name, num_partitions=partitions, replication_factor=1)
                    )

            created = []
            if to_create:
                for new_t in to_create:
                    try:
                        admin_client.create_topics(new_topics=[new_t], validate_only=False)
                        created.append(new_t.name)
                    except TopicAlreadyExistsError:
                        pass
                print(f"[TopicManager] Created Kafka topics: {created}")
            else:
                print("[TopicManager] All canonical topics already exist.")

            admin_client.close()
            return {
                "status": "ok",
                "broker": servers,
                "created": created,
                "existing": list(existing_topics),
            }

        except Exception as e:
            if resolved_mode in ("integration", "production"):
                raise ConnectionError(
                    f"Failed to provision Kafka topics at '{servers}': {e}. Refusing to proceed in integration mode."
                )
            print(f"[TopicManager Notice] Kafka broker unreachable at '{servers}' ({e}). Using simulated topic registry.")
            return {
                "status": "simulated",
                "broker": servers,
                "created": [t[0] for t in topic_configs],
                "existing": [],
            }


def setup_topics(bootstrap_servers: Optional[str] = None) -> Dict[str, Any]:
    """Backward-compatible entry point for scripts/DAGs."""
    return TopicManager.ensure_topics(bootstrap_servers=bootstrap_servers)


if __name__ == "__main__":
    setup_topics()
