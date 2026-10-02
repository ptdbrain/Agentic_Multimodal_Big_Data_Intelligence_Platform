"""SentinelAI Production Metrics Exporter.
Collects and formats operational metrics across Kafka, MinIO Data Lake, Spark Batch, and DW.
Supports Prometheus exposition format and JSON health telemetry.
"""
import time
import sys
from pathlib import Path
from typing import Dict, Any, List

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage
from dashboard.db_connector import DashboardDB

class SentinelMetricsExporter:
    """Collects multi-tier platform telemetry and exports Prometheus text format."""
    
    @classmethod
    def collect_all_metrics(cls) -> Dict[str, Any]:
        """Gathers system health and processing metrics."""
        db = DashboardDB()
        lake_stats = storage.get_storage_stats()
        kpis = db.get_kpi_summary()
        dq_df = db.get_data_quality_metrics()
        anom_df = db.get_anomaly_events()

        avg_dq = kpis.get("avg_dq_score", 100.0)
        total_anoms = len(anom_df) if not anom_df.empty else 0

        return {
            "timestamp": time.time(),
            "datalake": {
                "bronze_bytes": lake_stats.get("bronze", {}).get("size_bytes", 0),
                "bronze_files": lake_stats.get("bronze", {}).get("files", 0),
                "silver_bytes": lake_stats.get("silver", {}).get("size_bytes", 0),
                "silver_files": lake_stats.get("silver", {}).get("files", 0),
                "gold_bytes": lake_stats.get("gold", {}).get("size_bytes", 0),
                "gold_files": lake_stats.get("gold", {}).get("files", 0),
            },
            "warehouse": {
                "total_products": kpis.get("total_products", 0),
                "total_reviews": kpis.get("total_reviews", 0),
                "total_brands": kpis.get("total_brands", 0),
                "avg_rating": kpis.get("avg_rating", 0.0),
                "avg_dq_score": avg_dq,
                "active_anomalies": total_anoms
            }
        }

    @classmethod
    def generate_prometheus_metrics(cls) -> str:
        """Renders standard Prometheus exposition format."""
        data = cls.collect_all_metrics()
        lake = data["datalake"]
        wh = data["warehouse"]

        lines = [
            "# HELP sentinel_datalake_size_bytes Total bytes stored across Data Lake tiers",
            "# TYPE sentinel_datalake_size_bytes gauge",
            f'sentinel_datalake_size_bytes{{tier="bronze"}} {lake["bronze_bytes"]}',
            f'sentinel_datalake_size_bytes{{tier="silver"}} {lake["silver_bytes"]}',
            f'sentinel_datalake_size_bytes{{tier="gold"}} {lake["gold_bytes"]}',
            "",
            "# HELP sentinel_datalake_file_count Total objects stored in Data Lake",
            "# TYPE sentinel_datalake_file_count gauge",
            f'sentinel_datalake_file_count{{tier="bronze"}} {lake["bronze_files"]}',
            f'sentinel_datalake_file_count{{tier="silver"}} {lake["silver_files"]}',
            f'sentinel_datalake_file_count{{tier="gold"}} {lake["gold_files"]}',
            "",
            "# HELP sentinel_data_quality_score Current average DQ score percentage",
            "# TYPE sentinel_data_quality_score gauge",
            f'sentinel_data_quality_score {wh["avg_dq_score"]}',
            "",
            "# HELP sentinel_warehouse_records Total records in Data Warehouse entities",
            "# TYPE sentinel_warehouse_records gauge",
            f'sentinel_warehouse_records{{entity="products"}} {wh["total_products"]}',
            f'sentinel_warehouse_records{{entity="reviews"}} {wh["total_reviews"]}',
            f'sentinel_warehouse_records{{entity="brands"}} {wh["total_brands"]}',
            "",
            "# HELP sentinel_active_anomalies Number of active anomaly incidents detected",
            "# TYPE sentinel_active_anomalies gauge",
            f'sentinel_active_anomalies {wh["active_anomalies"]}',
            ""
        ]
        return "\n".join(lines)

if __name__ == "__main__":
    print(SentinelMetricsExporter.generate_prometheus_metrics())
