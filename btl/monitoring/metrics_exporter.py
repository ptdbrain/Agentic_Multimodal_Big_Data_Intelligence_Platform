"""SentinelAI Production Metrics Exporter.
Collects and formats operational metrics across Kafka, MinIO Data Lake, Spark Batch, and DW.
Supports Prometheus exposition format and JSON health telemetry.
"""
import time
import argparse
import math
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage
from dashboard.db_connector import DashboardDB

class SentinelMetricsExporter:
    """Collects multi-tier platform telemetry and exports Prometheus text format."""
    
    @classmethod
    def collect_all_metrics(cls, mode: Optional[str] = None) -> Dict[str, Any]:
        """Gathers system health and processing metrics."""
        db = DashboardDB(mode=mode)
        try:
            lake_stats = storage.get_storage_stats()
            kpis = db.get_kpi_summary()
            anom_df = db.get_anomaly_events()
        finally:
            db.close()
        avg_dq = kpis.get("avg_dq_score")
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
                "recent_anomalies": total_anoms
            }
        }

    @classmethod
    def generate_prometheus_metrics(cls, mode: Optional[str] = None) -> str:
        """Renders standard Prometheus exposition format."""
        data = cls.collect_all_metrics(mode=mode)
        lake = data["datalake"]
        wh = data["warehouse"]

        quality = wh["avg_dq_score"]
        quality = str(quality) if quality is not None and math.isfinite(float(quality)) else "NaN"

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
            f'sentinel_data_quality_score {quality}',
            "",
            "# HELP sentinel_warehouse_records Total records in Data Warehouse entities",
            "# TYPE sentinel_warehouse_records gauge",
            f'sentinel_warehouse_records{{entity="products"}} {wh["total_products"]}',
            f'sentinel_warehouse_records{{entity="reviews"}} {wh["total_reviews"]}',
            f'sentinel_warehouse_records{{entity="brands"}} {wh["total_brands"]}',
            "",
            "# HELP sentinel_recent_anomalies Number of recent anomaly records returned (up to 100; includes history)",
            "# TYPE sentinel_recent_anomalies gauge",
            f'sentinel_recent_anomalies {wh["recent_anomalies"]}',
            ""
        ]
        return "\n".join(lines)

def create_metrics_server(host="127.0.0.1", port=8000, mode=None, metrics_provider=None):
    """Return a server; callers own its serve/shutdown lifecycle."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            try:
                provider = metrics_provider or (lambda: SentinelMetricsExporter.generate_prometheus_metrics(mode=mode))
                body = provider().encode("utf-8")
            except Exception:
                # A failed scrape must not look like zero/healthy metrics.
                self.send_error(503, "Metrics collection unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    return ThreadingHTTPServer((host, port), Handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export measured Sentinel metrics")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    if args.serve:
        with create_metrics_server(args.host, args.port) as server:
            server.serve_forever()
    else:
        print(SentinelMetricsExporter.generate_prometheus_metrics())
