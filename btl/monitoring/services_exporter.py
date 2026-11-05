"""Read-only Prometheus probes of actual Kafka, MinIO, PostgreSQL and Elasticsearch.

No offline or memory backend. Missing measurements are NaN; a failed dependency
probe is an observed up=0. Probe exceptions never expose credential text.
"""
import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))

from config.settings import settings


def kafka_probe():
    from kafka.admin import KafkaAdminClient
    client = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers,
        client_id='sentinel-service-metrics',request_timeout_ms=3000)
    try:
        count = len(client.describe_cluster()['brokers'])
        if count < 1:
            raise RuntimeError('No reachable Kafka broker')
        return {'broker_count':count}
    finally:
        client.close()


def minio_probe():
    from minio import Minio
    from urllib3 import PoolManager, Timeout
    pool = PoolManager(timeout=Timeout(connect=2,read=3),retries=False)
    client = Minio(settings.storage.endpoint,access_key=settings.storage.access_key,
                   secret_key=settings.storage.secret_key,secure=settings.storage.secure,
                   http_client=pool)
    # bucket_exists is a real server RPC; absence does not mean MinIO is down.
    try:
        return {'configured_bucket_exists':client.bucket_exists(settings.storage.data_lake_bucket)}
    finally:
        pool.clear()


def postgres_probe():
    import psycopg2
    connection = psycopg2.connect(host=settings.database.host,port=settings.database.port,
        user=settings.database.user,password=settings.database.password,dbname=settings.database.database,connect_timeout=3)
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_database_size(current_database())')
            return {'database_size_bytes':int(cursor.fetchone()[0])}
    finally:
        connection.close()


def elasticsearch_probe():
    import requests
    endpoint = (os.environ.get('ES_URL') or f'http://{settings.search.es_host}:{settings.search.es_port}').rstrip('/')
    response = requests.get(endpoint + '/_cluster/health',timeout=3)
    response.raise_for_status()
    return {'health_code':{'red':0,'yellow':1,'green':2}[response.json()['status']]}


class ServiceMetricsExporter:
    @staticmethod
    def probes():
        return {'kafka':kafka_probe,'minio':minio_probe,'postgres':postgres_probe,'elasticsearch':elasticsearch_probe}

    @classmethod
    def generate_prometheus_metrics(cls):
        observed = {}
        lines = ['# HELP sentinel_dependency_up Successful real protocol probe (1) or failed probe (0)',
                 '# TYPE sentinel_dependency_up gauge']
        for name,probe in cls.probes().items():
            try:
                observed[name] = probe()
                up = 1
            except Exception:
                observed[name] = {}
                up = 0
            lines.append(f'sentinel_dependency_up{{service="{name}"}} {up}')
        definitions = [
            ('kafka','broker_count','sentinel_kafka_broker_count','Observed brokers; unknown when discovery fails'),
            ('minio','configured_bucket_exists','sentinel_minio_configured_bucket_exists','Configured bucket exists (1) or absent (0), unknown on probe failure'),
            ('postgres','database_size_bytes','sentinel_postgres_database_size_bytes','Measured size of configured PostgreSQL database'),
            ('elasticsearch','health_code','sentinel_elasticsearch_health_code','Measured cluster health: red=0 yellow=1 green=2, unknown on failure'),
        ]
        for service,key,name,help_text in definitions:
            value = observed.get(service,{}).get(key)
            value = 'NaN' if value is None else int(value)
            lines.extend([f'# HELP {name} {help_text}',f'# TYPE {name} gauge',f'{name} {value}'])
        return '\n'.join(lines) + '\n'


def main():
    from monitoring.metrics_exporter import create_metrics_server
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--serve',action='store_true')
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8001)
    args = parser.parse_args()
    if args.serve:
        with create_metrics_server(args.host,args.port,metrics_provider=ServiceMetricsExporter.generate_prometheus_metrics) as server:
            server.serve_forever()
    else:
        print(ServiceMetricsExporter.generate_prometheus_metrics(),end='')


if __name__ == '__main__':
    main()
