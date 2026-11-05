"""Measure labelled synthetic data through the actual six-stage service pipeline.

Each process uses a fresh database, bucket and Kafka topic group. No real-source
serving data is replaced. Services must already be running; no offline fallback.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _review(index, invalid=False):
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    return {'review_id':f'sim_bad_{index}' if invalid else f'sim_r{index:09}',
            'product_id':f'sim_p{index % 100:03}', 'user_id':'anon_redacted',
            'review_title':'Synthetic workload', 'rating':1 + index % 5,
            'review_text':f'Explicit synthetic full-path observation {index}',
            'review_date':'unparseable' if invalid else
                (start + timedelta(seconds=(index // 100) * 1800)).isoformat()}


def write_workload(directory: Path, review_inputs: int):
    """Stream exact 92% valid / 3% duplicate / 5% bad-clock review inputs."""
    if review_inputs < 100 or review_inputs % 100:
        raise ValueError('Review inputs must be a positive multiple of 100')
    valid = review_inputs * 92 // 100
    duplicates = review_inputs * 3 // 100
    invalid = review_inputs * 5 // 100
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    config = json.dumps({'review_inputs':review_inputs, 'version':1}, sort_keys=True)

    def rows(dataset):
        if dataset == 'products':
            for index in range(100):
                yield {'product_id':f'sim_p{index:03}', 'product_name':f'Synthetic product {index}',
                       'brand':f'FixtureBrand{index % 5}', 'category':f'FixtureCategory{index % 4}',
                       'price':1000 + index, 'currency':'VND', 'created_at':start.isoformat()}
        elif dataset == 'prices':
            for index in range(review_inputs // 10):
                yield {'price_id':f'sim_price{index:09}', 'product_id':f'sim_p{index % 100:03}',
                       'price':1000 + index % 100 + index // 100, 'currency':'VND',
                       'timestamp':(start + timedelta(hours=index // 100)).isoformat()}
        else:
            for index in range(valid):
                yield _review(index)
            for index in range(duplicates):
                yield _review(index)
            for index in range(invalid):
                yield _review(index, invalid=True)

    manifest = {'expected_reviews':{'input':review_inputs,'silver':valid,'quarantine':invalid,'duplicates':duplicates}}
    paths = {}
    for dataset in ('products','reviews','prices'):
        path = Path(directory) / f'{dataset}.jsonl'
        digest, count = hashlib.sha256(), 0
        with path.open('xb') as stream:
            for row in rows(dataset):
                row.update(source='synthetic_fullpath_benchmark', source_group='simulation',
                           source_name='fullpath_benchmark_v1', acquisition_method='generated',
                           generator_seed='42', generator_config=config)
                encoded = (json.dumps(row, sort_keys=True, ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
                stream.write(encoded)
                digest.update(encoded)
                count += 1
        paths[dataset + '_path'] = path
        manifest[dataset] = {'records':count,'sha256':digest.hexdigest()}
    return paths, manifest


def source_fingerprint():
    digest = hashlib.sha256()
    count = 0
    for path in sorted(ROOT.rglob('*')):
        relative = path.relative_to(ROOT)
        if (not path.is_file() or path.suffix not in {'.py','.json','.sql','.toml','.txt','.yml','.yaml'}
                or any(part in {'reports','.git','__pycache__','.pytest_cache','datalake','.superpowers'} for part in relative.parts)):
            continue
        digest.update(str(relative).encode() + b'\0' + path.read_bytes() + b'\0')
        count += 1
    return {'sha256':digest.hexdigest(),'files':count,
            'scope':'sorted source/config/schema/dependency files; excludes reports/caches/lake/git/workspace'}


def run_benchmark(review_inputs: int, output: Path):
    # Exclusive reservation precedes imports, settings changes or external writes.
    with Path(output).open('x', encoding='utf-8') as report_stream:
        report = {'completed':False,'status':'starting','executed_at':datetime.now(timezone.utc).isoformat(),
                  'review_inputs':review_inputs,'source':'labelled deterministic synthetic workload',
                  'source_fingerprint':source_fingerprint(), 'times_sec':{}}

        def persist():
            report_stream.seek(0)
            json.dump(report, report_stream, indent=2, allow_nan=False)
            report_stream.write('\n')
            report_stream.truncate()
            report_stream.flush()
            os.fsync(report_stream.fileno())

        monitor = spark = None
        persist()
        try:
            import psycopg2
            from psycopg2 import sql
            from config.settings import settings
            from storage.storage_manager import storage
            from spark.spark_session import SparkSessionFactory
            from scripts.benchmark import ResourceMonitor
            from scripts.run_e2e_pipeline import (
                configure_pipeline_mode, stage_1_ingest_sources, stage_2_kafka_produce_and_consume,
                stage_3_verify_bronze, stage_4_spark_batch_silver_dq,
                stage_5_spark_gold_marts, stage_6_warehouse_and_dashboard,
            )
            from dashboard.db_connector import DashboardDB
            work = Path(tempfile.mkdtemp(prefix='sentinel-fullpath-'))
            paths, inputs = write_workload(work, review_inputs)
            report['inputs'] = inputs
            report['workspace'] = str(work)
            suffix = uuid.uuid4().hex[:12]
            database = 'source_load_' + suffix
            connection = psycopg2.connect(host=settings.database.host,port=settings.database.port,
                user=settings.database.user,password=settings.database.password,dbname=settings.database.database)
            connection.autocommit = True
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
            finally:
                connection.close()
            settings.database.database = database
            settings.storage.data_lake_bucket = 'source-load-' + suffix
            for dataset in ('products','reviews','prices','events'):
                setattr(settings.kafka, 'topic_' + dataset, 'source-load-' + suffix + '-' + dataset)
            settings.kafka.group_id = 'source-load-' + suffix
            report['namespace'] = {'database':database,'bucket':settings.storage.data_lake_bucket,
                'topics':{dataset:getattr(settings.kafka,'topic_' + dataset) for dataset in ('products','reviews','prices')}}
            persist()
            configure_pipeline_mode('integration')
            spark = SparkSessionFactory.get_spark_session(master=os.environ.get('SPARK_MASTER','local[2]'))
            spark.range(1).count()
            monitor = ResourceMonitor()
            monitor.start()
            started = time.perf_counter()

            def timed(stage, callback):
                report['status'] = stage
                persist()
                before = time.perf_counter()
                result = callback()
                report['times_sec'][stage] = round(time.perf_counter() - before, 4)
                persist()
                print(f'Completed {stage}: {report["times_sec"][stage]} sec', flush=True)
                return result

            loaded = timed('source_load_pii', lambda: stage_1_ingest_sources(**{k:str(v) for k,v in paths.items()}))
            transport = timed('kafka_bronze', lambda: stage_2_kafka_produce_and_consume(
                loaded['products_raw'],loaded['reviews_raw'],loaded['prices_raw'],effective_mode='integration'))
            expected_sent = sum(inputs[name]['records'] for name in ('products','reviews','prices'))
            if (transport['records_consumed'] != expected_sent or transport['records_valid'] != expected_sent
                    or transport['records_dlq'] != 0):
                raise RuntimeError('Transport accounting differs from schema-valid workload')
            bronze = timed('bronze_verify', lambda: stage_3_verify_bronze(transport,effective_mode='integration'))
            processed = timed('spark_silver_dq', lambda: stage_4_spark_batch_silver_dq(
                batches_by_topic=bronze['batches_by_topic'],include_prices=True,enforce_dq_gate=True))
            result = processed['etl_res']
            accounting = result['reconciliation']['reviews']
            expected = inputs['expected_reviews']
            for field, value in [('input_count',expected['input']),('silver_written_count',expected['silver']),
                                 ('quarantine_count',expected['quarantine']),('intra_batch_duplicate_count',expected['duplicates']),
                                 ('cross_batch_duplicate_count',0)]:
                if accounting[field] != value:
                    raise RuntimeError(f'Review reconciliation failed: {field}')
            if not accounting['reconciled'] or not result['dq_gate']['passed']:
                raise RuntimeError('Review accounting or DQ gate failed')
            run_id = result['run_id']
            gold = timed('spark_gold_anomalies', lambda: stage_5_spark_gold_marts(run_id=run_id,source_run_id=run_id,
                dq_metrics=[result['products_dq'],result['reviews_dq'],result['prices_dq']],
                required_datasets=['products','reviews','prices']))
            serving = timed('postgres_dashboard', lambda: stage_6_warehouse_and_dashboard(effective_mode='integration'))
            db = DashboardDB(mode='integration',data_scope='simulation')
            try:
                def number(query):
                    return int(db.query(query).iloc[0]['n'])
                if (number('SELECT COUNT(*) n FROM reviews') != expected['silver']
                        or number('SELECT SUM(review_count) n FROM product_daily_stats') != expected['silver']
                        or number('SELECT SUM(data_points) n FROM price_daily_stats') != inputs['prices']['records']
                        or serving['kpis']['total_reviews'] != expected['silver']
                        or db.get_kpi_summary()['total_reviews'] != expected['silver']):
                    raise RuntimeError('PostgreSQL/Dashboard accounting differs from workload')
                marts = {}
                for name in ('product_daily_stats','brand_daily_stats','category_daily_stats','price_daily_stats'):
                    frame = db.query(f'SELECT DISTINCT run_id,source_run_id,source_group,currency FROM {name}')
                    if (len(frame) != 1 or frame.iloc[0].to_dict() !=
                            {'run_id':run_id,'source_run_id':run_id,'source_group':'simulation','currency':'VND'}):
                        raise RuntimeError(f'Mart lineage differs: {name}')
                    marts[name] = number(f'SELECT COUNT(*) n FROM {name}')
                dq = db.get_data_quality_metrics()
                if len(dq) != 3:
                    raise RuntimeError('Missing current dataset DQ metrics')
                real = DashboardDB(mode='integration',data_scope='real')
                try:
                    if real.get_kpi_summary()['total_reviews'] != 0:
                        raise RuntimeError('Synthetic data leaked into the real dashboard scope')
                finally:
                    real.close()
            finally:
                db.close()
            report.update(completed=True,status='passed',run_id=run_id,
                review_accounting={key:accounting[key] for key in ('input_count','valid_input_count','quarantine_count',
                    'intra_batch_duplicate_count','cross_batch_duplicate_count','silver_written_count','reconciled')},
                transport={key:transport[key] for key in ('records_consumed','records_valid','records_dlq','source_contracts')},
                mart_rows=marts,anomaly_events=gold['anomalies_count'],serving_review_count=expected['silver'],
                wall_sec=round(time.perf_counter() - started,4),resources=monitor.stop(),
                boundary='JSONL load/PII -> actual Kafka -> MinIO Bronze -> Spark Silver/DQ/quarantine/dedup -> Gold/anomalies -> PostgreSQL -> scoped DashboardDB',
                excluded=['input generation','namespace setup','JVM startup/warmup','browser rendering','separately running service daemon resources'],
                resource_boundary='Python driver and local child process tree; no cluster-wide resource claim',
                real_source_serving_modified=False)
            persist()
            return report
        except BaseException as exc:
            report.update(completed=False,status='failed',failed_stage=report['status'],error_type=type(exc).__name__)
            if monitor is not None:
                report['resources_at_failure'] = monitor.stop()
            persist()
            raise
        finally:
            if monitor is not None:
                monitor.stop()
            if spark is not None:
                spark.stop()



def run_velocity(rate: int, duration: float, output: Path):
    """Actual Kafka/MinIO only; keep startup, dispatch and drain boundaries explicit."""
    with Path(output).open('x', encoding='utf-8') as stream:
        report = {'completed':False,'status':'starting','executed_at':datetime.now(timezone.utc).isoformat(),
                  'target_rate_mps':rate,'requested_duration_sec':duration,'source_fingerprint':source_fingerprint()}

        def persist():
            stream.seek(0)
            json.dump(report,stream,indent=2,allow_nan=False)
            stream.write('\n')
            stream.truncate()
            stream.flush()
            os.fsync(stream.fileno())

        persist()
        monitor = None
        try:
            from config.settings import settings
            from storage.storage_manager import storage
            from storage.minio_backend import MinIOBackend
            from scripts.benchmark import benchmark_velocity, ResourceMonitor
            suffix = uuid.uuid4().hex[:12]
            settings.storage.data_lake_bucket = 'source-velocity-' + suffix
            for dataset in ('products','reviews','prices','events'):
                setattr(settings.kafka,'topic_' + dataset,'source-velocity-' + suffix + '-' + dataset)
            settings.set_runtime_mode('integration')
            storage.set_mode('integration')
            if not isinstance(storage.backend,MinIOBackend) or storage.backend.enable_local_mirror:
                raise RuntimeError('Velocity acceptance requires MinIO without a local mirror')
            storage.health_check()
            report['namespace'] = {'bucket':settings.storage.data_lake_bucket,'topic':settings.kafka.topic_reviews}
            report['status'] = 'measuring'
            persist()
            monitor = ResourceMonitor()
            monitor.start()
            rows = benchmark_velocity(rates=[rate],seed=42,dirty_ratio=.04,effective_mode='integration',duration_sec=duration)
            report.update(completed=True,status='passed',result=rows[0],resources=monitor.stop(),
                          source='labelled synthetic generate_reviews(seed=42+rate), 4% invalid schema inputs',
                          boundary='Actual Kafka send/ACK -> broker -> RawConsumer -> validation -> MinIO Bronze or durable DLQ',
                          resource_boundary='Python process tree; excludes separate service daemons',
                          excluded=['source generation in produce/consume durations','Spark/Gold/PostgreSQL','browser rendering'],
                          latency_boundary='send to validation or durable sink; sink latency includes batch buffering',
                          real_source_serving_modified=False)
            persist()
            return report
        except BaseException as exc:
            report.update(completed=False,status='failed',error_type=type(exc).__name__)
            if monitor is not None:
                report['resources_at_failure'] = monitor.stop()
            persist()
            raise
        finally:
            if monitor is not None:
                monitor.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--velocity-rate',type=int,help='Single Kafka/MinIO rate test; requires --output')
    parser.add_argument('--velocity-rates',nargs='+',type=int,help='Kafka matrix, e.g. 50 200 500')
    parser.add_argument('--duration',type=float,default=120,help='Velocity dispatch window in seconds')
    parser.add_argument('--reviews',type=int,help='Single isolated run; positive multiple of 100')
    parser.add_argument('--output',type=Path,help='New single-run JSON; existing evidence never overwritten')
    parser.add_argument('--sizes',nargs='+',type=int,default=[10000,100000,1000000])
    parser.add_argument('--iterations',type=int,default=3)
    parser.add_argument('--output-dir',type=Path,help='New directory for sequential fresh-process matrix runs')
    args = parser.parse_args()
    if args.velocity_rate is not None:
        if args.output is None or args.output_dir is not None or args.reviews is not None:
            parser.error('--velocity-rate requires --output and no --reviews/--output-dir')
        run_velocity(args.velocity_rate,args.duration,args.output)
        return
    if args.reviews is not None:
        if args.output is None or args.output_dir is not None:
            parser.error('--reviews requires --output and no --output-dir')
        run_benchmark(args.reviews,args.output)
        return
    if args.output_dir is None or args.output is not None or args.iterations <= 0:
        parser.error('Matrix mode requires --output-dir and positive --iterations')
    args.output_dir.mkdir(parents=True,exist_ok=False)
    for size in (args.velocity_rates or args.sizes):
        for iteration in range(1,args.iterations + 1):
            kind = 'velocity' if args.velocity_rates else 'volume'
            report = args.output_dir / f'{kind}-{size}-{iteration}.json'
            arguments = (['--velocity-rate',str(size),'--duration',str(args.duration)] if args.velocity_rates
                         else ['--reviews',str(size)])
            with report.with_suffix('.log').open('x') as log:
                subprocess.run([sys.executable,str(Path(__file__).resolve()),*arguments,'--output',str(report)],
                               check=True,stdout=log,stderr=subprocess.STDOUT,cwd=ROOT)
            print(f'Passed {kind} {size}, iteration {iteration}',flush=True)


if __name__ == '__main__':
    main()
