"""Measure an isolated 100k-review processing run through Spark, SQLite and DashboardDB.

Synthetic acceptance workload; excludes Kafka/MinIO/PostgreSQL and UI rendering.
Run from btl: python scripts/benchmark_processing.py --output /path/to/new-report.json
"""
import argparse
import hashlib
import json
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def run_benchmark(output: Path):
    if output.exists():
        raise FileExistsError(f'Report already exists: {output}')
    from config.settings import settings
    settings.set_runtime_mode('test')
    work = Path(tempfile.mkdtemp(prefix='sentinel-fullpath-100k-'))
    settings.storage.local_data_dir=work/'lake'
    settings.database.sqlite_path=work/'warehouse.sqlite'
    from storage.storage_manager import storage
    storage.set_mode('test',base_dir=work/'lake')
    from spark.spark_session import SparkSessionFactory
    from spark.batch.spark_etl_job import SparkBatchETLJob
    from scripts.run_e2e_pipeline import stage_5_spark_gold_marts
    from database.export_gold import export_gold_to_db
    from dashboard.db_connector import DashboardDB
    from scripts.benchmark import ResourceMonitor

    spark=SparkSessionFactory.get_spark_session(master='local[2]')
    spark.range(1).count()
    start_date=datetime(2026,10,1,tzinfo=timezone.utc)
    products=[{'product_id':f'p{i:03}','product_name':f'Acceptance product {i}', 'brand':f'Brand{i%5}',
               'category':f'Category{i%4}', 'price':str(1000+i), 'currency':'VND',
               'created_at':start_date.isoformat(), 'ingested_at':start_date.isoformat()} for i in range(100)]
    reviews=[{'review_id':f'r{i:06}', 'product_id':f'p{i%100:03}', 'rating':str(1+i%5),
              'review_text':f'Synthetic full-path acceptance observation {i}',
              'review_date':(start_date+timedelta(seconds=(i//100)*1800)).isoformat(),
              'ingested_at':start_date.isoformat()} for i in range(92000)]
    reviews += [dict(reviews[i]) for i in range(3000)]
    reviews += [{'review_id':f'bad{i:06}', 'product_id':f'p{i%100:03}', 'rating':'9', 'review_text':'Invalid acceptance rating',
                 'review_date':start_date.isoformat(), 'ingested_at':start_date.isoformat()} for i in range(5000)]
    prices=[{'price_id':f'price{i:06}','product_id':f'p{i%100:03}','price':str(1000+(i%100)+i//100), 'currency':'VND',
             'timestamp':(start_date+timedelta(hours=i//100)).isoformat(), 'ingested_at':start_date.isoformat()} for i in range(10000)]
    run_id='fullpath-100k-'+work.name
    monitor=ResourceMonitor(); monitor.start(); times={}; paths={}; inputs={}
    try:
        started=time.perf_counter()
        for name,rows in [('products',products),('reviews',reviews),('prices',prices)]:
            path=work/f'{name}.jsonl'
            with path.open('x') as stream:
                for row in rows:
                    stream.write(json.dumps(row)+'\n')
            paths[name]=str(path)
            inputs[name]={'records':len(rows),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
        times['bronze_json_write_sec']=time.perf_counter()-started
        started=time.perf_counter()
        result=SparkBatchETLJob(spark).run_pipeline(products_source=paths['products'], reviews_source=paths['reviews'],
                                                  prices_source=paths['prices'], enforce_dq_gate=True,run_id=run_id)
        times['spark_etl_sec']=time.perf_counter()-started
        r=result['reconciliation']['reviews']
        assert r['input_count']==100000 and r['silver_written_count']==92000
        assert r['quarantine_count']==5000 and r['intra_batch_duplicate_count']==3000
        assert r['reconciled'] and result['dq_gate']['passed']
        started=time.perf_counter()
        gold=stage_5_spark_gold_marts(run_id=run_id,
            dq_metrics=[result['products_dq'],result['reviews_dq'],result['prices_dq']], required_datasets=['products','reviews','prices'])
        times['spark_gold_and_anomalies_sec']=time.perf_counter()-started
        started=time.perf_counter()
        exported=export_gold_to_db(mode='test')
        times['sqlite_export_sec']=time.perf_counter()-started
        started=time.perf_counter()
        db=DashboardDB(mode='test')
        try:
            actual_reviews=int(db.query('SELECT COUNT(*) AS n FROM reviews').iloc[0]['n'])
            mart_counts={name:int(db.query(f'SELECT COUNT(*) AS n FROM {name}').iloc[0]['n']) for name in
                         ['product_daily_stats','brand_daily_stats','category_daily_stats','price_daily_stats']}
            assert actual_reviews==92000
            assert int(db.query('SELECT SUM(review_count) AS n FROM product_daily_stats').iloc[0]['n'])==92000
            assert int(db.query('SELECT SUM(data_points) AS n FROM price_daily_stats').iloc[0]['n'])==10000
            assert len(db.get_data_quality_metrics())==3
            for name in mart_counts:
                assert mart_counts[name]>0
                assert db.query(f'SELECT DISTINCT run_id FROM {name}')['run_id'].tolist()==[run_id]
        finally:
            db.close()
        times['dashboard_query_sec']=time.perf_counter()-started
        measured=monitor.stop()
        report={'completed':True,'executed_at':datetime.now(timezone.utc).isoformat(),'mode':'local Spark + SQLite',
                'boundary':'Bronze JSONL write -> Spark normalize/DQ/quarantine/dedup/Silver -> Spark Gold + pandas anomalies -> SQLite -> DashboardDB queries',
                'excluded':['Kafka transport','MinIO','PostgreSQL','browser-rendering load','input generation','JVM startup/warmup'],
                'iterations':1,'generator':'deterministic synthetic fixture, 100000 review inputs plus 100 products and 10000 price observations',
                'input':inputs,'review_accounting':{key:r[key] for key in ['input_count','valid_input_count','quarantine_count','intra_batch_duplicate_count','cross_batch_duplicate_count','silver_written_count','reconciled']},
                'times_sec':{key:round(value,4) for key,value in times.items()}, 'resources':measured,
                'mart_rows':mart_counts,'anomaly_events':gold['anomalies_count'],'exported_rows':exported,
                'serving_review_count':actual_reviews,'production_data_modified':False,'isolated_workspace':str(work)}
        output=Path(output)
        with output.open('x') as stream:
            json.dump(report,stream,ensure_ascii=False,indent=2,allow_nan=False);stream.write('\n')
        print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
    finally:
        monitor.stop()
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='New report path; never overwritten')
    args = parser.parse_args()
    run_benchmark(args.output)


if __name__ == '__main__':
    main()
