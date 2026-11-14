"""Bounded pandas article ETL; independent of commerce ratings and marts.

Immutable lake objects are referenced by an SQL run manifest committed in the
same transaction as serving tables. This is not a cross-system transaction.
"""
import hashlib
import json
import math
import re
import uuid
from urllib.parse import urlsplit
import pandas as pd
from config.settings import settings
from data.schemas.validator import SchemaValidator
from database.warehouse import WarehouseManager, _database_value
from orchestration.run_manifest import canonical_writer
from storage.storage_manager import storage


ARTICLE_COLUMNS = ['article_id', 'source_group', 'source_name', 'source_url', 'acquisition_method',
                   'title', 'body', 'published_at', 'published_at_raw', 'publication_time_precision',
                   'collected_at', 'collected_at_raw', 'content_sha256', 'evidence_sha256', 'run_id',
                   'source_record_id','evidence_ref','parser_version','schema_version','collection_run_id',
                   'original_acquisition_method']
GOLD_COLUMNS = ['date', 'source_name', 'article_count', 'total_text_characters', 'source_group', 'run_id']


def _article_clock(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, 'unknown', True
    timestamp = pd.to_datetime(value, errors='coerce')
    if pd.isna(timestamp):
        return None, 'unknown', False
    if timestamp.tzinfo is None:
        precision = 'date' if re.fullmatch(r'\d{4}-\d{2}-\d{2}', str(value).strip()) else 'timezone_unknown'
        return None, precision, True
    return timestamp.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'instant', True


def prepare_articles(records, run_id):
    valid, quarantine = [], []
    for row in records:
        ok, errors = SchemaValidator.validate_record(row, 'article')
        if ok:
            raw_collected = row.get('collected_at_raw') or row['collected_at']
            raw_published = row.get('published_at_raw')
            if raw_published is None:
                raw_published = row.get('published_at')
            collected, collection_precision, collection_valid = _article_clock(raw_collected)
            published, publication_precision, publication_valid = _article_clock(raw_published)
            if not collection_valid or collection_precision != 'instant':
                errors.append('INVALID_COLLECTION_TIME')
            if not publication_valid:
                errors.append('INVALID_PUBLICATION_TIME')
            parts = urlsplit(row['source_url'])
            if parts.scheme not in ('http', 'https') or parts.hostname != row['source_name']:
                errors.append('SOURCE_DOMAIN_MISMATCH')
            if hashlib.sha256(row['body'].encode()).hexdigest() != row['content_sha256']:
                errors.append('CONTENT_HASH_MISMATCH')
        if errors:
            quarantine.append({'record': row, 'errors': errors})
        else:
            valid.append({**row, 'run_id': run_id, 'published_at': published,
                          'published_at_raw': raw_published, 'publication_time_precision': publication_precision,
                          'collected_at': collected, 'collected_at_raw': raw_collected})
    frame = pd.DataFrame(valid, columns=ARTICLE_COLUMNS)
    duplicates = superseded = 0
    if not frame.empty:
        frame['_collected'] = pd.to_datetime(frame['collected_at'], utc=True, format='mixed')
        ambiguous = frame.groupby(['article_id', '_collected'])['content_sha256'].transform('nunique').gt(1)
        for index in frame[ambiguous].index:
            quarantine.append({'record': valid[index], 'errors': ['AMBIGUOUS_ARTICLE_VERSION']})
        frame = frame[~ambiguous]
        duplicates = int(frame.duplicated().sum())
        frame = frame.drop_duplicates()
        candidates = len(frame)
        # No arbitrary winner for conflicting versions at an identical clock.
        frame = frame.sort_values('_collected').drop_duplicates('article_id', keep='last')
        superseded = candidates - len(frame)
        frame = frame.drop(columns='_collected').reset_index(drop=True)
    timed = frame.assign(date=pd.to_datetime(frame['published_at'], utc=True, errors='coerce', format='mixed').dt.date)
    if timed.empty:
        gold = pd.DataFrame(columns=GOLD_COLUMNS)
    else:
        timed['text_characters'] = timed['body'].str.len()
        gold = timed[timed['date'].notna()].groupby(['date', 'source_name'], as_index=False).agg(
            article_count=('article_id', 'count'), total_text_characters=('text_characters', 'sum'))
        gold['source_group'] = 'web_content'
        gold['run_id'] = run_id
        gold = gold[GOLD_COLUMNS]
    quality = {'records_received': len(records), 'records_valid': len(frame),
               'records_duplicate': duplicates, 'records_superseded': superseded, 'records_quarantined': len(quarantine),
               'records_missing_publication_time': int(timed['date'].isna().sum())}
    return frame, gold, quarantine, quality


def _article_fingerprint(row):
    """Content/publication changes version; re-collection alone does not."""
    fields = ('title', 'body', 'published_at', 'published_at_raw', 'publication_time_precision')
    payload = {key: _database_value(row.get(key), sqlite=True) for key in fields}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _merge_article_serving(wh, silver, run_id):
    """Join the caller's SQL transaction for current state, versions and dates."""
    cursor = wh._conn.cursor()
    placeholder = '%s' if wh._engine_type == 'postgres' else '?'
    affected_dates = set()
    current_rows = []
    for row in silver.to_dict('records'):
        cursor.execute(f'SELECT * FROM articles WHERE article_id={placeholder}', (row['article_id'],))
        stored = cursor.fetchone()
        previous = dict(zip((column[0] for column in cursor.description), stored)) if stored else None
        fingerprint = _article_fingerprint(row)
        cursor.execute(f'SELECT version_number,content_fingerprint,collected_at FROM article_versions WHERE article_id={placeholder}',(row['article_id'],))
        historical=cursor.fetchall()
        matching_version=None
        new_clock=pd.to_datetime(row['collected_at'],utc=True)
        for number,stored_fingerprint,clock in historical:
            if pd.to_datetime(clock,utc=True)==new_clock:
                if stored_fingerprint!=fingerprint:
                    raise ValueError('Conflicting article observation at the same collection clock')
                matching_version=number
        older=False
        if previous:
            old_clock=pd.to_datetime(previous['collected_at'],utc=True)
            older=new_clock<old_clock
            if new_clock==old_clock and previous['content_fingerprint'] not in (None,fingerprint):
                raise ValueError('Conflicting article observation at the same collection clock')
        if matching_version is not None:
            version=matching_version
        elif previous is None or previous['content_fingerprint'] != fingerprint:
            # Legacy rows deliberately have no inferred version. The first new
            # observation starts recorded history without inventing a past one.
            cursor.execute(f'SELECT MAX(version_number) FROM article_versions WHERE article_id={placeholder}',(row['article_id'],))
            version=int(cursor.fetchone()[0] or 0)+1
            snapshot = {**row, 'version_number': version, 'content_fingerprint': fingerprint}
            columns = list(snapshot)
            cursor.execute(
                f'INSERT INTO article_versions ({", ".join(columns)}) '
                f'VALUES ({", ".join([placeholder] * len(columns))})',
                tuple(_database_value(snapshot[key], sqlite=wh._engine_type == 'sqlite') for key in columns))
        else:
            version = previous['current_version']
        if older:
            # Keep this genuinely observed older version, but a delayed replay
            # cannot roll back the latest observed serving record or its stats.
            continue
        current_rows.append({**row, 'content_fingerprint': fingerprint, 'current_version': version,
                             'first_seen_run_id': previous['first_seen_run_id'] if previous else run_id,
                             'last_seen_run_id': run_id})
        for observation in (previous, row):
            if observation and observation.get('published_at') is not None:
                timestamp = pd.to_datetime(observation['published_at'], utc=True, errors='coerce')
                if not pd.isna(timestamp):
                    affected_dates.add(timestamp.date().isoformat())
    if current_rows:wh.upsert_dataframe('articles', pd.DataFrame(current_rows), ['article_id'])
    # Recompute affected dates from all currently served articles, including
    # those absent from this batch. Remove dates made empty by a correction.
    for date in sorted(affected_dates):
        cursor.execute(f'DELETE FROM article_daily_stats WHERE date={placeholder}', (date,))
        cursor.execute(
            'INSERT INTO article_daily_stats '
            '(date, source_name, article_count, total_text_characters, source_group, run_id) '
            f'SELECT DATE(published_at), source_name, COUNT(*), SUM(LENGTH(body)), source_group, {placeholder} '
            f'FROM articles WHERE DATE(published_at)={placeholder} '
            'GROUP BY DATE(published_at), source_name, source_group', (run_id, date))


@canonical_writer
def publish_articles(records, mode=None, run_id=None, bronze_paths=None):
    run_id = run_id or str(uuid.uuid4())
    prefix = hashlib.sha256(run_id.encode()).hexdigest()
    from storage.write_lock import write_lock
    from spark.spark_session import SparkSessionFactory
    # The lease allows the same owner; serialize same-ID attempts before
    # checking SQL or touching evidence, across processes/hosts.
    with write_lock(SparkSessionFactory.resolve_storage_path(prefix, layer="article_runs")):
        return _publish_articles_locked(records, mode, run_id, prefix, bronze_paths)


def _publish_articles_locked(records, mode, run_id, prefix, bronze_paths):
    wh = WarehouseManager(mode=mode)
    try:
        wh.initialize_schema()
        existing = wh.query("SELECT run_id FROM article_pipeline_runs")
        if run_id in set(existing['run_id']):
            raise ValueError("Article run_id already committed; use a new run_id")
        silver, gold, quarantine, quality = prepare_articles(records, run_id)
        denominator = quality['records_valid'] + quality['records_quarantined']
        score = 100 * quality['records_valid'] / denominator if denominator else 0.0
        threshold = settings.quality.min_dq_score
        if not math.isfinite(threshold) or not 0 <= threshold <= 100:
            raise ValueError("Invalid article quality threshold")
        quality.update(dq_score=score, required_score=threshold)
        if silver.empty or score < threshold:
            attempt = f"article_attempts/{prefix}/{uuid.uuid4().hex}"
            storage.write_json(f"{attempt}/quarantine.json", quarantine)
            storage.write_json(f"{attempt}/quality.json", quality)
            raise ValueError("Article quality gate failed; serving snapshot left unchanged")
        silver_path = f"silver/article_snapshots/{prefix}/articles.parquet"
        gold_path = f"gold/article_snapshots/{prefix}/article_daily_stats.parquet"
        quarantine_path = f"quarantine/articles/{prefix}/records.json"
        storage.write_parquet(silver_path, silver)
        storage.write_parquet(gold_path, gold)
        storage.write_json(quarantine_path, quarantine)
        storage.write_json(f"article_runs/{prefix}/quality.json", quality)
        backend = storage.backend
        lake = {'backend': type(backend).__name__}
        if hasattr(backend, 'bucket'):
            lake.update(bucket=backend.bucket, endpoint=backend.endpoint, secure=backend.secure)
        else:
            lake['root'] = str(backend.base_dir.resolve())
        manifest = {'lake': lake, 'run_id': run_id, 'silver_path': silver_path, 'gold_path': gold_path,
                    'quarantine_path': quarantine_path, 'bronze_paths': bronze_paths or [], 'quality': quality,
                    'engine': 'pandas', 'status': 'complete'}
        with wh.transaction():
            _merge_article_serving(wh, silver, run_id)
            wh.upsert_dataframe('article_pipeline_runs', pd.DataFrame([{
                'run_id': run_id, 'manifest_json': json.dumps(manifest, ensure_ascii=False)}]), ['run_id'])
            import os
            from serving.store import ServingStore
            publication=ServingStore(wh._conn,os.environ.get('SERVING_DATASET_ID',os.environ.get('SOURCE_JOB_NAMESPACE','legacy-web')),'web_content')
            publication.initialize()
            from serving.lineage import article_receipt, stage_lineage
            serving_silver_path = f"silver/article_snapshots/{prefix}/serving_articles.parquet"
            storage.write_parquet(serving_silver_path, wh.query("SELECT * FROM articles WHERE source_group='web_content'"))
            serving_gold_path = f"gold/article_snapshots/{prefix}/serving_article_daily_stats.parquet"
            storage.write_parquet(serving_gold_path, wh.query("SELECT * FROM article_daily_stats WHERE source_group='web_content'"))
            manifest['serving_silver_path'] = serving_silver_path
            manifest['serving_gold_path'] = serving_gold_path
            wh.upsert_dataframe('article_pipeline_runs', pd.DataFrame([{
                'run_id': run_id, 'manifest_json': json.dumps(manifest, ensure_ascii=False)}]), ['run_id'])
            stage_lineage(publication, run_id, article_receipt(run_id, bronze_paths or [], serving_silver_path, serving_gold_path,
                                                             store=publication))
            publication.stage_publication(run_id,run_id,'web')
        return manifest
    finally:
        wh.close()
