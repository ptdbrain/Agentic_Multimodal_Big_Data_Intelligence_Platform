"""Inspect and replay repaired records from durable lake DLQ/quarantine objects.

Kafka-only dead letters must first be exported to a lake DLQ JSON object. This
utility never fabricates successful delivery or automatically repairs source facts.
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))

from config.settings import settings
from data.schemas.validator import SchemaValidator
from ingestion.privacy import sanitize_record_pii
from kafka.producers.stream_producer import StreamProducer
from spark.etl.normalizer import EntityNormalizer
from storage.storage_manager import storage


def _payload(item):
    # Durable DLQ -> original envelope -> business payload, or a flat Silver row.
    for _ in range(3):
        if isinstance(item,dict) and isinstance(item.get('payload'),dict):
            item = item['payload']
        else:
            break
    if not isinstance(item,dict):
        return None
    return {key:value for key,value in item.items()
            if not (pd.api.types.is_scalar(value) and pd.isna(value))}


def _schema(row):
    if 'article_id' in row:
        return 'article'
    if 'review_id' in row:
        return 'review'
    if 'price_id' in row:
        return 'price'
    if 'product_id' in row:
        return 'product'
    return None


def replay_dlq_messages(max_records: int = 100, dry_run: bool = False) -> Dict[str, Any]:
    """Count eligibility in dry-run; otherwise require real acknowledged Kafka delivery.

    Parse/storage/delivery failures propagate. Existing quarantine remains intact;
    downstream deterministic identity/dedup makes an explicit retry recoverable.
    """
    if max_records <= 0:
        raise ValueError('max_records must be positive')
    if not dry_run and settings.is_local_mode:
        raise ValueError('Live replay requires integration/production mode; use --dry-run for local inspection')
    paths = sorted({path for prefix in ('quarantine','silver/quarantine','bronze/dead-letter')
                    for path in storage.list_objects(prefix) if path.endswith(('.json','.parquet'))})
    inspected = eligible = replayed = failed = 0
    eligible_topics = Counter()
    producer = None
    try:
        for path in paths:
            if inspected >= max_records:
                break
            if path.endswith('.parquet'):
                rows = storage.read_parquet(path).to_dict('records')
            else:
                rows = storage.read_json(path)
                rows = rows if isinstance(rows,list) else [rows]
            for item in rows:
                if inspected >= max_records:
                    break
                inspected += 1
                row = _payload(item)
                schema = _schema(row) if row is not None else None
                valid = bool(schema) and SchemaValidator.validate_record(row,schema)[0]
                if valid and schema != 'article':
                    normalized = EntityNormalizer.normalize_record(row,{'review':'reviews','product':'products','price':'prices'}[schema])
                    valid = normalized['normalization_status'] == 'valid'
                elif valid:
                    from spark.batch.article_etl import prepare_articles
                    _,_,_,quality = prepare_articles([row],'dlq-inspection')
                    valid = quality['records_valid'] == 1
                if not valid:
                    failed += 1
                    continue
                topic = getattr(settings.kafka,'topic_' + {'review':'reviews','product':'products','price':'prices','article':'articles'}[schema])
                eligible += 1
                eligible_topics[topic] += 1
                if dry_run:
                    continue
                if producer is None:
                    producer = StreamProducer(enable_offline_buffer=False)
                row = sanitize_record_pii(row)
                if row.get('acquisition_method') in ('live','snapshot_replay'):
                    row['original_acquisition_method'] = row.get('original_acquisition_method',row['acquisition_method'])
                    row['acquisition_method'] = 'snapshot_replay'
                _, delivery = producer.send_message(topic=topic,
                    key=str(row.get('article_id') or row.get('product_id') or row.get('review_id')),
                    value=row,event_type=f'REPLAY_{schema.upper()}',source='dlq_replay',wait_for_delivery=True)
                if not isinstance(delivery,dict) or delivery.get('status') != 'DELIVERED':
                    raise RuntimeError('Replay lacks confirmed Kafka delivery; refusing a buffered success claim')
                replayed += 1
    finally:
        if producer is not None:
            producer.close()
    result = {'inspected':inspected,'eligible':eligible,'replayed':replayed,'failed':failed,
              'eligible_by_topic':dict(eligible_topics),'dry_run':dry_run,
              'transport':'none' if dry_run else 'real_kafka',
              'boundary':'Lake JSON/Parquet quarantine and lake-exported DLQ only; Kafka-only DLQ is not scanned'}
    print(json.dumps(result,indent=2))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max',type=int,default=100,help='Maximum records to inspect across all objects')
    parser.add_argument('--dry-run',action='store_true',help='Inspect eligibility without publishing')
    args = parser.parse_args()
    replay_dlq_messages(max_records=args.max,dry_run=args.dry_run)
