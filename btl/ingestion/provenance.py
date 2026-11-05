"""Source classes are analytical isolation boundaries, not liveness attestation."""
import math
import pandas as pd


def _nullable(value):
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NA:
        return None
    if not isinstance(value, str):
        raise ValueError('Source metadata must be strings or unknown')
    return value.strip() or None


def _pairs(data):
    if data is None:
        return []
    fields = ['source_group', 'acquisition_method']
    if hasattr(data, 'schema') and hasattr(data, 'select'):
        from pyspark.sql import functions as F
        rows = data.select(*[(F.col(name) if name in data.columns else F.lit(None)).alias(name)
                             for name in fields]).distinct().limit(16).collect()
        if len(rows) == 16:
            raise ValueError('Unexpected source metadata domain')
        return [(row['source_group'], row['acquisition_method']) for row in rows]
    if isinstance(data, pd.DataFrame):
        if data.empty:
            return []
        return list(data.reindex(columns=fields).drop_duplicates().itertuples(index=False, name=None))
    return [(row.get('source_group'), row.get('acquisition_method')) for row in data]


def classify_source_group(*datasets):
    """Return one source class; reject heterogeneous or contradictory inputs.

    A legacy source string such as web_crawler is not evidence of an observed
    marketplace source. Missing classifications remain unverified.
    """
    groups = set()
    for data in datasets:
        for raw_group, raw_method in _pairs(data):
            group = _nullable(raw_group) or 'unverified'
            method = _nullable(raw_method)
            if group not in ('marketplace', 'simulation', 'unverified'):
                raise ValueError(f'Unsupported commerce source group: {group}')
            if group == 'marketplace' and method not in ('live', 'snapshot_replay'):
                raise ValueError('Source acquisition does not match marketplace group')
            if group == 'simulation' and method != 'generated':
                raise ValueError('Source acquisition does not match simulation group')
            groups.add(group)
    if len(groups) > 1:
        raise ValueError('mixed source groups require separate analytical generations')
    return next(iter(groups), 'unverified')


def inherit_source_metadata(record, source):
    """Carry a listing's provenance into its observed-price snapshot."""
    result = dict(record)
    for name in ('source_group', 'source_name', 'source_record_id', 'source_url', 'acquisition_method',
                 'collected_at', 'evidence_sha256', 'evidence_ref', 'parser_version', 'schema_version',
                 'collection_run_id', 'generator_seed', 'generator_config'):
        if source.get(name) is not None:
            result[name] = source[name]
    return result
