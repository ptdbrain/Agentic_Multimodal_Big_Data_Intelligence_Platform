"""The current marts have one monetary unit per serving namespace; no FX conversion."""
from spark.etl.normalizer import EntityNormalizer


def require_single_currency(*frames):
    """Return the observed unit, or None for wholly unverified legacy data.

    Mixing known/unknown units or multiple currencies requires separate runs.
    Spark only collects the small normalized unit domain, never price rows.
    """
    units = set()
    for frame in frames:
        if (frame is None or not hasattr(frame, 'columns')
                or not {'price','avg_price','min_price','max_price'}.intersection(frame.columns)):
            continue
        money_columns = {'price','avg_price','min_price','max_price'}.intersection(frame.columns)
        if hasattr(frame, 'sparkSession'):
            from pyspark.sql import functions as F
            observed = F.lit(False)
            for name in money_columns:
                observed = observed | F.col(name).isNotNull()
            frame = frame.filter(observed)
            value = F.lower(F.trim(F.col('currency').cast('string'))) if 'currency' in frame.columns else F.lit(None)
            aliases = F.create_map(*[F.lit(item) for pair in EntityNormalizer.CURRENCY_ALIASES.items() for item in pair])
            unit = F.coalesce(F.element_at(aliases, value), F.when(value.rlike('^[a-z]{3}$'), F.upper(value)))
            values = [row['unit'] for row in frame.select(unit.alias('unit')).distinct().limit(2).collect()]
        elif not frame.empty:
            observed = frame.loc[:,sorted(money_columns)].notna().any(axis=1)
            frame = frame.loc[observed]
            if frame.empty:
                continue
            values = (frame['currency'].tolist() if 'currency' in frame.columns else [None])
        else:
            continue
        units.update(EntityNormalizer.normalize_currency(value) for value in values)
        if len(units) > 1:
            raise ValueError('Incompatible currency units: isolate currencies before analysis; FX conversion is not configured')
    return next(iter(units), None)
