"""Local pandas Gold implementation with the same statistical contract as Spark."""
import uuid
from typing import Dict, Optional

import numpy as np
import pandas as pd

from config.settings import settings
from analytics.rounding import round_series
from storage.storage_manager import storage, StorageManager


class GoldAggregator:
    @staticmethod
    def _dated(frame: pd.DataFrame, candidates: tuple[str, ...], allow_ingestion_time: bool = False) -> pd.DataFrame:
        result = frame.copy()
        dates = pd.Series(pd.NaT, index=result.index, dtype='datetime64[ns, UTC]')
        for name in dict.fromkeys(('date',) + candidates):
            if name in result:
                parsed = pd.to_datetime(result[name], errors='coerce', utc=True, format='mixed')
                dates = dates.fillna(parsed)
        result['time_available'] = dates.notna()
        if allow_ingestion_time and 'ingested_at' in result:
            dates = dates.fillna(pd.to_datetime(result['ingested_at'], errors='coerce', utc=True, format='mixed'))
        result['date'] = dates.dt.date
        return result[result['date'].notna()].copy()

    @staticmethod
    def _prices(values: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(values, errors='coerce')
        return numeric.where(np.isfinite(numeric) & numeric.gt(0))

    @staticmethod
    def build_gold_marts(
        df_products: Optional[pd.DataFrame] = None,
        df_reviews: Optional[pd.DataFrame] = None,
        df_prices: Optional[pd.DataFrame] = None,
        storage_mgr: Optional[StorageManager] = None,
        positive_threshold: Optional[float] = None,
        negative_threshold: Optional[float] = None,
        save_to_storage: bool = True,
        run_id: Optional[str] = None,
        source_run_id: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        mgr = storage_mgr or storage
        products = (df_products if df_products is not None else mgr.read_silver_parquet('products')).copy()
        reviews = df_reviews if df_reviews is not None else mgr.read_silver_parquet('reviews')
        prices = df_prices if df_prices is not None else pd.DataFrame()
        from ingestion.provenance import classify_source_group
        source_group = classify_source_group(products, reviews, prices)
        from analytics.currency import require_single_currency
        currency = require_single_currency(products, prices)
        positive = positive_threshold if positive_threshold is not None else settings.analytics.sentiment_positive_threshold
        negative = negative_threshold if negative_threshold is not None else settings.analytics.sentiment_negative_threshold
        marts = {}
        if not products.empty:
            if ('product_id' not in products or products['product_id'].isna().any()
                    or products['product_id'].astype(str).str.strip().eq('').any()
                    or products['product_id'].duplicated().any()):
                raise ValueError('Catalog product_id must be present and unique')
            products['price'] = GoldAggregator._prices(products['price']) if 'price' in products else np.nan

        if not prices.empty:
            observed = GoldAggregator._dated(prices, ('timestamp', 'created_at'))
            observed['price'] = GoldAggregator._prices(observed['price'])
            observed = observed[observed['price'].notna() & observed['product_id'].notna()]
            if not observed.empty:
                daily = observed.groupby(['date', 'product_id']).agg(
                    min_price=('price', 'min'), max_price=('price', 'max'), avg_price=('price', 'mean'),
                    price_std=('price', 'std'), data_points=('price', 'count'),
                ).reset_index()
                daily[['min_price', 'max_price', 'avg_price', 'price_std']] = daily[
                    ['min_price', 'max_price', 'avg_price', 'price_std']
                ].apply(round_series)
                marts['price_daily_stats'] = daily

        if not reviews.empty:
            valid = GoldAggregator._dated(reviews, ('review_date', 'timestamp', 'created_at'),
                                          allow_ingestion_time=not settings.analytics.strict_analytics_time)
            valid['rating'] = pd.to_numeric(valid['rating'], errors='coerce')
            valid = valid[valid['rating'].between(1, 5) & valid['product_id'].notna() & valid['review_id'].notna()
                          & valid['review_id'].astype(str).str.strip().ne('')]
            if not valid.empty:
                daily = valid.groupby(['date', 'product_id']).agg(
                    time_available=('time_available', 'min'),
                    review_count=('review_id', 'count'), rating_sample_size=('rating', 'count'),
                    avg_rating=('rating', 'mean'), rating_std=('rating', 'std'),
                    positive_ratio=('rating', lambda x: (x >= positive).mean()),
                    negative_ratio=('rating', lambda x: (x <= negative).mean()),
                ).reset_index()
                daily['avg_rating'] = round_series(daily['avg_rating'])
                daily['weighted_avg_rating'] = daily['avg_rating']
                daily['rating_std'] = round_series(daily['rating_std'])
                for ratio in ['positive_ratio', 'negative_ratio']:
                    daily[ratio] = round_series(daily[ratio],4)
                daily['high_rating_ratio'] = daily['positive_ratio']
                daily['low_rating_ratio'] = daily['negative_ratio']
                metric_names = ['min_price', 'max_price', 'avg_price']
                if 'price_daily_stats' in marts:
                    daily = daily.merge(marts['price_daily_stats'][['date', 'product_id'] + metric_names],
                                        on=['date', 'product_id'], how='left', validate='one_to_one')
                else:
                    daily[metric_names] = np.nan
                daily['price_source'] = np.where(daily['avg_price'].notna(), 'observed', 'unavailable')
                if not products.empty:
                    daily = daily.merge(products[['product_id', 'price']], on='product_id',
                                        how='left', validate='many_to_one')
                    fallback = daily['avg_price'].isna() & daily['price'].notna()
                    daily.loc[fallback, 'price_source'] = 'catalog_fallback'
                    for name in metric_names:
                        daily[name] = round_series(daily[name].fillna(daily['price']))
                    daily = daily.drop(columns='price')
                marts['product_daily_stats'] = daily

                for dimension, name in [('brand', 'brand_daily_stats'), ('category', 'category_daily_stats')]:
                    if products.empty or dimension not in products:
                        continue
                    joined = valid.merge(products[['product_id', dimension, 'price']], on='product_id',
                                         how='inner', validate='many_to_one')
                    groups = ['date', dimension]
                    stats = joined.groupby(groups).agg(
                        time_available=('time_available', 'min'),
                        product_count=('product_id', 'nunique'), review_count=('review_id', 'count'),
                        rating_sample_size=('rating', 'count'), avg_rating=('rating', 'mean'),
                    ).reset_index()
                    stats['avg_rating'] = round_series(stats['avg_rating'])
                    stats['weighted_avg_rating'] = stats['avg_rating']
                    # One catalog price per reviewed product, irrespective of its review volume.
                    catalog_mean = joined.drop_duplicates(groups + ['product_id']).groupby(groups)['price'].mean()
                    stats = stats.merge(round_series(catalog_mean.rename('avg_price')), on=groups, validate='one_to_one')
                    marts[name] = stats

        effective_run = run_id or f'gold-{uuid.uuid4().hex}'
        for name, frame in marts.items():
            frame['currency'] = currency
            frame['source_group'] = source_group
            frame['run_id'] = effective_run
            frame['source_run_id'] = source_run_id
            if 'time_available' not in frame:
                frame['time_available'] = frame['date'].notna()
            observed_dates = frame.loc[frame['time_available'], 'date']
            frame['data_as_of'] = observed_dates.max() if not observed_dates.empty else None
        if save_to_storage:
            for name, dimension in [('product_daily_stats','product_id'),('brand_daily_stats','brand'),
                                    ('category_daily_stats','category'),('price_daily_stats','product_id')]:
                if name not in marts:
                    columns = {'date':'datetime64[ns]',dimension:'str','source_group':'str','run_id':'str','source_run_id':'str',
                               'data_as_of':'datetime64[ns]','time_available':'bool'}
                    if name == 'price_daily_stats':
                        columns.update({key:'float64' for key in ['min_price','max_price','avg_price','price_std']})
                        columns['data_points'] = 'int64'
                    else:
                        columns.update({key:'int64' for key in ['review_count','rating_sample_size']})
                        columns.update({key:'float64' for key in ['avg_rating','weighted_avg_rating','rating_std',
                                                                 'positive_ratio','negative_ratio','avg_price']})
                        if name == 'product_daily_stats':
                            columns.update(min_price='float64',max_price='float64',price_source='str')
                        else:
                            columns['product_count'] = 'int64'
                    marts[name] = pd.DataFrame({key:pd.Series(dtype=kind) for key,kind in columns.items()})
            from storage.write_lock import write_lock
            from spark.spark_session import SparkSessionFactory
            with write_lock(SparkSessionFactory.resolve_storage_path('', layer='gold')):
                snapshot = {'run_id':effective_run,'source_run_id':source_run_id,'status':'publishing','source_group':source_group,
                            'rows':{name:len(frame) for name,frame in marts.items()}}
                mgr.write_json('gold/_snapshot.json', snapshot)
                for name, frame in marts.items():
                    mgr.write_gold_parquet(name, frame, allow_empty=True, replace_snapshot=True)
                snapshot['status'] = 'complete'
                mgr.write_json('gold/_snapshot.json', snapshot)
        return marts
