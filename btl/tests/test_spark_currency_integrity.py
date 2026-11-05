import pytest
from spark.analytics.gold_builder import SparkGoldBuilder

pytestmark = pytest.mark.spark


def test_spark_price_mart_rejects_currency_mixture(spark_session):
    rows = spark_session.createDataFrame([
        ('p',1000.0,'USD','2026-11-01T00:00:00Z'),
        ('p',25000000.0,'VND','2026-11-01T01:00:00Z'),
    ], 'product_id string, price double, currency string, timestamp string')
    with pytest.raises(ValueError, match='currency'):
        SparkGoldBuilder(spark_session).build_price_daily_stats(rows)
