"""Acceptance Test Script: Spark Gold -> PostgreSQL -> Dashboard.
Verifies:
  1. SparkGoldBuilder distributed aggregation (4 Gold Marts: product, brand, category, price)
  2. Transactional snapshot export to PostgreSQL DW
  3. SQL row-count reconciliation and sampled product metrics
  4. DashboardDB reads PostgreSQL directly with active source labeling
  5. Strict integration mode: raises ConnectionError when PostgreSQL is down, zero Parquet fallback.
"""
import os
import sys
from pathlib import Path
import psycopg2
import pandas as pd
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage
from spark.spark_session import SparkSessionFactory
from spark.analytics.gold_builder import SparkGoldBuilder
from database.warehouse import WarehouseManager
from database.export_gold import export_gold_to_db
from dashboard.db_connector import DashboardDB
from dashboard.utils import load_catalog_data, load_reviews_data, load_prices_data, load_gold_mart


def test_gold_generation_and_postgres_sync():
    print("\n" + "=" * 65)
    print("STEP 1: SPARK GOLD MART GENERATION (DISTRIBUTED SPARK)")
    print("=" * 65)

    # Ensure MinIO / integration mode
    settings.set_runtime_mode("integration")
    storage.set_mode("integration")

    # Use the actual producer identity; never create fresh labels or seed sample
    # data to make a service acceptance command appear successful.
    from spark.quality.spark_dq import SparkDataQualityEvaluator
    datasets = ['products', 'reviews', 'prices']
    snapshots = {}
    for name in datasets:
        try:
            snapshots[name] = storage.read_json(f'silver/_snapshot_{name}.json')
        except FileNotFoundError as exc:
            raise RuntimeError(
                f'Missing verified Silver snapshot for {name}; run the canonical pipeline first.'
            ) from exc
    source_runs = {snapshot.get('run_id') for snapshot in snapshots.values()}
    if len(source_runs) != 1 or not next(iter(source_runs)):
        raise RuntimeError('Silver snapshots must belong to one verified producer run')
    source_run_id = next(iter(source_runs))
    reports = storage.read_gold_parquet('data_quality_metrics')
    if reports.empty or 'run_id' not in reports:
        raise RuntimeError('Missing DQ reports for the verified Silver snapshots; run the pipeline first.')
    dq_metrics = reports[reports['run_id'].eq(source_run_id)].to_dict(orient='records')
    SparkDataQualityEvaluator.check_dq_gate(
        dq_metrics, required_datasets=datasets, run_id=source_run_id, raise_on_failure=True,
    )

    # Inspect existing verified Silver; missing inputs are errors.
    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    df_prices = storage.read_silver_parquet("prices")

    print(f"Input Silver Data: {len(df_prods)} products, {len(df_revs)} reviews, {len(df_prices)} prices")
    assert not df_prods.empty, "Silver products must not be empty"
    assert not df_revs.empty, "Silver reviews must not be empty"

    # Build Gold Marts via SparkGoldBuilder
    gold_builder = SparkGoldBuilder()
    assert gold_builder.spark is not None, "SparkSession must be initialized"
    
    marts = gold_builder.build_all(
        save_gold=True,
        dq_metrics=dq_metrics,
        run_id=source_run_id,
        source_run_id=source_run_id,
        required_datasets=datasets,
        enforce_dq_gate=True,
    )

    print(f"Generated Gold Marts in memory: {list(marts.keys())}")
    for mart_name in ["product_daily_stats", "brand_daily_stats", "category_daily_stats", "price_daily_stats"]:
        assert mart_name in marts, f"Mart '{mart_name}' must be built by SparkGoldBuilder"

    # Verify persisted Parquet in Gold layer
    gold_p = storage.read_gold_parquet("product_daily_stats")
    gold_b = storage.read_gold_parquet("brand_daily_stats")
    gold_c = storage.read_gold_parquet("category_daily_stats")
    gold_pr = storage.read_gold_parquet("price_daily_stats")

    print(f"Persisted Gold Marts: "
          f"product={len(gold_p)}, brand={len(gold_b)}, category={len(gold_c)}, price={len(gold_pr)}")
    assert not gold_p.empty, "product_daily_stats Gold Parquet must not be empty"
    assert not gold_b.empty, "brand_daily_stats Gold Parquet must not be empty"
    assert not gold_c.empty, "category_daily_stats Gold Parquet must not be empty"
    assert not gold_pr.empty, "price_daily_stats Gold Parquet must not be empty"

    # Verify column naming matches schema.sql
    assert "positive_ratio" in gold_p.columns, "positive_ratio must exist in product_daily_stats"
    assert "negative_ratio" in gold_p.columns, "negative_ratio must exist in product_daily_stats"
    assert "avg_rating" in gold_b.columns, "avg_rating must exist in brand_daily_stats"
    assert "avg_price" in gold_c.columns, "avg_price must exist in category_daily_stats"
    assert "price_std" in gold_pr.columns, "price_std must exist in price_daily_stats"

    print("\n" + "=" * 65)
    print("STEP 2: EXPORT TO POSTGRESQL (TRANSACTIONAL UPSERT)")
    print("=" * 65)

    synced = export_gold_to_db(mode="integration", use_postgres=True)
    print(f"Export summary: {synced}")
    assert "product_daily_stats" in synced
    assert "brand_daily_stats" in synced
    assert "category_daily_stats" in synced
    assert "price_daily_stats" in synced

    print("\n" + "=" * 65)
    print("STEP 3: INDEPENDENT SQL RECONCILIATION")
    print("=" * 65)

    conn = psycopg2.connect(
        host=settings.database.host,
        port=settings.database.port,
        user=settings.database.user,
        password=settings.database.password,
        database=settings.database.database
    )
    cursor = conn.cursor()

    # Reconcile product_daily_stats
    cursor.execute("SELECT COUNT(*) FROM product_daily_stats")
    pg_p_count = cursor.fetchone()[0]
    print(f"[product_daily_stats] Gold Parquet count: {len(gold_p)} | PostgreSQL count: {pg_p_count}")
    assert pg_p_count == len(gold_p), f"Count mismatch: Parquet={len(gold_p)} vs PG={pg_p_count}"

    # Sample metric check
    sample_pid = gold_p["product_id"].iloc[0]
    sample_date = str(gold_p["date"].iloc[0])
    cursor.execute(
        "SELECT review_count, avg_rating, positive_ratio, negative_ratio FROM product_daily_stats WHERE product_id = %s AND date = %s",
        (sample_pid, sample_date)
    )
    row = cursor.fetchone()
    assert row is not None, f"Row for {sample_pid} on {sample_date} not found in PostgreSQL"
    parquet_row = gold_p[(gold_p["product_id"] == sample_pid) & (gold_p["date"].astype(str) == sample_date)].iloc[0]
    
    print(f"[product_daily_stats sample check] PID={sample_pid}, Date={sample_date}:")
    print(f"  Parquet review_count={parquet_row['review_count']}, PG={row[0]}")
    print(f"  Parquet avg_rating={parquet_row['avg_rating']}, PG={float(row[1])}")
    print(f"  Parquet positive_ratio={parquet_row['positive_ratio']}, PG={float(row[2])}")
    print(f"  Parquet negative_ratio={parquet_row['negative_ratio']}, PG={float(row[3])}")
    assert int(parquet_row['review_count']) == int(row[0])
    assert abs(float(parquet_row['avg_rating']) - float(row[1])) < 0.01

    # Reconcile brand_daily_stats
    cursor.execute("SELECT COUNT(*) FROM brand_daily_stats")
    pg_b_count = cursor.fetchone()[0]
    print(f"[brand_daily_stats] Gold Parquet count: {len(gold_b)} | PostgreSQL count: {pg_b_count}")
    assert pg_b_count == len(gold_b)

    # Reconcile category_daily_stats
    cursor.execute("SELECT COUNT(*) FROM category_daily_stats")
    pg_c_count = cursor.fetchone()[0]
    print(f"[category_daily_stats] Gold Parquet count: {len(gold_c)} | PostgreSQL count: {pg_c_count}")
    assert pg_c_count == len(gold_c)

    # Reconcile price_daily_stats
    cursor.execute("SELECT COUNT(*) FROM price_daily_stats")
    pg_pr_count = cursor.fetchone()[0]
    print(f"[price_daily_stats] Gold Parquet count: {len(gold_pr)} | PostgreSQL count: {pg_pr_count}")
    assert pg_pr_count == len(gold_pr)

    conn.close()
    print(">>> All 4 mart row counts and sampled product metrics reconciled with PostgreSQL.")

    print("\n" + "=" * 65)
    print("STEP 4: DASHBOARD CONNECTOR & ZERO FALLBACK VERIFICATION")
    print("=" * 65)

    db = DashboardDB(mode="integration")
    source_label = db.get_active_source_label()
    print(f"Active Dashboard Source: {source_label}")
    assert "PostgreSQL" in source_label
    assert db.is_integration is True

    # Test dashboard queries reading from PostgreSQL
    dash_p = db.get_product_daily_stats()
    dash_b = db.get_brand_daily_stats()
    kpis = db.get_kpi_summary()
    print(f"Dashboard query product_daily_stats rows: {len(dash_p)}")
    print(f"Dashboard query brand_daily_stats rows:   {len(dash_b)}")
    print(f"Dashboard live KPI Summary: {kpis}")
    assert len(dash_p) == pg_p_count
    assert len(dash_b) == pg_b_count
    assert kpis["total_products"] > 0
    assert kpis["total_reviews"] > 0

    # Test utils load functions in integration mode
    cat_df = load_catalog_data()
    rev_df = load_reviews_data()
    gold_m = load_gold_mart("brand_daily_stats")
    assert not cat_df.empty, "load_catalog_data must return PostgreSQL products"
    assert not rev_df.empty, "load_reviews_data must return PostgreSQL reviews"
    assert not gold_m.empty, "load_gold_mart must return PostgreSQL brand_daily_stats"

    print("\n" + "=" * 65)
    print("STEP 5: FAILURE TESTING (ZERO FALLBACK IN INTEGRATION MODE)")
    print("=" * 65)

    # When PostgreSQL is unreachable in integration mode:
    # 1. DashboardDB must raise ConnectionError (NEVER fall back to SQLite or Parquet)
    # 2. export_gold_to_db must raise ConnectionError
    print("Testing unreachable PostgreSQL error handling...")
    orig_port = settings.database.port
    try:
        settings.database.port = 59999  # Closed port
        try:
            broken_db = DashboardDB(mode="integration")
            assert False, "DashboardDB must raise ConnectionError when PostgreSQL is unavailable"
        except ConnectionError as ce:
            print(f"[OK] DashboardDB correctly raised ConnectionError: {ce}")

        try:
            export_gold_to_db(mode="integration", use_postgres=True)
            assert False, "export_gold_to_db must raise ConnectionError when PostgreSQL is unavailable"
        except ConnectionError as ce:
            print(f"[OK] export_gold_to_db correctly raised ConnectionError: {ce}")

    finally:
        settings.database.port = orig_port

    # Test empty warehouse query in integration mode: MUST return empty DataFrame, strictly no Parquet fallback
    print("Testing empty warehouse query in integration mode (strictly no Parquet fallback)...")
    empty_res = db.query("SELECT * FROM products WHERE product_id = 'NON_EXISTENT_XYZ'")
    assert empty_res.empty, "Query for non-existent row must return empty DataFrame"
    assert len(empty_res) == 0, "No records must be returned when warehouse has no matches"
    db.close()
    print("[OK] Empty warehouse returns empty DataFrame without Parquet substitution.")

    print("\n" + "=" * 65)
    print(">>> ACCEPTANCE VERIFICATION COMPLETED WITH 100% SUCCESS <<<")
    print("=" * 65)


if __name__ == "__main__":
    test_gold_generation_and_postgres_sync()
