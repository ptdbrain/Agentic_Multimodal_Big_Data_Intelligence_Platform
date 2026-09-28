import pytest
import sys
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.run_e2e_pipeline import run_end_to_end
from storage.storage_manager import storage

def test_full_e2e_pipeline():
    success = run_end_to_end()
    assert success is True
    
    # Verify Silver Parquet exists
    df_prods = storage.read_silver_parquet("products")
    df_revs = storage.read_silver_parquet("reviews")
    assert not df_prods.empty
    assert not df_revs.empty
    assert "product_id" in df_prods.columns
    assert "review_id" in df_revs.columns

    # Verify Gold Marts exist
    df_gold = storage.read_gold_parquet("product_daily_stats")
    assert not df_gold.empty
