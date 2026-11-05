import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from storage.storage_manager import storage


def _load_table(table: str, silver_name: str = None, sample_name: str = None, db=None) -> pd.DataFrame:
    from dashboard.db_connector import DashboardDB

    owns_connection = db is None
    db = db if db is not None else open_dashboard_db()
    try:
        frame = db.read_table(table)
        integration = db.is_integration
    finally:
        if owns_connection:
            db.close()
    frame.attrs["data_source"] = "PostgreSQL warehouse" if integration else "SQLite warehouse"
    frame.attrs["data_scope"] = db.data_scope
    if integration or db.data_scope != "all" or not frame.empty:
        return frame
    frame = storage.read_silver_parquet(silver_name) if silver_name else storage.read_gold_parquet(table)
    frame.attrs["data_source"] = "Silver lake" if silver_name else "Gold lake"
    if frame.empty and sample_name:
        sample = Path(__file__).resolve().parent.parent / "data" / "fixtures" / "canonical" / f"{sample_name}.parquet"
        if sample.exists():
            frame = pd.read_parquet(sample)
            frame.attrs["data_source"] = "Sample fixture (synthetic)"
    return frame


def load_catalog_data(db=None) -> pd.DataFrame:
    return _load_table("products", silver_name="products", sample_name="products", db=db)


def load_reviews_data(db=None) -> pd.DataFrame:
    return _load_table("reviews", silver_name="reviews", sample_name="reviews", db=db)


def load_prices_data() -> pd.DataFrame:
    return _load_table("price_daily_stats", silver_name="prices", sample_name="prices")


def load_gold_mart(mart_name: str) -> pd.DataFrame:
    if not mart_name.replace("_", "").isalnum():
        raise ValueError("Invalid mart name")
    return _load_table(mart_name)


def render_data_sources(*frames: pd.DataFrame) -> None:
    import streamlit as st

    sources = sorted({frame.attrs.get("data_source", "Unknown") for frame in frames})
    st.caption("Nguồn đang đọc: " + ", ".join(sources))


def summarize_analytics_frame(frame: pd.DataFrame) -> dict:
    """Return provenance/coverage metadata for dashboard captions and warnings."""
    if frame is None or frame.empty:
        return {
            "rows": 0,
            "timed_rows": 0,
            "untimed_rows": 0,
            "unknown_time_rows": 0,
            "data_as_of": None,
            "catalog_price_fallback_rows": 0,
            "rating_sample_size": 0,
        }
    timed = untimed = None
    unknown_time = len(frame)
    if "time_available" in frame:
        normalized = (
            frame["time_available"]
            .map(lambda value: value.strip().lower() if isinstance(value, str) else value)
            .map({True: True, False: False, "true": True, "false": False, "1": True, "0": False})
        )
        unknown_time = int(normalized.isna().sum())
        if normalized.notna().any():
            timed = int(normalized.eq(True).sum())
            untimed = int(normalized.eq(False).sum())
    data_as_of = None
    if "data_as_of" in frame:
        values = frame["data_as_of"].dropna().astype(str)
        data_as_of = values.max() if not values.empty else None
    fallback_rows = int((frame["price_source"] == "catalog_fallback").sum()) if "price_source" in frame else 0
    sample_size = None
    if "rating_sample_size" in frame:
        samples = pd.to_numeric(frame["rating_sample_size"], errors="coerce").replace(
            [float("inf"), -float("inf")], float("nan")
        )
        samples = samples[samples.ge(0)]
        if not samples.empty:
            sample_size = int(samples.sum())
    return {
        "rows": int(len(frame)),
        "timed_rows": timed,
        "untimed_rows": untimed,
        "unknown_time_rows": unknown_time,
        "data_as_of": data_as_of,
        "catalog_price_fallback_rows": fallback_rows,
        "rating_sample_size": sample_size,
    }


def filter_source_scope(frame, scope):
    """Filter a bounded operational sample using the same labels as SQL views."""
    if scope == "all":
        return frame
    groups = (
        frame.get("source_group", pd.Series(None, index=frame.index, dtype=object)).fillna("").astype(str).str.strip()
    )
    observed = groups.isin(["marketplace", "web_content"])
    simulated = groups.eq("simulation")
    methods = (
        frame.get("acquisition_method", pd.Series(None, index=frame.index, dtype=object))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    observed &= methods.isin(["live", "snapshot_replay"])
    simulated &= methods.eq("generated")
    masks = {"real": observed, "simulation": simulated, "unverified": ~(observed | simulated)}
    if scope not in masks:
        raise ValueError("Unknown source scope")
    return frame.loc[masks[scope]].copy()


def open_dashboard_db():
    import streamlit as st
    from dashboard.db_connector import DashboardDB

    scope = st.session_state.get("dashboard_data_scope", os.environ.get("DASHBOARD_DATA_SCOPE", "real"))
    return DashboardDB(data_scope=scope)


def render_sidebar_source():
    from dashboard.ui import sidebar

    sidebar()


def get_datalake_metrics() -> dict:
    return storage.get_storage_stats()


def create_kpi_card(title: str, value: str, delta: str = None) -> str:
    from dashboard.ui import metric_html

    return metric_html(title, value, delta)


def render_analytics_quality(frame: pd.DataFrame) -> None:
    """Coverage metadata stays available without overwhelming the main view."""
    import streamlit as st

    summary = summarize_analytics_frame(frame)
    with st.expander("Thông tin chất lượng & nguồn gốc"):
        st.caption("Dữ liệu đến: " + (summary["data_as_of"] or "Chưa xác định"))
        if summary["untimed_rows"] is None:
            st.info("Chưa biết độ bao phủ thời gian quan sát do thiếu metadata.")
        elif summary["untimed_rows"]:
            st.warning(f"{summary['untimed_rows']} bản ghi không có thời gian quan sát hợp lệ.")
        if summary["unknown_time_rows"] and summary["untimed_rows"] is not None:
            st.info(f"{summary['unknown_time_rows']} bản ghi chưa biết độ bao phủ thời gian quan sát.")
        if "price_source" not in frame:
            st.caption("Nguồn gốc giá: Chưa xác định")
        elif summary["catalog_price_fallback_rows"]:
            st.warning(
                f"{summary['catalog_price_fallback_rows']} bản ghi dùng giá danh mục thay cho quan sát giá theo thời gian."
            )
        if summary["rating_sample_size"] is not None:
            st.caption(f"Số mẫu điểm hợp lệ: {summary['rating_sample_size']:,}")
            if not summary["rating_sample_size"]:
                st.warning("Chưa có mẫu điểm hợp lệ; chỉ số điểm đánh giá chưa khả dụng.")
        else:
            st.caption("Số mẫu điểm hợp lệ: Chưa xác định")
