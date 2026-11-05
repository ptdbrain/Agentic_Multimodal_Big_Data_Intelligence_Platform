"""Pure view helpers: user filters never repair missing business values."""

import unicodedata
import pandas as pd


def search_text(value):
    text = str(value).casefold().replace("đ", "d")
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def search_frame(frame, query, columns):
    if not query.strip() or frame.empty:
        return frame.copy()
    needle = search_text(query.strip())
    mask = pd.Series(False, index=frame.index)
    for name in columns:
        if name in frame:
            mask |= frame[name].fillna("").map(search_text).str.contains(needle, regex=False)
    return frame.loc[mask].copy()


def filter_catalog(frame, query="", brands=None, categories=None, sources=None, minimum_rating=0):
    result = search_frame(frame, query, ["product_id", "product_name", "brand", "category", "source_name"])
    for name, values in [("brand", brands), ("category", categories), ("source_name", sources)]:
        if values and name in result:
            result = result.loc[result[name].isin(values)]
    if minimum_rating and "avg_rating" in result:
        result = result.loc[pd.to_numeric(result["avg_rating"], errors="coerce").ge(minimum_rating)]
    return result.copy()


def page_slice(frame, page=1, size=20):
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ValueError("Page size must be positive")
    pages = max(1, (len(frame) + size - 1) // size)
    page = max(1, min(int(page), pages))
    return frame.iloc[(page - 1) * size : page * size].copy(), page, pages


def options(frame, column):
    return sorted(frame[column].dropna().astype(str).unique().tolist()) if column in frame else []


def clear_keys(*keys):
    import streamlit as st

    for key in keys:
        st.session_state.pop(key, None)


def csv_download_bytes(frame):
    """Excel-friendly UTF8; source text is inert when opened in a spreadsheet."""
    exported = frame.copy()

    def inert(value):
        if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@", "\\t", "\\r")):
            return "'" + value
        return value

    for column in exported.columns:
        if exported[column].dtype == object or pd.api.types.is_string_dtype(exported[column]):
            exported[column] = exported[column].map(inert)
    return exported.to_csv(index=False).encode("utf-8-sig")


def purchase_status(value):
    """Keep absent verification separate from an observed false value."""
    if value is None or pd.isna(value):
        return "Chưa có thông tin"
    normalized = str(value).strip().casefold()
    if normalized in ("true", "1"):
        return "Đã xác minh"
    if normalized in ("false", "0"):
        return "Chưa xác minh"
    return "Chưa có thông tin"


def price_comparison(frame):
    """Only comparable observed values; each currency gets its own chart facet."""
    required = {"price", "avg_rating", "review_count", "currency"}
    if not required.issubset(frame.columns):
        return frame.iloc[:0].copy()
    result = frame.copy()
    for column in ("price", "avg_rating", "review_count"):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    known_currency = result["currency"].notna() & result["currency"].astype(str).str.strip().ne("")
    valid = known_currency & result["price"].ge(0) & result["price"].lt(float("inf"))
    valid &= result["avg_rating"].between(1, 5) & result["review_count"].gt(0)
    valid &= result["review_count"].lt(float("inf"))
    return result.loc[valid].copy()
