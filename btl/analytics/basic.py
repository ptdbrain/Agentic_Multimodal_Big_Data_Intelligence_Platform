"""Read-only descriptive reports from a current catalog and observed review/price history."""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence
import numpy as np
import pandas as pd
from analytics.trend.trends import TrendAnalyzer
from spark.etl.normalizer import EntityNormalizer

KEYS = ["source_group", "product_id"]


def source_groups(*frames: pd.DataFrame | None) -> list[str]:
    return sorted({group for frame in frames for group in _prepare(frame)["source_group"].unique()})


def _prepare(frame):
    frame = frame.copy() if frame is not None else pd.DataFrame()
    for column in KEYS:
        if column not in frame:
            frame[column] = pd.Series(index=frame.index, dtype=object)
    frame["source_group"] = frame["source_group"].fillna("unverified").replace("", "unverified")
    return frame


def _column(frame, name):
    return frame[name] if name in frame else pd.Series(index=frame.index, dtype=object)


def _currency(value):
    # pandas nulls stringify as three letters and must never become a currency.
    if pd.isna(value) or str(value).strip().lower() in {"nan", "nat", "none", "null", "<na>", ""}:
        return None
    return EntityNormalizer.normalize_currency(value)


def _dates(values):
    return pd.to_datetime(values, errors="coerce", utc=True, format="mixed").dt.floor("D")


def article_source_breakdown(articles: pd.DataFrame) -> pd.DataFrame:
    """Summarize exactly the library rows selected by the reader."""
    labels = _column(articles, "source_name").astype("string").str.strip().replace("", pd.NA)
    rows = pd.DataFrame({
        "source_name": labels.fillna("Chưa xác định"),
        "has_date": _dates(_column(articles, "published_at")).notna(),
    })
    groups = rows.groupby("source_name", as_index=False).agg(
        article_count=("has_date", "size"), dated_articles=("has_date", "sum"),
    )
    groups.insert(2, "article_share_pct", groups["article_count"].div(len(rows)).mul(100))
    return groups.sort_values(["article_count", "source_name"], ascending=[False, True]).reset_index(drop=True)


def _bounds(start_date, end_date):
    start = pd.to_datetime(start_date, utc=True).floor("D") if start_date is not None else None
    end = pd.to_datetime(end_date, utc=True).floor("D") if end_date is not None else None
    if (start is not None and pd.isna(start)) or (end is not None and pd.isna(end)):
        raise ValueError("Invalid date bound")
    if start is not None and end is not None and start > end:
        raise ValueError("Start date must not follow end date")
    return start, end


def _in_dates(frame, column, start, end):
    result = frame.copy()
    dates = _dates(_column(result, column))
    keep = pd.Series(True, index=result.index)
    if start is not None:
        keep &= dates.ge(start)
    if end is not None:
        keep &= dates.lt(end + timedelta(days=1))
    return result.loc[keep].copy()


def _linked(frame, catalog):
    """Count orphans before applying catalog filters; never multiply source identities."""
    linked = frame.merge(catalog[KEYS], on=KEYS, how="inner", validate="many_to_one")
    return linked, len(frame) - len(linked)


def price_sample_by_day(price_daily: pd.DataFrame) -> pd.DataFrame:
    """Summarize the observed product means and expose changes in sample membership."""
    columns = ["date", "currency", "avg_price", "product_count", "common_products", "entered_products", "exited_products"]
    rows = []
    for currency, prices in price_daily.groupby("currency"):
        previous = None
        for day, sample in prices.groupby("date", sort=True):
            products = set(sample[KEYS].itertuples(index=False, name=None))
            rows.append((day, currency, sample["avg_price"].mean(), len(products),
                         len(products & previous) if previous is not None else None,
                         len(products - previous) if previous is not None else None,
                         len(previous - products) if previous is not None else None))
            previous = products
    return pd.DataFrame(rows, columns=columns)


@dataclass
class BasicReport:
    summary: dict
    coverage: dict
    brands: pd.DataFrame
    categories: pd.DataFrame
    sellers: pd.DataFrame
    catalog_prices: pd.DataFrame
    products: pd.DataFrame
    star_distribution: pd.DataFrame
    review_daily: pd.DataFrame
    price_daily: pd.DataFrame
    price_changes: pd.DataFrame

    def price_movements(self) -> pd.DataFrame:
        """Compare the first and last observed daily mean per product and currency."""
        change = self.price_changes["change_pct"]
        counts = pd.DataFrame({
            "currency": self.price_changes["currency"],
            "price_up": change.gt(0),
            "price_down": change.lt(0),
            "same_endpoints": change.eq(0),
            "one_day_only": change.isna(),
        })
        return counts.groupby("currency", as_index=False).sum()

    def rankings(self, min_reviews: int = 3) -> pd.DataFrame:
        if type(min_reviews) is not int or min_reviews < 1:
            raise ValueError("Minimum review count must be a positive integer")
        return (
            self.products.loc[self.products["rating_sample_size"].ge(min_reviews)]
            .sort_values(["avg_rating", "rating_sample_size", "product_id"], ascending=[False, False, True])
            .reset_index(drop=True)
        )


def build_basic_report(
    products: pd.DataFrame | None,
    reviews: pd.DataFrame | None,
    price_daily: pd.DataFrame | None,
    *,
    source_group: str | None = None,
    brands: Sequence[str] = (),
    categories: Sequence[str] = (),
    sellers: Sequence[str] = (),
    start_date: date | str | None = None,
    end_date: date | str | None = None,
) -> BasicReport:
    start, end = _bounds(start_date, end_date)
    catalog, reviews, prices = (_prepare(frame) for frame in (products, reviews, price_daily))
    groups = source_groups(catalog, reviews, prices)
    if source_group is None and len(groups) > 1:
        raise ValueError("Choose one source group before calculating a report")
    group = source_group if source_group is not None else (groups[0] if groups else "unverified")
    catalog, reviews, prices = (
        frame.loc[frame["source_group"].eq(group)].copy() for frame in (catalog, reviews, prices)
    )
    catalog = catalog.loc[catalog["product_id"].notna() & catalog["product_id"].astype(str).str.strip().ne("")].copy()
    if catalog.duplicated(KEYS).any():
        raise ValueError("Ambiguous catalog identity")
    reviews, unmatched_reviews = _linked(reviews, catalog)
    prices, unmatched_prices = _linked(prices, catalog)
    for name, selected in (("brand", brands), ("category", categories), ("seller", sellers)):
        if selected:
            catalog = catalog.loc[_column(catalog, name).isin(selected)].copy()
    reviews, _ = _linked(reviews, catalog)
    prices, _ = _linked(prices, catalog)
    undated_reviews = int(_dates(_column(reviews, "review_date")).isna().sum())
    undated_prices = int(_dates(_column(prices, "date")).isna().sum())
    reviews = _in_dates(reviews, "review_date", start, end)
    prices = _in_dates(prices, "date", start, end)
    ratings = pd.to_numeric(_column(reviews, "rating"), errors="coerce")
    valid = reviews.loc[ratings.between(1, 5)].copy()
    valid["rating"] = ratings.loc[valid.index]
    sample = len(valid)
    summary = {
        "products": len(catalog),
        "reviews": len(reviews),
        "valid_ratings": sample,
        "avg_rating": float(valid["rating"].mean()) if sample else None,
        "positive_ratio": float(valid["rating"].ge(4).mean()) if sample else None,
        "negative_ratio": float(valid["rating"].le(2).mean()) if sample else None,
    }
    per_product = valid.groupby(KEYS)["rating"].agg(rating_sample_size="count", avg_rating="mean").reset_index()
    metrics = catalog.merge(per_product, on=KEYS, how="left", validate="one_to_one")
    metrics["rating_sample_size"] = metrics["rating_sample_size"].fillna(0).astype(int)

    def compare(dimension):
        data = metrics.copy()
        data[dimension] = _column(data, dimension).fillna("Chưa xác định").replace("", "Chưa xác định")
        data["_rating_sum"] = (data["avg_rating"] * data["rating_sample_size"]).fillna(0)
        result = (
            data.groupby(dimension, dropna=False)
            .agg(
                product_count=("product_id", "count"),
                rating_sample_size=("rating_sample_size", "sum"),
                _rating_sum=("_rating_sum", "sum"),
            )
            .reset_index()
        )
        result["avg_rating"] = result.pop("_rating_sum") / result["rating_sample_size"].replace(0, np.nan)
        return result.sort_values(["rating_sample_size", dimension], ascending=[False, True]).reset_index(drop=True)

    units = _column(catalog, "currency").map(_currency)
    catalog_price = pd.to_numeric(_column(catalog, "price"), errors="coerce")
    comparable = catalog.loc[units.notna() & catalog_price.gt(0) & np.isfinite(catalog_price)].copy()
    comparable["currency"] = units.loc[comparable.index]
    comparable["price"] = catalog_price.loc[comparable.index]
    price_summary = (
        comparable.groupby("currency")["price"]
        .agg(price_sample_size="count", avg_price="mean", median_price="median", min_price="min", max_price="max")
        .reset_index()
    )
    stars = (
        valid["rating"]
        .value_counts()
        .reindex([1, 2, 3, 4, 5], fill_value=0)
        .rename_axis("rating")
        .reset_index(name="review_count")
    )
    # Fractional ratings are valid in statistics but are not manufactured into star bins.
    stars.attrs["fractional_ratings"] = int((valid["rating"] % 1 != 0).sum())
    known_units = _column(prices, "currency").map(_currency)
    observed_price = pd.to_numeric(_column(prices, "avg_price"), errors="coerce")
    unknown_currency = int(known_units.isna().sum())
    dates = _dates(_column(prices, "date"))
    usable = dates.notna() & known_units.notna() & observed_price.gt(0) & np.isfinite(observed_price)
    history = prices.loc[usable].copy()
    history["date"] = dates.loc[usable].dt.date
    history["currency"] = known_units.loc[usable]
    history["avg_price"] = observed_price.loc[usable]
    if history.duplicated(KEYS + ["currency", "date"]).any():
        raise ValueError("Duplicate daily price identity; choose one published snapshot")
    history = history.sort_values(KEYS + ["currency", "date"]).reset_index(drop=True)
    changes = []
    for (source, product, unit), observations in history.groupby(KEYS + ["currency"]):
        first, last = observations.iloc[0], observations.iloc[-1]
        enough = len(observations) >= 2
        changes.append(
            {
                "source_group": source,
                "product_id": product,
                "currency": unit,
                "first_date": first["date"],
                "last_date": last["date"],
                "first_daily_avg": first["avg_price"],
                "last_daily_avg": last["avg_price"],
                "observed_days": len(observations),
                "change_pct": (last["avg_price"] / first["avg_price"] - 1) * 100 if enough else np.nan,
                "status": "available" if enough else "insufficient_data",
            }
        )
    price_changes = pd.DataFrame(
        changes,
        columns=[
            "source_group",
            "product_id",
            "currency",
            "first_date",
            "last_date",
            "first_daily_avg",
            "last_daily_avg",
            "observed_days",
            "change_pct",
            "status",
        ],
    )
    coverage = {
        "catalog_seller_known": int(_column(catalog, "seller").astype("string").str.strip().fillna("").ne("").sum()),
        "rated_products": len(per_product),
        "top_three_ratings": int(per_product["rating_sample_size"].nlargest(3).sum()),
        "price_comparable_products": price_changes.loc[price_changes["status"].eq("available"), "product_id"].nunique(),
        "unmatched_reviews": unmatched_reviews,
        "unmatched_prices": unmatched_prices,
        "undated_reviews": undated_reviews,
        "undated_prices": undated_prices,
        "unknown_currency_prices": unknown_currency,
        "invalid_ratings": len(reviews) - sample,
        "excluded_price_rows": len(prices) - len(history),
        "catalog_price_excluded": len(catalog) - len(comparable),
    }
    return BasicReport(
        summary,
        coverage,
        compare("brand"),
        compare("category"),
        compare("seller"),
        price_summary,
        metrics,
        stars,
        TrendAnalyzer.daily_review_trends(valid),
        history,
        price_changes,
    )
