"""Independent arithmetic oracles for basic, descriptive warehouse analysis."""

import pandas as pd
import pytest

pytestmark = pytest.mark.unit


def inputs():
    products = pd.DataFrame(
        [
            {"product_id": "p1", "brand": "A", "category": "Phone", "price": 100, "currency": "VND"},
            {"product_id": "p2", "brand": "A", "category": "Phone", "price": 300, "currency": "VND"},
            {"product_id": "p3", "brand": "B", "category": "Tablet", "price": 20, "currency": "USD"},
            {"product_id": "p4", "brand": "A", "category": "Tablet", "price": 999, "currency": None},
        ]
    ).assign(source_group="marketplace")
    reviews = pd.DataFrame(
        [
            {"review_id": "r1", "product_id": "p1", "rating": 1, "review_date": "2026-01-01"},
            {"review_id": "r2", "product_id": "p1", "rating": 5, "review_date": "2026-01-03"},
            {"review_id": "r3", "product_id": "p1", "rating": 5, "review_date": None},
            {"review_id": "r4", "product_id": "p2", "rating": 1, "review_date": "2026-01-02"},
            {"review_id": "r5", "product_id": "p3", "rating": 6, "review_date": "2026-01-03"},
            {"review_id": "r6", "product_id": "p3", "rating": None, "review_date": "2026-01-03"},
            {"review_id": "orphan", "product_id": "missing", "rating": 4, "review_date": "2026-01-03"},
        ]
    ).assign(source_group="marketplace")
    prices = pd.DataFrame(
        [
            {"product_id": "p1", "date": "2026-01-01", "avg_price": 100, "currency": "VND", "data_points": 10},
            {"product_id": "p1", "date": "2026-01-03", "avg_price": 80, "currency": "VND", "data_points": 1},
            {"product_id": "p2", "date": "2026-01-01", "avg_price": 300, "currency": "VND", "data_points": 1},
            {"product_id": "p3", "date": "2026-01-01", "avg_price": 20, "currency": "USD", "data_points": 1},
            {"product_id": "p3", "date": "2026-01-03", "avg_price": 30, "currency": "USD", "data_points": 1},
            {"product_id": "p1", "date": "2026-01-04", "avg_price": 10000, "currency": None, "data_points": 1},
        ]
    ).assign(source_group="marketplace")
    return products, reviews, prices


def test_basic_summary_uses_collected_samples_and_separates_currencies():
    from analytics.basic import build_basic_report

    p, r, prices = inputs()
    report = build_basic_report(p, r, prices)
    assert report.summary["products"] == 4 and report.summary["reviews"] == 6
    assert report.summary["valid_ratings"] == 4 and report.summary["avg_rating"] == 3
    assert report.summary["positive_ratio"] == 0.5 and report.summary["negative_ratio"] == 0.5
    assert report.coverage["unmatched_reviews"] == 1 and report.coverage["undated_reviews"] == 1
    assert report.coverage["unknown_currency_prices"] == 1
    assert report.star_distribution["review_count"].tolist() == [2, 0, 0, 0, 2]
    brand = report.brands.set_index("brand").loc["A"]
    assert brand["avg_rating"] == 3 and brand["rating_sample_size"] == 4 and brand["product_count"] == 3
    assert pd.isna(report.brands.set_index("brand").loc["B", "avg_rating"])
    units = report.catalog_prices.set_index("currency")
    assert units.loc["VND", "median_price"] == 200 and units.loc["USD", "median_price"] == 20
    ranked = report.rankings(min_reviews=2)
    assert ranked["product_id"].tolist() == ["p1"] and ranked.iloc[0]["avg_rating"] == pytest.approx(11 / 3)


def test_price_changes_use_first_last_observed_daily_averages_and_require_two_dates():
    from analytics.basic import build_basic_report

    p, r, prices = inputs()
    report = build_basic_report(p, r, prices)
    changes = report.price_changes.set_index("product_id")
    assert changes.loc["p1", "change_pct"] == pytest.approx(-20) and changes.loc["p1", "observed_days"] == 2
    assert changes.loc["p3", "change_pct"] == 50 and changes.loc["p3", "currency"] == "USD"
    assert pd.isna(changes.loc["p2", "change_pct"])
    assert changes.loc["p2", "status"] == "insufficient_data"
    assert len(report.price_daily) == 5


def test_filters_use_inclusive_utc_dates_and_current_catalog_context():
    from analytics.basic import build_basic_report

    p, r, prices = inputs()
    report = build_basic_report(p, r, prices, start_date="2026-01-02", end_date="2026-01-03", brands=["A"])
    assert report.summary["products"] == 3 and report.summary["valid_ratings"] == 2
    assert report.summary["avg_rating"] == 3 and report.coverage["undated_reviews"] == 1
    assert report.price_changes["product_id"].tolist() == ["p1"]
    assert pd.isna(report.price_changes.iloc[0]["change_pct"])
    assert set(report.review_daily["date"].astype(str)) == {"2026-01-02", "2026-01-03"}
    with pytest.raises(ValueError, match="date|Date"):
        build_basic_report(p, r, prices, start_date="2026-01-03", end_date="2026-01-01")


def test_sources_cannot_cross_join_and_ambiguous_catalog_is_refused():
    from analytics.basic import build_basic_report

    p, r, prices = inputs()
    simulated = p.iloc[:1].assign(source_group="simulation", price=99999)
    p = pd.concat([p, simulated], ignore_index=True)
    r = pd.concat(
        [
            r,
            pd.DataFrame(
                [{"product_id": "p1", "rating": 5, "review_date": "2026-01-01", "source_group": "simulation"}]
            ),
        ],
        ignore_index=True,
    )
    with pytest.raises(ValueError, match="source"):
        build_basic_report(p, r, prices)
    report = build_basic_report(p, r, prices, source_group="marketplace")
    assert report.summary["avg_rating"] == 3 and report.summary["products"] == 4
    with pytest.raises(ValueError, match="identity|catalog"):
        build_basic_report(pd.concat([p, p.iloc[:1]], ignore_index=True), r, prices, source_group="marketplace")


def test_empty_and_invalid_samples_stay_unavailable_and_do_not_mutate_input():
    from analytics.basic import build_basic_report

    report = build_basic_report(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    assert report.summary["avg_rating"] is None and report.summary["positive_ratio"] is None
    assert report.price_changes.empty and report.rankings(1).empty
    p, r, prices = inputs()
    original = r.copy(deep=True)
    report = build_basic_report(p, r.assign(rating=float("inf")), prices)
    assert report.summary["valid_ratings"] == 0 and report.summary["avg_rating"] is None
    assert report.review_daily.empty
    pd.testing.assert_frame_equal(r, original)


def test_utc_end_day_is_inclusive_and_single_day_is_not_a_price_change():
    from analytics.basic import build_basic_report

    p, r, prices = inputs()
    r = r.iloc[:2].copy()
    r["review_date"] = ["2026-01-03T23:59:59Z", "2026-01-04T00:00:00Z"]
    report = build_basic_report(p, r, prices, start_date="2026-01-03", end_date="2026-01-03")
    assert report.summary["reviews"] == 1 and report.summary["avg_rating"] == 1
    assert report.price_changes["change_pct"].isna().all()


def test_fractional_ratings_are_not_rounded_into_star_bins_and_bad_prices_are_excluded():
    from analytics.basic import build_basic_report

    p, r, prices = inputs()
    r = r.iloc[:1].assign(rating=3.5)
    prices = prices.assign(avg_price=[float("inf"), -1, 0, 20, 30, 10000])
    report = build_basic_report(p, r, prices)
    assert report.summary["avg_rating"] == 3.5 and report.summary["valid_ratings"] == 1
    assert report.star_distribution["review_count"].sum() == 0
    assert report.star_distribution.attrs["fractional_ratings"] == 1
    assert set(report.price_daily["product_id"]) == {"p3"}
    assert report.coverage["excluded_price_rows"] == 4
    duplicated = pd.concat([prices, prices.iloc[3:4]], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate daily price"):
        build_basic_report(p, r, duplicated)


def test_missing_numpy_currency_cannot_become_a_fabricated_nan_unit():
    from analytics.basic import build_basic_report

    p, r, prices = inputs()
    p["currency"] = float("nan")
    prices["currency"] = float("nan")
    report = build_basic_report(p, r, prices)
    assert report.catalog_prices.empty and report.price_daily.empty
    assert report.coverage["unknown_currency_prices"] == len(prices)


def test_sample_coverage_and_concentration_follow_filtered_catalog():
    from analytics.basic import build_basic_report

    products, reviews, prices = inputs()
    report = build_basic_report(products, reviews, prices)
    assert report.coverage["rated_products"] == 2
    assert report.coverage["top_three_ratings"] == 4
    assert report.coverage["price_comparable_products"] == 2
    movements = report.price_movements().set_index("currency")
    assert movements.loc["USD"].tolist() == [1, 0, 0, 0]
    assert movements.loc["VND"].tolist() == [0, 1, 0, 1]
    filtered = build_basic_report(products, reviews, prices, brands=["B"])
    assert filtered.coverage["rated_products"] == filtered.coverage["top_three_ratings"] == 0
    assert filtered.coverage["price_comparable_products"] == 1


def test_article_source_breakdown_keeps_unknowns_and_uses_filtered_denominator():
    from analytics.basic import article_source_breakdown

    articles = pd.DataFrame({"source_name": ["A", "A", "B", None],
                             "published_at": ["2026-11-01", None, "invalid", "2026-11-03T01:00:00Z"]})
    result = article_source_breakdown(articles).set_index("source_name")
    assert result.loc["A"].tolist() == [2, 50.0, 1]
    assert result.loc["B"].tolist() == [1, 25.0, 0]
    assert result.loc["Chưa xác định"].tolist() == [1, 25.0, 1]
    assert article_source_breakdown(articles.iloc[:1]).iloc[0]["article_share_pct"] == 100
    assert article_source_breakdown(articles.iloc[:0]).empty
