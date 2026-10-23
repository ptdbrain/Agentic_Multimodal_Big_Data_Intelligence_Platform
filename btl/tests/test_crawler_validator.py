"""Unit tests for TikiCrawler, PoliteTikiCrawler, and CrawlerDataValidator (pre-Spark guard)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch
import pytest

from ingestion.crawler.tiki import (
    PoliteTikiCrawler,
    TikiCrawler,
    extract_page_fields,
    find_json_product,
    parse_price,
    product_id_from_url,
)
from ingestion.crawler.validator import CrawlerDataValidator, validate_crawler_payloads


@pytest.mark.unit
class TestTikiCrawlerFallbackElimination:
    """Verify that TikiCrawler in real crawl mode strictly eliminates hardcoded mock fallback."""

    def test_default_disallows_public_fallback(self):
        crawler = TikiCrawler()
        assert crawler.allow_public_fallback is False

    def test_search_products_fails_without_synthetic_fallback(self):
        """When live requests fail or return empty, real crawl mode returns empty list, NEVER base_catalog."""
        crawler = TikiCrawler(allow_public_fallback=False, use_polite_scraper=False)
        with patch.object(crawler, "_request_with_retry", return_value=None):
            results = crawler.search_products("smartphones", limit=5)
            assert results == []
            assert not any("iPhone 15 Pro Max" in str(r) for r in results)

    def test_get_product_reviews_fails_without_synthetic_fallback(self):
        """When review endpoint fails, real crawl mode returns empty list, NEVER fake customer feedback."""
        crawler = TikiCrawler(allow_public_fallback=False)
        with patch.object(crawler.polite_crawler, "fetch_product_reviews", return_value=[]), patch.object(
            crawler, "_request_with_retry", return_value=None
        ):
            revs = crawler.get_product_reviews("278505394", limit=5)
            assert revs == []
            assert not any("Hàng chính hãng Tiki Trading" in str(r) for r in revs)

    def test_explicit_legacy_fallback_works_when_explicitly_requested(self):
        """For legacy tests that explicitly ask for allow_public_fallback=True."""
        crawler = TikiCrawler(allow_public_fallback=True, use_polite_scraper=False)
        with patch.object(crawler, "_request_with_retry", return_value=None):
            prods = crawler.search_products("test", limit=3)
            assert len(prods) == 3
            revs = crawler.get_product_reviews("123", limit=2)
            assert len(revs) == 2


@pytest.mark.unit
class TestTikiExtractionLogic:
    """Verify HTML parsing, price parsing, and product ID extraction."""

    def test_product_id_from_url(self):
        url = "https://tiki.vn/dien-thoai-samsung-galaxy-a17-lte-8-128gb-hang-chinh-hang-p278505394.html?spid=278505405"
        assert product_id_from_url(url) == "tiki_278505394"

        invalid_url = "https://tiki.vn/dien-thoai-smartphone/c1795"
        assert product_id_from_url(invalid_url) is None

    def test_parse_price_formats(self):
        assert parse_price(5990000) == 5990000.0
        assert parse_price("5.990.000 ₫") == 5990000.0
        assert parse_price("27,990,000 VND") == 27990000.0
        assert parse_price({"price": "1200000"}) == 1200000.0
        assert parse_price(-500) is None
        assert parse_price("invalid") is None
        assert parse_price(None) is None

    def test_extract_page_fields_from_json_ld(self):
        sample_html = """
        <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "https://schema.org",
                "@type": "Product",
                "name": "Samsung Galaxy S24",
                "brand": {"@type": "Brand", "name": "Samsung"},
                "offers": {
                    "@type": "Offer",
                    "price": "22990000",
                    "priceCurrency": "VND"
                },
                "aggregateRating": {
                    "@type": "AggregateRating",
                    "ratingValue": "4.8",
                    "reviewCount": "45"
                }
            }
            </script>
        </head>
        <body>
            <h1>Samsung Galaxy S24</h1>
        </body>
        </html>
        """
        url = "https://tiki.vn/samsung-galaxy-s24-p99887766.html"
        fields = extract_page_fields(sample_html, url, category_seed="smartphones")
        assert fields["product_id"] == "tiki_99887766"
        assert fields["product_name"] == "Samsung Galaxy S24"
        assert fields["brand"] == "Samsung"
        assert fields["price"] == 22990000.0
        assert fields["currency"] == "VND"
        assert fields["rating"] == 4.8
        assert fields["review_count"] == 45


@pytest.mark.unit
class TestCrawlerDataValidator:
    """Pre-Spark verification of field names, data types, product IDs, and prices."""

    def test_valid_product_and_price_pass_validation(self):
        prod = {
            "product_id": "tiki_278505394",
            "product_name": "Samsung Galaxy A17",
            "brand": "Samsung",
            "category": "smartphones",
            "price": 5990000.0,
            "currency": "VND",
            "rating": 5.0,
            "review_count": 11,
            "seller": "Tiki Trading",
            "product_url": "https://tiki.vn/p278505394.html",
            "image_url": "https://salt.tikicdn.com/img.jpg",
            "source": "tiki_crawler",
            "created_at": "2026-10-18T10:00:00Z",
            "updated_at": "2026-10-18T10:00:00Z",
        }
        price = {
            "price_id": "pr_tiki_278505394_0001",
            "product_id": "tiki_278505394",
            "price": 5990000.0,
            "currency": "VND",
            "seller": "Tiki Trading",
            "timestamp": "2026-10-18T10:00:00Z",
            "source": "tiki_crawler",
        }

        report = CrawlerDataValidator.validate_crawled_dataset(
            products=[prod], prices=[price], enforce_tiki_id_format=True
        )
        assert report["is_valid"] is True
        assert report["stats"]["products_valid"] == 1
        assert report["stats"]["prices_valid"] == 1
        assert len(report["errors"]) == 0

    def test_missing_required_fields_detected(self):
        invalid_prod = {
            "product_id": "tiki_123",
            "price": 1000.0,
            # Missing product_name, brand, category, currency, source
        }
        ok, errors = CrawlerDataValidator.validate_product(invalid_prod)
        assert ok is False
        assert any("Missing required product fields" in e for e in errors)

    def test_invalid_price_and_type_rejected(self):
        invalid_prod = {
            "product_id": "tiki_123",
            "product_name": "Test Phone",
            "brand": "TestBrand",
            "category": "Electronics",
            "price": -100.0,  # Negative price disallowed!
            "currency": "VND",
            "source": "tiki_crawler",
        }
        ok, errors = CrawlerDataValidator.validate_product(invalid_prod)
        assert ok is False
        assert any("Invalid price" in e for e in errors)

    def test_invalid_product_id_rejected(self):
        invalid_prod = {
            "product_id": "malformed_id_without_tiki_prefix",
            "product_name": "Test Phone",
            "brand": "TestBrand",
            "category": "Electronics",
            "price": 50000.0,
            "currency": "VND",
            "source": "tiki_crawler",
        }
        ok, errors = CrawlerDataValidator.validate_product(invalid_prod, enforce_tiki_id_format=True)
        assert ok is False
        assert any("Invalid product_id format" in e for e in errors)

    def test_price_referential_integrity_mismatch_rejected(self):
        prod = {
            "product_id": "tiki_111",
            "product_name": "Phone 1",
            "brand": "Brand",
            "category": "Tech",
            "price": 1000.0,
            "currency": "VND",
            "source": "tiki_crawler",
        }
        orphan_price = {
            "price_id": "pr_orphan_1",
            "product_id": "tiki_999",  # Does NOT match tiki_111!
            "price": 1000.0,
            "currency": "VND",
            "timestamp": "2026-10-18T10:00:00Z",
            "source": "tiki_crawler",
        }
        report = CrawlerDataValidator.validate_crawled_dataset(
            products=[prod], prices=[orphan_price]
        )
        assert report["is_valid"] is False
        assert any("does not exist in valid products set" in e for e in report["errors"])

    def test_pre_spark_guard_raises_in_strict_mode(self):
        bad_prod = {"product_id": "", "price": "invalid"}
        with pytest.raises(ValueError, match="Pre-Spark Validation Guard FAILED"):
            CrawlerDataValidator.verify_pre_spark_guard(
                products=[bad_prod], prices=[], strict=True
            )

    def test_genuine_review_validation_passes(self):
        prod = {
            "product_id": "tiki_278505394",
            "product_name": "Samsung Galaxy A17",
            "brand": "Samsung",
            "category": "smartphones",
            "price": 5990000.0,
            "currency": "VND",
            "source": "tiki_crawler",
        }
        price = {
            "price_id": "pr_tiki_278505394_0001",
            "product_id": "tiki_278505394",
            "price": 5990000.0,
            "currency": "VND",
            "timestamp": "2026-10-18T10:00:00Z",
            "source": "tiki_crawler",
        }
        rev = {
            "review_id": "tiki_rv_20243132",
            "product_id": "tiki_278505394",
            "user_id": "anon_efe652cd4c50fad1",
            "rating": 4.0,
            "review_title": "Hài lòng",
            "review_text": "Điện thoại tốt trong tầm giá.",
            "review_date": "2026-06-27T22:51:06Z",
            "verified_purchase": True,
            "helpful_count": 0,
            "source": "tiki_crawler",
            "language": "vi",
            "created_at": "2026-06-27T22:51:06Z",
        }
        report = CrawlerDataValidator.validate_crawled_dataset(
            products=[prod], prices=[price], reviews=[rev], enforce_tiki_id_format=True
        )
        assert report["is_valid"] is True
        assert report["stats"]["reviews_valid"] == 1
        assert report["stats"]["reviews_invalid"] == 0

    def test_review_missing_fields_or_unanonymized_user_rejected(self):
        # 1. Missing review_text
        bad_rev1 = {
            "review_id": "tiki_rv_1",
            "product_id": "tiki_123",
            "user_id": "anon_123",
            "rating": 5.0,
            "review_title": "Good",
            "review_date": "2026-10-18T10:00:00Z",
            # missing review_text
        }
        ok, errs = CrawlerDataValidator.validate_review(bad_rev1)
        assert ok is False
        assert any("Missing required review fields" in e for e in errs)

        # 2. Raw unanonymized user_id (PII leak)
        bad_rev2 = {
            "review_id": "tiki_rv_2",
            "product_id": "tiki_123",
            "user_id": "raw_customer_name_not_hashed",
            "rating": 5.0,
            "review_title": "Good",
            "review_text": "Nice phone",
            "review_date": "2026-10-18T10:00:00Z",
        }
        ok, errs = CrawlerDataValidator.validate_review(bad_rev2)
        assert ok is False
        assert any("not properly anonymized/hashed" in e for e in errs)

    def test_review_referential_mismatch_rejected(self):
        prod = {
            "product_id": "tiki_111",
            "product_name": "Phone 1",
            "brand": "Brand",
            "category": "Tech",
            "price": 1000.0,
            "currency": "VND",
            "source": "tiki_crawler",
        }
        orphan_rev = {
            "review_id": "tiki_rv_orphan",
            "product_id": "tiki_999",  # Does NOT match tiki_111!
            "user_id": "anon_0123456789abcdef",
            "rating": 5.0,
            "review_title": "Title",
            "review_text": "Text",
            "review_date": "2026-10-18T10:00:00Z",
        }
        report = CrawlerDataValidator.validate_crawled_dataset(
            products=[prod], prices=[], reviews=[orphan_rev]
        )
        assert report["is_valid"] is False
        assert any("Review product_id 'tiki_999' does not exist" in e for e in report["errors"])


@pytest.mark.unit
class TestTikiGenuineReviews:
    """Verify genuine customer review fetching and standardization without fake simulation."""

    def test_fetch_product_reviews_genuine_response(self):
        crawler = PoliteTikiCrawler(delay_seconds=2.0)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {
                    "id": 112233,
                    "title": "Rất hài lòng",
                    "content": "Máy dùng mượt mà, pin trâu.",
                    "rating": 5,
                    "created_at": 1782600000,
                    "customer_id": 998877,
                    "thank_count": 2,
                    "purchased": True,
                }
            ]
        }
        with patch.object(crawler, "_check_robots"), patch.object(crawler.session, "get", return_value=mock_response):
            reviews = crawler.fetch_product_reviews("278505394", limit=2)
            assert len(reviews) == 1
            assert reviews[0]["id"] == 112233
            assert reviews[0]["content"] == "Máy dùng mượt mà, pin trâu."

    def test_standardize_review_hashes_pii_and_conforms_to_schema(self):
        tiki_crawler = TikiCrawler(allow_public_fallback=False)
        raw_rev = {
            "id": 887766,
            "title": "Chất lượng tốt",
            "content": "Đóng gói cẩn thận",
            "rating": 4,
            "created_at": 1782600000,
            "customer_id": 554433,
            "thank_count": 1,
            "purchased": True,
        }
        std = tiki_crawler.standardize_review(raw_rev, product_id="tiki_278505394")
        assert std["review_id"] == "tiki_rv_887766"
        assert std["product_id"] == "tiki_278505394"
        assert std["rating"] == 4.0
        assert std["review_title"] == "Chất lượng tốt"
        assert std["review_text"] == "Đóng gói cẩn thận"
        assert std["user_id"].startswith("anon_")
        assert "554433" not in std["user_id"]
        assert std["source"] == "tiki_crawler"
        assert std["verified_purchase"] is True

