import pytest

pytestmark = pytest.mark.unit


class Pages:
    def __init__(self, rows):
        self.rows = rows
        self.pages = []

    def get_product_reviews(self, product_id, limit, page, sort, strict=False):
        self.pages.append(page)
        return self.rows[(page - 1) * 20 : page * 20]


def test_incremental_reviews_page_past_twenty_and_stop_at_observed_identity():
    from ingestion.review_cursor import collect_review_delta

    crawler = Pages([{"id": str(i)} for i in range(45, 0, -1)])
    result = collect_review_delta(crawler, "p", {"ids": ["5"]}, max_pages=4)
    assert len(result["rows"]) == 40 and result["complete"]
    assert crawler.pages == [1, 2, 3]
    assert "5" in result["cursor"]["ids"]


def test_bounded_gap_never_advances_committed_cursor():
    from ingestion.review_cursor import collect_review_delta

    old = {"ids": ["old"]}
    result = collect_review_delta(Pages([{"id": str(i)} for i in range(60)]), "p", old, max_pages=2)
    assert len(result["rows"]) == 40 and not result["complete"]
    assert result["cursor"]["ids"] == old["ids"]
    assert result["cursor"]["pending"]["anchor"] == "39"
    assert old == {"ids": ["old"]}


def test_large_backlog_resumes_with_bounded_requests_without_committing_gaps():
    from ingestion.review_cursor import collect_review_delta

    rows = [{"id": str(i)} for i in range(105, 0, -1)]
    cursor = {"ids": []}
    captured = []
    for attempt in range(8):
        crawler = Pages(rows)
        result = collect_review_delta(crawler, "p", cursor, max_pages=2)
        captured.extend(result["rows"])
        assert len(crawler.pages) <= 2
        if result["complete"]:
            cursor = result["cursor"]
            break
        assert result["cursor"]["ids"] == []
        cursor = result["cursor"]
        if attempt == 0:
            rows = [{"id": "107"}, {"id": "106"}] + rows
    else:
        pytest.fail("Backlog never made progress")
    assert {row["id"] for row in captured} == {str(i) for i in range(1, 106)}
    fresh = collect_review_delta(Pages(rows), "p", cursor, max_pages=2)
    assert [row["id"] for row in fresh["rows"]] == ["107", "106"]
    assert fresh["complete"]


def test_deleted_resume_anchor_restarts_without_advancing_committed_frontier():
    from ingestion.review_cursor import collect_review_delta

    rows = [{"id": str(i)} for i in range(45, 0, -1)]
    first = collect_review_delta(Pages(rows), "p", max_pages=1)
    remaining = [row for row in rows if row["id"] != "26"]
    result = collect_review_delta(Pages(remaining), "p", first["cursor"], max_pages=4)
    assert not result["complete"] and result["cursor"]["ids"] == []
    assert result["reason"] == "resume_anchor_missing"
    recovered = collect_review_delta(Pages(remaining), "p", result["cursor"], max_pages=4)
    assert recovered["complete"]
    assert {row["id"] for row in recovered["rows"]} == {row["id"] for row in remaining}
