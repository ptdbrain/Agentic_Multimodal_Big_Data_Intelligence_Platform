"""Bounded newest-first discovery with an anchored, durable backlog continuation.

The caller must persist returned cursors only after returned rows are durable.
Pending IDs are not a committed frontier. New head arrivals are discovered on
 the first incremental pass after the anchored backlog completes.
"""

import copy


def collect_review_delta(crawler, product_id, cursor=None, max_pages=10):
    if type(max_pages) is not int or max_pages < 1:
        raise ValueError("Positive page budget required")
    cursor = copy.deepcopy(cursor or {"ids": []})
    known = set(cursor.get("ids", []))
    pending = cursor.get("pending", {})
    pending_ids = set(pending.get("ids", []))
    anchor = pending.get("anchor")
    page = int(pending.get("page", 1))
    searching = anchor is not None
    seen = set()
    rows = []
    complete = False
    last_identity = anchor
    for request in range(max_pages):
        values = crawler.get_product_reviews(product_id, limit=20, page=page, sort="id|desc", strict=True)
        boundary = False
        for item in values:
            if not isinstance(item, dict) or item.get("id") is None:
                raise ValueError("Observed review identity required for cursor")
            identity = str(item["id"])
            if searching:
                if identity == anchor:
                    searching = False
                continue
            if identity in known:
                boundary = True
                break
            if identity not in seen and identity not in pending_ids:
                rows.append(item)
                seen.add(identity)
            last_identity = identity
        if len(values) < 20 and searching:
            # The anchor disappeared (e.g. moderation/deletion). Restart a full
            # scan next time; never declare a gap complete from page numbers.
            return {
                "rows": [],
                "complete": False,
                "pages": request + 1,
                "reason": "resume_anchor_missing",
                "cursor": {"ids": sorted(known)},
            }
        if boundary or (len(values) < 20 and not searching):
            complete = True
            break
        page += 1
    if complete:
        next_cursor = {"ids": sorted(known | pending_ids | seen)}
    else:
        next_cursor = {
            "ids": sorted(known),
            "pending": {
                "ids": sorted(pending_ids | seen),
                "anchor": anchor if searching else last_identity,
                "page": page if searching else page - 1,
            },
        }
    return {
        "rows": rows,
        "complete": complete,
        "pages": request + 1,
        "reason": None if complete else "page_budget_reached",
        "cursor": next_cursor,
    }
