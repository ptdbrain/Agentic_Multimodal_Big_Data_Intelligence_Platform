from typing import List, Dict, Any

class DataCleaner:
    """Handles field-specific missing, malformed, and anomalous data.
    Section 17 in plan.md:
    - review_text NULL -> drop review
    - price NULL -> keep review, flag for price calculations
    - rating NULL -> impute or handle gracefully
    """

    @staticmethod
    def clean_reviews(reviews: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned = []
        for r in reviews:
            # Rule 1: Drop if review_text is missing
            text = r.get("review_text")
            if not text or not isinstance(text, str) or str(text).strip() == "":
                continue

            item = dict(r)
            # Rule 2: Clean and trim text
            item["review_text"] = item["review_text"].strip()
            item["review_title"] = (item.get("review_title") or "No Title").strip()

            # Rule 3: Validate rating
            rating = item.get("rating")
            if rating is None or not isinstance(rating, (int, float)):
                continue
            item["rating"] = max(1.0, min(5.0, float(rating)))

            cleaned.append(item)
        return cleaned

    @staticmethod
    def clean_products(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned = []
        for p in products:
            if not p.get("product_id") or not p.get("product_name"):
                continue
            item = dict(p)
            price = item.get("price")
            if price is not None:
                try:
                    item["price"] = max(0.0, float(price))
                except (ValueError, TypeError):
                    item["price"] = 0.0
            cleaned.append(item)
        return cleaned
