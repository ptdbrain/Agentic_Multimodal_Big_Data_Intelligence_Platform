from typing import List, Dict, Any, Optional

class DataCleaner:
    """Handles field-specific missing, malformed, and anomalous data.
    Section 17 in plan.md:
    - Flexible schema validation and robust type conversion
    - Configurable minimum/maximum rating boundaries
    """

    @staticmethod
    def clean_reviews(
        reviews: List[Dict[str, Any]],
        min_rating: float = 1.0,
        max_rating: float = 5.0,
        default_title: str = "No Title",
        text_field: str = "review_text",
        title_field: str = "review_title",
        rating_field: str = "rating"
    ) -> List[Dict[str, Any]]:
        cleaned = []
        for r in reviews:
            # Rule 1: Drop if review_text is missing or empty
            text = r.get(text_field)
            if not text or not isinstance(text, str) or str(text).strip() == "":
                continue

            item = dict(r)
            # Rule 2: Clean and trim text
            item[text_field] = item[text_field].strip()
            item[title_field] = (item.get(title_field) or default_title).strip()

            # Rule 3: Validate & convert rating safely (int, float, or parseable string)
            raw_rating = item.get(rating_field)
            if raw_rating is None:
                continue
            try:
                numeric_rating = float(raw_rating)
                item[rating_field] = max(min_rating, min(max_rating, numeric_rating))
            except (ValueError, TypeError):
                continue

            cleaned.append(item)
        return cleaned

    @staticmethod
    def clean_products(
        products: List[Dict[str, Any]],
        id_field: str = "product_id",
        name_field: str = "product_name",
        price_field: str = "price",
        min_price: float = 0.0,
        default_price: float = 0.0
    ) -> List[Dict[str, Any]]:
        cleaned = []
        for p in products:
            p_id = p.get(id_field)
            p_name = p.get(name_field)
            if not p_id or not p_name:
                continue
            item = dict(p)
            price = item.get(price_field)
            if price is not None:
                try:
                    item[price_field] = max(min_price, float(price))
                except (ValueError, TypeError):
                    item[price_field] = default_price
            else:
                item[price_field] = default_price
            cleaned.append(item)
        return cleaned

