from typing import List, Dict, Any, Optional, Tuple

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

    @staticmethod
    def validate_reviews(reviews: List[Dict[str, Any]], min_rating: float = 1.0, max_rating: float = 5.0) -> Tuple[List, List]:
        """Returns (valid_records, invalid_records). Invalid = out of range, NOT clamped."""
        valid_records = []
        invalid_records = []
        
        for r in reviews:
            raw_rating = r.get("rating")
            if raw_rating is None:
                invalid_records.append(r)
                continue
            
            try:
                numeric_rating = float(raw_rating)
                if min_rating <= numeric_rating <= max_rating:
                    valid_records.append(r)
                else:
                    invalid_records.append(r)
            except (ValueError, TypeError):
                invalid_records.append(r)
                
        return valid_records, invalid_records

    @staticmethod  
    def clean_text_fields(records: List[Dict[str, Any]], text_fields: List[str]) -> List[Dict[str, Any]]:
        """Pure cleaning: trim, fix encoding. No validation."""
        cleaned = []
        for r in records:
            item = dict(r)
            for field in text_fields:
                if field in item and isinstance(item[field], str):
                    item[field] = item[field].strip()
            cleaned.append(item)
        return cleaned

    @staticmethod
    def validate_and_clean(reviews: List[Dict[str, Any]], min_rating: float = 1.0, max_rating: float = 5.0) -> Dict[str, Any]:
        """Returns {'valid': [...], 'invalid': [...], 'stats': {'valid': N, 'invalid': N, 'repaired': N}}"""
        valid, invalid = DataCleaner.validate_reviews(reviews, min_rating, max_rating)
        cleaned_valid = DataCleaner.clean_text_fields(valid, ["review_text", "review_title"])
        
        return {
            'valid': cleaned_valid,
            'invalid': invalid,
            'stats': {
                'valid': len(cleaned_valid),
                'invalid': len(invalid),
                'repaired': 0
            }
        }
