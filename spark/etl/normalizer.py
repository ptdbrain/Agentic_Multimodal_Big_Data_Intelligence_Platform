import re
from typing import Dict, Any, List

BRAND_MAP = {
    "apple": "Apple",
    "apple inc.": "Apple",
    "apple inc": "Apple",
    "samsung": "Samsung",
    "samsung electronics": "Samsung",
    "xiaomi": "Xiaomi",
    "mi": "Xiaomi",
    "sony": "Sony",
    "asus": "Asus",
    "dell": "Dell",
    "lenovo": "Lenovo",
    "google": "Google"
}

CATEGORY_MAP = {
    "phone": "Smartphone",
    "smartphones": "Smartphone",
    "smartphone": "Smartphone",
    "laptop": "Laptop",
    "laptops": "Laptop",
    "tablet": "Tablet",
    "tablets": "Tablet",
    "accessory": "Accessories",
    "accessories": "Accessories",
    "headphone": "Accessories"
}

class EntityNormalizer:
    """Normalizes brands, categories, currency, and formats (Section 18 in plan.md)."""

    @staticmethod
    def normalize_brand(raw_brand: str) -> str:
        if not raw_brand:
            return "Unknown"
        b = raw_brand.strip().lower()
        return BRAND_MAP.get(b, raw_brand.strip().title())

    @staticmethod
    def normalize_category(raw_cat: str) -> str:
        if not raw_cat:
            return "Other"
        c = raw_cat.strip().lower()
        return CATEGORY_MAP.get(c, raw_cat.strip().title())

    @staticmethod
    def normalize_price(price_val: Any) -> float:
        if isinstance(price_val, (int, float)):
            return float(price_val)
        if isinstance(price_val, str):
            # Remove dots/commas and currency indicators e.g. "19.990.000 VND" -> 19990000.0
            cleaned = re.sub(r"[^\d.]", "", price_val.replace(".", "").replace(",", "."))
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        return 0.0

    @staticmethod
    def normalize_product(product: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(product)
        item["brand"] = EntityNormalizer.normalize_brand(item.get("brand", ""))
        item["category"] = EntityNormalizer.normalize_category(item.get("category", ""))
        item["price"] = EntityNormalizer.normalize_price(item.get("price", 0))
        item["currency"] = "VND"
        return item
