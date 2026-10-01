import re
from typing import Dict, Any, List, Optional
from config.settings import settings

DEFAULT_BRAND_MAP = {
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

DEFAULT_CATEGORY_MAP = {
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
    """Normalizes brands, categories, currency, and formats (Section 18 in plan.md).
    Fully parameterized to handle international formats and custom business rules.
    """

    @staticmethod
    def normalize_brand(raw_brand: str, custom_map: Optional[Dict[str, str]] = None) -> str:
        if not raw_brand:
            return "Unknown"
        b = str(raw_brand).strip().lower()
        mapping = custom_map or DEFAULT_BRAND_MAP
        return mapping.get(b, raw_brand.strip().title())

    @staticmethod
    def normalize_category(raw_cat: str, custom_map: Optional[Dict[str, str]] = None) -> str:
        if not raw_cat:
            return "Other"
        c = str(raw_cat).strip().lower()
        mapping = custom_map or DEFAULT_CATEGORY_MAP
        return mapping.get(c, raw_cat.strip().title())

    @staticmethod
    def normalize_price(price_val: Any) -> float:
        """Parses price numbers, supporting integer, float, and diverse string formats
        (both US 1,299.99 and European/Vietnamese 19.990.000 or 1.299,50).
        """
        if price_val is None:
            return 0.0
        if isinstance(price_val, (int, float)):
            return float(price_val)
        if isinstance(price_val, str):
            val = price_val.strip()
            # Strip currency symbols and letters
            cleaned = re.sub(r"[^\d.,]", "", val)
            if not cleaned:
                return 0.0

            # Case A: Both dot and comma present
            if "." in cleaned and "," in cleaned:
                last_dot = cleaned.rfind(".")
                last_comma = cleaned.rfind(",")
                if last_dot > last_comma:
                    # US style: 1,299.99 -> remove commas
                    cleaned = cleaned.replace(",", "")
                else:
                    # EU/VN style: 1.299,50 -> remove dots, comma to dot
                    cleaned = cleaned.replace(".", "").replace(",", ".")
            # Case B: Only commas present
            elif "," in cleaned:
                parts = cleaned.split(",")
                # If only 2 decimals after comma (e.g. 1299,50) -> treat as decimal
                if len(parts) == 2 and len(parts[1]) in (1, 2):
                    cleaned = parts[0] + "." + parts[1]
                else:
                    # Thousands separator: 1,000,000 -> 1000000
                    cleaned = cleaned.replace(",", "")
            # Case C: Only dots present
            elif "." in cleaned:
                parts = cleaned.split(".")
                # If multiple dots or 3 digits after dot (e.g. 19.990.000 or 25.000) -> thousands separator
                if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) == 3):
                    cleaned = cleaned.replace(".", "")
                # Otherwise standard decimal (e.g. 19.99) -> keep as is

            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        return 0.0

    @staticmethod
    def normalize_product(
        product: Dict[str, Any],
        brand_map: Optional[Dict[str, str]] = None,
        category_map: Optional[Dict[str, str]] = None,
        default_currency: Optional[str] = None
    ) -> Dict[str, Any]:
        item = dict(product)
        item["brand"] = EntityNormalizer.normalize_brand(item.get("brand", ""), custom_map=brand_map)
        item["category"] = EntityNormalizer.normalize_category(item.get("category", ""), custom_map=category_map)
        item["price"] = EntityNormalizer.normalize_price(item.get("price", 0))
        item["currency"] = item.get("currency") or default_currency or settings.analytics.default_currency
        return item

