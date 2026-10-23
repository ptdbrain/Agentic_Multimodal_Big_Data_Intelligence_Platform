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

    CURRENCY_ALIASES = {
        "vnd": "VND",
        "vnđ": "VND",
        "đ": "VND",
        "₫": "VND",
        "dong": "VND",
        "usd": "USD",
        "$": "USD",
        "us$": "USD",
        "dollar": "USD",
        "eur": "EUR",
        "€": "EUR",
        "euro": "EUR",
        "gbp": "GBP",
        "£": "GBP",
        "jpy": "JPY",
        "¥": "JPY",
    }

    @staticmethod
    def normalize_currency(raw_currency: Any, default_currency: Optional[str] = None) -> str:
        """Normalizes currency symbols/codes (e.g. 'vnđ', '₫', '$', 'usd') to ISO 4217 codes."""
        fallback = default_currency or settings.analytics.default_currency
        if raw_currency is None:
            return fallback
        cleaned = str(raw_currency).strip().lower()
        if not cleaned:
            return fallback
        if cleaned in EntityNormalizer.CURRENCY_ALIASES:
            return EntityNormalizer.CURRENCY_ALIASES[cleaned]
        upper = cleaned.upper()
        if len(upper) == 3 and upper.isalpha():
            return upper
        return fallback

    @staticmethod
    def normalize_timestamp(raw_ts: Any) -> Optional[str]:
        """Normalizes Unix epoch (seconds or ms), date strings, and ISO-8601 strings with offsets
        into canonical UTC ISO-8601 format ('YYYY-MM-DDTHH:MM:SSZ'). Returns None if unparseable.
        """
        import datetime

        if raw_ts is None:
            return None
        if isinstance(raw_ts, datetime.datetime):
            dt = raw_ts
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            else:
                dt = dt.astimezone(datetime.timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        if isinstance(raw_ts, datetime.date):
            return f"{raw_ts.isoformat()}T00:00:00Z"
        if isinstance(raw_ts, (int, float)) and not isinstance(raw_ts, bool):
            try:
                sec = float(raw_ts) / 1000.0 if abs(float(raw_ts)) > 1e11 else float(raw_ts)
                dt = datetime.datetime.fromtimestamp(sec, tz=datetime.timezone.utc)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, OSError, OverflowError):
                return None
        if isinstance(raw_ts, str):
            s = raw_ts.strip()
            if not s:
                return None
            # Numeric string epoch
            if re.fullmatch(r"\d+(\.\d+)?", s):
                return EntityNormalizer.normalize_timestamp(float(s))
            # Standardize slash dates YYYY/MM/DD
            s_norm = re.sub(r"^(\d{4})/(\d{2})/(\d{2})", r"\1-\2-\3", s)
            try:
                dt = datetime.datetime.fromisoformat(s_norm.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                else:
                    dt = dt.astimezone(datetime.timezone.utc)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
                    try:
                        dt = datetime.datetime.strptime(s, fmt).replace(tzinfo=datetime.timezone.utc)
                        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    except ValueError:
                        continue
                return None
        return None

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
        item["currency"] = EntityNormalizer.normalize_currency(item.get("currency"), default_currency=default_currency)
        for ts_field in ("created_at", "updated_at", "timestamp"):
            if ts_field in item and item[ts_field] is not None:
                norm_ts = EntityNormalizer.normalize_timestamp(item[ts_field])
                if norm_ts is not None:
                    item[ts_field] = norm_ts
        return item


