import re
import math
import json
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

PRICE_CURRENCY = r"(?:US\$|USD|VND|VNĐ|DONG|Đ|₫|EUR|GBP|JPY|\$|€|£|¥)"
PRICE_NUMBER = r"(?:\d+|\d+[.]\d+|\d+,\d{1,2}|\d{1,3}(?:,\d{3})+(?:[.]\d+)?|\d{1,3}(?:[.]\d{3})+(?:,\d{1,2})?)"
PRICE_TEXT_PATTERN = (rf"(?i)^(?:[+-]?{PRICE_NUMBER}|[+-]?\s*{PRICE_CURRENCY}\s*{PRICE_NUMBER}|"
                      rf"{PRICE_CURRENCY}\s*[+-]?{PRICE_NUMBER}|[+-]?{PRICE_NUMBER}\s*{PRICE_CURRENCY}|"
                      r"[+-]?\d+(?:[.]\d+)?[eE][+-]?\d+)$")

class EntityNormalizer:
    """Normalizes brands, categories, currency, and formats (Section 18 in plan.md).
    Fully parameterized to handle international formats and custom business rules.
    """

    @staticmethod
    def normalization_error_codes(value: Any) -> List[str]:
        """Accept array metadata from Parquet and its JSON representation in SQL."""
        if hasattr(value, 'tolist'):
            value = value.tolist()
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return []
        if isinstance(value, str):
            if not value.strip():
                return []
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                return ['INVALID_NORMALIZATION_METADATA']
            if value is None:
                return []
        if not isinstance(value, (list, tuple)) or any(not isinstance(code, str) for code in value):
            return ['INVALID_NORMALIZATION_METADATA']
        return sorted(set(code for code in value if code))

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
            if not re.fullmatch(PRICE_TEXT_PATTERN, val):
                return 0.0
            if re.fullmatch(r"[+-]?\d+(?:\.\d+)?[eE][+-]?\d+", val):
                return float(val)
            # Strip currency symbols and letters
            negative = '-' in val
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
                if len(parts) > 2 or (len(parts) == 2 and len(parts[1]) == 3 and len(parts[0]) <= 3):
                    cleaned = cleaned.replace(".", "")
                # Otherwise standard decimal (e.g. 19.99) -> keep as is

            try:
                return -float(cleaned) if negative else float(cleaned)
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
    def normalize_currency(raw_currency: Any, default_currency: Optional[str] = None) -> Optional[str]:
        """Normalizes currency symbols/codes (e.g. 'vnđ', '₫', '$', 'usd') to ISO 4217 codes."""
        fallback = default_currency
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
    def normalize_record(record: Dict[str, Any], dataset_type: str) -> Dict[str, Any]:
        """Preserve raw evidence; invalid values remain null, with explicit errors."""
        if dataset_type not in {"products", "reviews", "prices"}:
            raise ValueError(f"Unknown dataset: {dataset_type}")
        item = dict(record)
        errors = set(EntityNormalizer.normalization_error_codes(item.get("normalization_errors")))
        numeric = "rating" if dataset_type == "reviews" else "price"
        raw = item.setdefault(f"{numeric}_raw", item.get(numeric))
        missing = raw is None or (isinstance(raw, str) and not raw.strip())
        try:
            parsed = EntityNormalizer.normalize_price(raw) if numeric == "price" else float(raw)
            valid = not isinstance(raw, bool) and math.isfinite(parsed) and (1 <= parsed <= 5 if numeric == "rating" else parsed > 0)
        except (TypeError, ValueError, OverflowError):
            parsed, valid = None, False
        item[numeric] = parsed if valid else None
        if missing or not valid:
            errors.add(f"{'MISSING' if missing else 'INVALID'}_{numeric.upper()}")

        if dataset_type in {'products', 'prices'}:
            raw_currency = item.setdefault('currency_raw', item.get('currency'))
            item['currency'] = EntityNormalizer.normalize_currency(raw_currency)

        business_time = {"products": "created_at", "reviews": "review_date", "prices": "timestamp"}[dataset_type]
        time_fields = {business_time, "ingested_at"} | (set(item) & {"created_at", "updated_at", "timestamp", "review_date"})
        for field in time_fields:
            raw = item.setdefault(f"{field}_raw", item.get(field))
            parsed = EntityNormalizer.normalize_timestamp(raw)
            item[field] = parsed
            if raw is not None and str(raw).strip() and parsed is None:
                errors.add(f"INVALID_{field.upper()}")
        item['time_available'] = item[business_time] is not None
        item['ingestion_time_available'] = item['ingested_at'] is not None
        item['normalization_errors'] = sorted(errors)
        item['normalization_status'] = 'invalid' if errors else 'valid'
        return item

    @staticmethod
    def normalize_product(
        product: Dict[str, Any],
        brand_map: Optional[Dict[str, str]] = None,
        category_map: Optional[Dict[str, str]] = None,
        default_currency: Optional[str] = None,
    ) -> Dict[str, Any]:
        item = EntityNormalizer.normalize_record(product, 'products')
        item['brand'] = EntityNormalizer.normalize_brand(item.get('brand', ''), custom_map=brand_map)
        item['category'] = EntityNormalizer.normalize_category(item.get('category', ''), custom_map=category_map)
        item['currency'] = EntityNormalizer.normalize_currency(item.get('currency'), default_currency=default_currency)
        return item
