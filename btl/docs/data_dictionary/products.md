# Products Data Dictionary

| Field Name | Type | Nullable | Description | Source | Transformation |
|---|---|---|---|---|---|
| `product_id` | String | No | Unique identifier | Raw data | None |
| `product_name` | String | No | Name of product | Raw data | Trimmed |
| `brand` | String | No | Manufacturer / Brand | Raw data | Uppercased |
| `category` | String | No | Product category | Raw data | None |
| `price` | Float | No | Current retail price | Raw data | Clamped > 0 |
| `ingested_at` | Timestamp | No | Ingestion timestamp | Pipeline | Auto-generated |
