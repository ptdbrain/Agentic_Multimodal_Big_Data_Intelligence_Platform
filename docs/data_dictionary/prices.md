# Prices Data Dictionary

| Field Name | Type | Nullable | Description | Source | Transformation |
|---|---|---|---|---|---|
| `price_id` | String | No | Unique price ID | Raw data | None |
| `product_id` | String | No | FK to Products | Raw data | None |
| `price` | Float | No | Price observation | Raw data | Cleaned/Converted |
| `currency` | String | No | Currency code | Raw data | Default 'USD' |
| `seller` | String | Yes | Seller name | Raw data | None |
| `timestamp` | Timestamp | No | Observation time | Raw data | None |
| `source` | String | Yes | Source system | Raw data | None |
