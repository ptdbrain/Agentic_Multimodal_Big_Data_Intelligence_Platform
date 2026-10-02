# Anomaly Events Data Dictionary

| Field Name | Type | Nullable | Description | Source | Transformation |
|---|---|---|---|---|---|
| `timestamp` | Timestamp | No | Event time | Streaming | None |
| `product_id` | String | No | Target product | Streaming | None |
| `anomaly_type` | String | No | PRICE_DROP, etc. | Streaming | Identified |
| `severity` | String | No | HIGH/MEDIUM/LOW | Streaming | Computed |
| `details` | String | Yes | JSON context | Streaming | Constructed |
