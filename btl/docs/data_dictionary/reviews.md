# Reviews Data Dictionary

| Field Name | Type | Nullable | Description | Source | Transformation |
|---|---|---|---|---|---|
| `review_id` | String | No | Unique review ID | Raw data | None |
| `product_id` | String | No | FK to Products | Raw data | None |
| `rating` | Float | No | User rating (1-5) | Raw data | Validation range [1,5] |
| `review_text` | String | No | Text content | Raw data | Trimmed |
| `review_title` | String | Yes | Title of review | Raw data | Trimmed |
| `timestamp` | Timestamp | No | Original timestamp | Raw data | Parsed |
| `ingested_at` | Timestamp | No | Ingestion time | Pipeline | Auto-generated |
