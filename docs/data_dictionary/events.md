# Events Data Dictionary

| Field Name | Type | Nullable | Description | Source | Transformation |
|---|---|---|---|---|---|
| `event_id` | String | No | Unique event ID | Raw data | None |
| `event_type` | String | No | Type of event (click, etc) | Raw data | None |
| `user_id` | String | Yes | User identifier | Raw data | None |
| `product_id` | String | Yes | Product interacted with | Raw data | None |
| `timestamp` | Timestamp | No | Event timestamp | Raw data | None |
