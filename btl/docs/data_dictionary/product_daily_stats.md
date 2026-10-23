# Product Daily Stats Data Dictionary

| Field Name | Type | Nullable | Description | Source | Transformation |
|---|---|---|---|---|---|
| `date` | Date | No | Metric date | Reviews/Prices | Truncated to day |
| `product_id` | String | No | FK to Product | Reviews/Prices | Group key |
| `review_count` | Long | No | Count of reviews | Reviews | Aggregated |
| `avg_rating` | Double | Yes | Avg rating | Reviews | Aggregated |
| `rating_std` | Double | Yes | Stddev of rating | Reviews | Aggregated |
| `avg_price` | Double | Yes | Avg price | Prices | Aggregated |
| `min_price` | Double | Yes | Min price | Prices | Aggregated |
| `max_price` | Double | Yes | Max price | Prices | Aggregated |
| `high_rating_ratio` | Double | Yes | % >= 4.0 | Reviews | Computed |
| `low_rating_ratio` | Double | Yes | % <= 2.0 | Reviews | Computed |
