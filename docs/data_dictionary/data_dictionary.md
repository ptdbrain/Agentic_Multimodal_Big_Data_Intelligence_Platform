# SentinelAI Data Dictionary

## 1. Product Entity (`raw.products`)
| Field | Type | Required | Description | Example |
|---|---|---|---|---|
| `product_id` | String | Yes | Unique product SKU/identifier | `iphone_15_pro_256` |
| `product_name` | String | Yes | Full product display name | `iPhone 15 Pro 256GB Natural Titanium` |
| `brand` | String | Yes | Manufacturer / Brand | `Apple` |
| `category` | String | Yes | Primary product category | `Smartphone` |
| `subcategory` | String | No | Secondary subcategory | `Flagship Phone` |
| `price` | Float | Yes | Current price in local currency | `28990000.0` |
| `currency` | String | Yes | ISO Currency code (`VND`, `USD`) | `VND` |
| `rating` | Float | No | Aggregated catalog rating (0.0 - 5.0) | `4.7` |
| `review_count`| Integer| No | Total review count | `1420` |
| `seller` | String | No | Retail partner / store | `Apple Flagship Store` |
| `product_url` | String | No | Store product URL | `https://store.apple.com/...` |
| `image_url` | String | No | Product hero image URL | `https://cdn.store.com/ip15.jpg` |
| `source` | String | Yes | Data source origin | `tiki_crawler` |
| `created_at` | String | No | Timestamp of creation (ISO-8601) | `2026-08-01T10:00:00Z` |
| `updated_at` | String | No | Timestamp of last update | `2026-09-24T08:30:00Z` |

## 2. Review Entity (`raw.reviews`)
| Field | Type | Required | Description | Example |
|---|---|---|---|---|
| `review_id` | String | Yes | Unique review ID | `rv_2026_09_0012` |
| `product_id` | String | Yes | Associated product ID | `iphone_15_pro_256` |
| `user_id` | String | Yes | Hashed user identifier | `usr_88329` |
| `rating` | Float | Yes | Star rating (1.0 to 5.0) | `5.0` |
| `review_title`| String | No | Headline of customer review | `Hiệu năng xuất sắc, pin trâu` |
| `review_text` | String | Yes | Full customer text feedback | `Máy cầm rất nhẹ, khung titan sang trọng...` |
| `review_date` | String | Yes | Review timestamp / date | `2026-09-24T12:00:00Z` |
| `verified_purchase` | Boolean | No | Whether buyer was verified | `true` |
| `helpful_count` | Integer | No | Helpful votes by community | `18` |
| `source` | String | No | Ingestion source origin | `ecommerce_api` |
| `language` | String | No | Detected language code (`vi`, `en`)| `vi` |
