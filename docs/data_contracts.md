# SentinelAI Data Contracts Specification

This document defines the formal data contracts, message envelopes, and JSON schema definitions enforced across the SentinelAI platform.

---

## 1. Canonical Message Envelope (Kafka Stream Layer)

All messages flowing through Kafka topics (`raw.products`, `raw.reviews`, `raw.prices`, `raw.events`) MUST be wrapped in the canonical metadata envelope:

```json
{
  "event_id": "evt_4a7c819b3d11e82a9f24c321",
  "event_type": "NEW_REVIEW",
  "schema_version": "1.0",
  "source": "tiki",
  "event_time": "2026-10-01T10:00:00Z",
  "ingested_at": "2026-10-01T10:00:02Z",
  "payload": {
    "review_id": "rv_001",
    "product_id": "prod_100",
    "rating": 5.0,
    "review_text": "Great device"
  }
}
```

### Envelope Field Specifications:
| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `event_id` | String | Yes | Deterministic SHA256 identifier: `evt_` + `sha256(source\|entity_id\|event_time)[:24]` |
| `event_type` | String | Yes | Enumerated event classification (`NEW_PRODUCT`, `NEW_REVIEW`, `PRICE_UPDATE`, `EVENT`) |
| `schema_version` | String | Yes | Semantic schema contract version (e.g. `"1.0"`) |
| `source` | String | Yes | Originating data collector identifier (`tiki`, `web_crawler`, `dataset_replay`) |
| `event_time` | String | Yes | ISO 8601 UTC timestamp of original event creation |
| `ingested_at` | String | Yes | ISO 8601 UTC timestamp of ingestion processing |
| `payload` | Object | Yes | Underlying domain entity adhering to the entity schema contract |

---

## 2. Domain Entity Contracts

### 2.1 Product Contract (`raw.products` / `silver.products`)
- **Primary Key:** `product_id`
- **Fields:**
  - `product_id` (String, required): Unique product identifier.
  - `product_name` (String, required): Human-readable product name.
  - `brand` (String, required): Canonical normalized brand name.
  - `category` (String, required): Product category.
  - `subcategory` (String, optional): Granular category classification.
  - `price` (Number, required): Listed price in Vietnamese Dong (must be $> 0$).
  - `currency` (String, required): ISO currency code (default: `"VND"`).
  - `rating` (Number, optional): Aggregate star rating $[1.0, 5.0]$.
  - `review_count` (Integer, optional): Total count of customer reviews.
  - `seller` (String, optional): Merchant or vendor name.
  - `source` (String, required): Origin platform.

### 2.2 Review Contract (`raw.reviews` / `silver.reviews`)
- **Primary Key:** `review_id`
- **Fields:**
  - `review_id` (String, required): Unique customer review identifier.
  - `product_id` (String, required): Foreign key reference to product.
  - `rating` (Number, required): Customer star rating $[1.0, 5.0]$.
  - `review_text` (String, required): Textual review content.
  - `review_title` (String, optional): Review headline.
  - `review_date` (String, optional): ISO 8601 creation date.
  - `verified_purchase` (Boolean, optional): Whether buyer verified.
  - `helpful_count` (Integer, optional): Upvotes count.
  - `source` (String, required): Origin platform.

### 2.3 Price History Contract (`raw.prices` / `silver.prices`)
- **Primary Key:** `price_id`
- **Fields:**
  - `price_id` (String, required): Unique price observation identifier.
  - `product_id` (String, required): Target product identifier.
  - `price` (Number, required): Numeric price in standard currency ($> 0$).
  - `currency` (String, required): ISO currency code.
  - `seller` (String, optional): Merchant name.
  - `timestamp` (String, required): Observation timestamp.
  - `source` (String, required): Poller or crawler identifier.

---

## 3. Dead-Letter Queue (DLQ) Contract (`dead-letter`)

When a message fails validation or schema parsing, it is redirected to the DLQ:

```json
{
  "error_type": "SCHEMA_VALIDATION_ERROR",
  "source_topic": "raw.reviews",
  "partition": 1,
  "offset": 4502,
  "payload": { "corrupted": true },
  "errors": ["Missing required field: 'review_text'"],
  "retry_count": 1,
  "timestamp": "2026-10-01T10:00:03Z"
}
```
