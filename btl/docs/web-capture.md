# Durable web capture

`ingestion.web_capture.capture_web(root, seed_urls, limit=50, max_pages=150, request_interval=1)` separates network capture from SQL publication. Use a private persistent root and retain `attempts/` with `state.json` in coordinated backups.

Each attempted page writes its collected records, HTTP evidence and immutable checksummed manifest before the URL state advances. The next call prioritizes pending URLs, then revisits seeds and known pages using fresh collection clocks. Content changes are compared by observed fingerprint; unchanged articles keep their original publication clocks. Every attempt ID is new, and reusing one is refused.

Results distinguish `complete`, `partial`, `failed` and `blocked`. Pending work, exhausted failures or reaching the request/article bounds are reported explicitly. Failed/blocked URLs receive at most three consecutive attempts across calls; success resets that budget. Exhausted URLs remain in state for operator review rather than disappearing. Back up state before editing a retry budget, and retain prior attempts.

`records_path`, `manifest_path`, their SHA-256 values, page references and counts are returned to the managed source handoff. Publishing articles is a separate operation. Capturing a page successfully does not prove Kafka delivery, ETL acceptance or SQL publication.

Unit tests use controlled HTTP responses. They prove cursor/evidence behavior, not availability or successful collection from the public site.
