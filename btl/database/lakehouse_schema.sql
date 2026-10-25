-- Snapshot catalog; distinct from the Delta protocol. Managed by MetadataStore.
CREATE TABLE IF NOT EXISTS lakehouse_commits (
    table_key TEXT NOT NULL,
    version INTEGER NOT NULL,
    operation_id TEXT NOT NULL,
    commit_json TEXT NOT NULL,
    PRIMARY KEY (table_key, version),
    UNIQUE (table_key, operation_id)
);
