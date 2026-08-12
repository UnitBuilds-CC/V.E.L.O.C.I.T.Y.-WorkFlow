-- Migration 006: Add Search Attribute Schema
-- Creates tables for custom search attribute definitions and search
-- attribute index management. Allows namespaces to define custom
-- search attributes with typed schemas and track index creation.

BEGIN;

-- ─── Custom Search Attributes ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS custom_search_attributes (
    namespace_id    BIGINT NOT NULL,
    name            TEXT NOT NULL,
    type            SMALLINT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (namespace_id, name),
    CONSTRAINT chk_custom_sa_type CHECK (type BETWEEN 0 AND 7)
);

CREATE INDEX IF NOT EXISTS idx_custom_sa_namespace ON custom_search_attributes (namespace_id);
CREATE INDEX IF NOT EXISTS idx_custom_sa_type ON custom_search_attributes (type);

-- ─── Search Attribute Indexes ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS search_attribute_indexes (
    namespace_id    BIGINT NOT NULL,
    attribute_name  TEXT NOT NULL,
    index_type      TEXT NOT NULL DEFAULT 'btree',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at    TIMESTAMPTZ,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,

    PRIMARY KEY (namespace_id, attribute_name),
    CONSTRAINT chk_sa_index_type CHECK (index_type IN ('btree', 'hash', 'gin', 'gist'))
);

CREATE INDEX IF NOT EXISTS idx_sa_indexes_namespace ON search_attribute_indexes (namespace_id);
CREATE INDEX IF NOT EXISTS idx_sa_indexes_active ON search_attribute_indexes (is_active) WHERE is_active = TRUE;

-- Record migration
INSERT INTO schema_version (version, name, applied_at)
VALUES (6, 'add_search_attribute_schema', NOW());

COMMIT;
