-- Migration 005: Add Multi-Region Tables
-- Creates tables for multi-region deployment support: region registry,
-- replication queue for cross-region event propagation, and failover
-- event tracking for disaster recovery.

BEGIN;

-- ─── Regions ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS regions (
    id                  SERIAL PRIMARY KEY,
    region_name         TEXT NOT NULL UNIQUE,
    endpoint            TEXT NOT NULL,
    priority            INTEGER NOT NULL DEFAULT 0,
    state               SMALLINT NOT NULL DEFAULT 0,
    replication_lag_ms  BIGINT NOT NULL DEFAULT 0,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_regions_state CHECK (state BETWEEN 0 AND 3),
    CONSTRAINT chk_regions_priority CHECK (priority >= 0)
);

CREATE INDEX IF NOT EXISTS idx_regions_active ON regions (is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_regions_priority ON regions (priority) WHERE is_active = TRUE;

-- ─── Replication Queue ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS replication_queue (
    id              BIGSERIAL PRIMARY KEY,
    source_region   TEXT NOT NULL,
    target_region   TEXT NOT NULL,
    workflow_key    BIGINT NOT NULL,
    event_type      SMALLINT NOT NULL,
    payload         BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          SMALLINT NOT NULL DEFAULT 0,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    completed_at    TIMESTAMPTZ,

    CONSTRAINT chk_repl_queue_status CHECK (status BETWEEN 0 AND 3),
    CONSTRAINT chk_repl_queue_retry CHECK (retry_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_repl_queue_status_created ON replication_queue (status, created_at);
CREATE INDEX IF NOT EXISTS idx_repl_queue_target ON replication_queue (target_region) WHERE status = 0;
CREATE INDEX IF NOT EXISTS idx_repl_queue_workflow ON replication_queue (workflow_key);

-- ─── Failover Events ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS failover_events (
    id              BIGSERIAL PRIMARY KEY,
    from_region     TEXT NOT NULL,
    to_region       TEXT NOT NULL,
    initiated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          SMALLINT NOT NULL DEFAULT 0,
    reason          TEXT NOT NULL DEFAULT '',
    details         JSONB NOT NULL DEFAULT '{}'::JSONB,

    CONSTRAINT chk_failover_status CHECK (status BETWEEN 0 AND 3)
);

CREATE INDEX IF NOT EXISTS idx_failover_from_region ON failover_events (from_region);
CREATE INDEX IF NOT EXISTS idx_failover_to_region ON failover_events (to_region);
CREATE INDEX IF NOT EXISTS idx_failover_status ON failover_events (status);
CREATE INDEX IF NOT EXISTS idx_failover_initiated ON failover_events (initiated_at DESC);

-- Trigger for regions updated_at
CREATE TRIGGER trg_regions_updated_at
    BEFORE UPDATE ON regions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Record migration
INSERT INTO schema_version (version, name, applied_at)
VALUES (5, 'add_multi_region_tables', NOW());

COMMIT;
