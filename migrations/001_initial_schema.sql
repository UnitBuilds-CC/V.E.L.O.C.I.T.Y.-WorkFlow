-- Migration 001: Initial Schema
-- VELOCITY-WorkFlow PostgreSQL Persistence Schema
-- Version: 1.0
--
-- Creates the core tables for workflow state, events, search attributes,
-- namespace metadata, and checkpoints. Also establishes the schema_version
-- tracking table used by the migration runner.

BEGIN;

-- ─── Schema Version Tracking ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INTEGER NOT NULL DEFAULT 0
);

-- ─── Namespaces ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS namespaces (
    name            TEXT PRIMARY KEY,
    display_name    TEXT NOT NULL DEFAULT '',
    description     TEXT NOT NULL DEFAULT '',
    retention_days  INTEGER NOT NULL DEFAULT 7,
    is_global       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_namespaces_created_at ON namespaces (created_at);

-- ─── Workflows ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS workflows (
    workflow_key        BIGINT PRIMARY KEY,
    workflow_id         BIGINT NOT NULL,
    run_id              BIGINT NOT NULL,
    workflow_type_id    BIGINT NOT NULL DEFAULT 0,
    namespace_id        BIGINT NOT NULL DEFAULT 0,
    namespace_name      TEXT NOT NULL DEFAULT 'default',
    task_queue_hash     BIGINT NOT NULL DEFAULT 0,

    -- Slab header fields
    current_step        INTEGER NOT NULL DEFAULT 0,
    total_steps         INTEGER NOT NULL DEFAULT 0,
    merkle_root         BYTEA NOT NULL DEFAULT '\x'::BYTEA,
    step_bitmask        BYTEA NOT NULL DEFAULT '\x'::BYTEA,

    -- Execution state
    status              SMALLINT NOT NULL DEFAULT 1,
    step_results        JSONB NOT NULL DEFAULT '{}'::JSONB,
    signal_buffer       JSONB NOT NULL DEFAULT '{}'::JSONB,
    update_buffer       JSONB NOT NULL DEFAULT '{}'::JSONB,
    input_data          BYTEA,
    result_data         BYTEA,

    -- Hierarchy
    parent_key          BIGINT,
    child_keys          BIGINT[] NOT NULL DEFAULT '{}',

    -- Timing
    start_time          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    close_time          TIMESTAMPTZ,
    execution_timeout_ms BIGINT,
    run_timeout_ms      BIGINT,
    task_timeout_ms     BIGINT,

    -- Sequence tracking
    event_sequence      BIGINT NOT NULL DEFAULT 0,

    -- Schema metadata
    schema_version      INTEGER NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_workflows_namespace FOREIGN KEY (namespace_name) REFERENCES namespaces(name) ON DELETE SET DEFAULT,
    CONSTRAINT fk_workflows_parent FOREIGN KEY (parent_key) REFERENCES workflows(workflow_key) ON DELETE SET NULL,
    CONSTRAINT chk_workflow_status CHECK (status BETWEEN 0 AND 7),
    CONSTRAINT chk_workflow_steps CHECK (current_step >= 0 AND total_steps >= 0)
);

CREATE INDEX IF NOT EXISTS idx_workflows_workflow_id ON workflows (workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflows_run_id ON workflows (run_id);
CREATE INDEX IF NOT EXISTS idx_workflows_namespace ON workflows (namespace_name);
CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows (status);
CREATE INDEX IF NOT EXISTS idx_workflows_task_queue ON workflows (task_queue_hash);
CREATE INDEX IF NOT EXISTS idx_workflows_parent ON workflows (parent_key);
CREATE INDEX IF NOT EXISTS idx_workflows_created_at ON workflows (created_at);
CREATE INDEX IF NOT EXISTS idx_workflows_updated_at ON workflows (updated_at);
CREATE INDEX IF NOT EXISTS idx_workflows_ns_status ON workflows (namespace_name, status);

-- ─── Workflow Events ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS workflow_events (
    id              BIGSERIAL PRIMARY KEY,
    workflow_key    BIGINT NOT NULL,
    event_type      SMALLINT NOT NULL,
    event_type_name TEXT NOT NULL DEFAULT '',
    sequence_num    BIGINT NOT NULL DEFAULT 0,
    data            BYTEA,
    metadata        JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_events_workflow FOREIGN KEY (workflow_key) REFERENCES workflows(workflow_key) ON DELETE CASCADE,
    CONSTRAINT chk_event_type CHECK (event_type BETWEEN 1 AND 20)
);

CREATE INDEX IF NOT EXISTS idx_events_workflow_key ON workflow_events (workflow_key);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON workflow_events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON workflow_events (created_at);
CREATE INDEX IF NOT EXISTS idx_events_workflow_seq ON workflow_events (workflow_key, sequence_num);

-- ─── Search Attributes ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS search_attributes (
    workflow_key    BIGINT NOT NULL,
    attr_name       TEXT NOT NULL,
    attr_type       SMALLINT NOT NULL DEFAULT 0,
    string_value    TEXT,
    int_value       BIGINT,
    float_value     DOUBLE PRECISION,
    bool_value      BOOLEAN,
    datetime_value  TIMESTAMPTZ,
    bytes_value     BYTEA,
    string_array    TEXT[],
    int_array       BIGINT[],
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (workflow_key, attr_name),
    CONSTRAINT fk_search_attrs_workflow FOREIGN KEY (workflow_key) REFERENCES workflows(workflow_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_search_attrs_name ON search_attributes (attr_name);
CREATE INDEX IF NOT EXISTS idx_search_attrs_string ON search_attributes (attr_name, string_value);
CREATE INDEX IF NOT EXISTS idx_search_attrs_int ON search_attributes (attr_name, int_value);
CREATE INDEX IF NOT EXISTS idx_search_attrs_datetime ON search_attributes (attr_name, datetime_value);

-- ─── Workflow Checkpoints (for fast recovery) ────────────────────────────────

CREATE TABLE IF NOT EXISTS workflow_checkpoints (
    workflow_key    BIGINT PRIMARY KEY,
    snapshot_data   BYTEA NOT NULL,
    merkle_root     BYTEA NOT NULL DEFAULT '\x'::BYTEA,
    step_count      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_checkpoint_workflow FOREIGN KEY (workflow_key) REFERENCES workflows(workflow_key) ON DELETE CASCADE
);

-- ─── Functions ───────────────────────────────────────────────────────────────

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_workflows_updated_at
    BEFORE UPDATE ON workflows
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_namespaces_updated_at
    BEFORE UPDATE ON namespaces
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_search_attrs_updated_at
    BEFORE UPDATE ON search_attributes
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ─── Record Migration ───────────────────────────────────────────────────────

INSERT INTO schema_version (version, name, applied_at)
VALUES (1, 'initial_schema', NOW());

COMMIT;
