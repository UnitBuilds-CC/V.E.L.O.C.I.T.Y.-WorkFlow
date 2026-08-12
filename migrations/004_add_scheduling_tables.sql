-- Migration 004: Add Scheduling Tables
-- Creates tables for workflow schedules (cron-like recurring execution)
-- and their execution history. Supports jitter, retry policies, and
-- pause/resume functionality.

BEGIN;

-- ─── Schedules ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS schedules (
    id                  BIGSERIAL PRIMARY KEY,
    namespace_id        BIGINT NOT NULL DEFAULT 0,
    workflow_type_id    BIGINT NOT NULL,
    cron_expression     TEXT NOT NULL,
    start_at            TIMESTAMPTZ,
    end_at              TIMESTAMPTZ,
    jitter_ms           BIGINT NOT NULL DEFAULT 0,
    retry_policy_json   JSONB NOT NULL DEFAULT '{}'::JSONB,
    memo                BYTEA,
    paused              BOOLEAN NOT NULL DEFAULT FALSE,
    last_fire_at        TIMESTAMPTZ,
    next_fire_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_schedules_jitter CHECK (jitter_ms >= 0)
);

CREATE INDEX IF NOT EXISTS idx_schedules_next_fire ON schedules (next_fire_at) WHERE paused = FALSE;
CREATE INDEX IF NOT EXISTS idx_schedules_namespace ON schedules (namespace_id);
CREATE INDEX IF NOT EXISTS idx_schedules_workflow_type ON schedules (workflow_type_id);

-- ─── Schedule Executions ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS schedule_executions (
    id              BIGSERIAL PRIMARY KEY,
    schedule_id     BIGINT NOT NULL,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          SMALLINT NOT NULL DEFAULT 0,
    workflow_key    BIGINT,
    error_message   TEXT,

    CONSTRAINT chk_schedule_exec_status CHECK (status BETWEEN 0 AND 4),
    CONSTRAINT fk_schedule_exec_schedule FOREIGN KEY (schedule_id) REFERENCES schedules(id) ON DELETE CASCADE,
    CONSTRAINT fk_schedule_exec_workflow FOREIGN KEY (workflow_key) REFERENCES workflows(workflow_key) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_schedule_executions_schedule ON schedule_executions (schedule_id);
CREATE INDEX IF NOT EXISTS idx_schedule_executions_fired_at ON schedule_executions (fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_executions_status ON schedule_executions (status);

-- Trigger for schedules updated_at
CREATE TRIGGER trg_schedules_updated_at
    BEFORE UPDATE ON schedules
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Record migration
INSERT INTO schema_version (version, name, applied_at)
VALUES (4, 'add_scheduling_tables', NOW());

COMMIT;
