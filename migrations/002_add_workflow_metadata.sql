-- Migration 002: Add Workflow Metadata Columns
-- Adds extended metadata fields to the workflows table for richer workflow
-- configuration: memo storage, retry policies, parent close behavior,
-- cron schedules, and granular timeout controls.

BEGIN;

-- Memo: arbitrary user-defined binary blob attached to a workflow
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS memo BYTEA;

-- Retry policy stored as JSONB for flexible schema evolution
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS retry_policy_json JSONB;

-- Parent close policy: controls what happens to children when parent closes
-- 0 = TERMINATE, 1 = ABANDON, 2 = REQUEST_CANCEL
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS parent_close_policy SMALLINT NOT NULL DEFAULT 1;

-- Cron schedule expression (e.g. "0 * * * *")
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS cron_schedule TEXT;

-- Granular timeout controls (override defaults at the workflow level)
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS execution_timeout_ms BIGINT;
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS run_timeout_ms BIGINT;
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS task_timeout_ms BIGINT;

-- Index for cron-scheduled workflow lookups
CREATE INDEX IF NOT EXISTS idx_workflows_cron_schedule ON workflows (cron_schedule) WHERE cron_schedule IS NOT NULL;

-- Index for retry policy queries
CREATE INDEX IF NOT EXISTS idx_workflows_retry_policy ON workflows ((retry_policy_json IS NOT NULL)) WHERE retry_policy_json IS NOT NULL;

-- Record migration
INSERT INTO schema_version (version, name, applied_at)
VALUES (2, 'add_workflow_metadata', NOW());

COMMIT;
