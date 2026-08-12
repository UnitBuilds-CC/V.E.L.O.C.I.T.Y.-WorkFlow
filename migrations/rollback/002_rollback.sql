-- Rollback Migration 002: Remove Workflow Metadata Columns

BEGIN;

ALTER TABLE workflows DROP COLUMN IF EXISTS memo;
ALTER TABLE workflows DROP COLUMN IF EXISTS retry_policy_json;
ALTER TABLE workflows DROP COLUMN IF EXISTS parent_close_policy;
ALTER TABLE workflows DROP COLUMN IF EXISTS cron_schedule;
ALTER TABLE workflows DROP COLUMN IF EXISTS execution_timeout_ms;
ALTER TABLE workflows DROP COLUMN IF EXISTS run_timeout_ms;
ALTER TABLE workflows DROP COLUMN IF EXISTS task_timeout_ms;

DELETE FROM schema_version WHERE version = 2;

COMMIT;
