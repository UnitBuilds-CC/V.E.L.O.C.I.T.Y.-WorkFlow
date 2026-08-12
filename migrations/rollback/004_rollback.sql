-- Rollback Migration 004: Remove Scheduling Tables

BEGIN;

DROP TRIGGER IF EXISTS trg_schedules_updated_at ON schedules;

DROP TABLE IF EXISTS schedule_executions CASCADE;
DROP TABLE IF EXISTS schedules CASCADE;

DELETE FROM schema_version WHERE version = 4;

COMMIT;
