-- Rollback Migration 003: Remove Audit Tables

BEGIN;

DROP TRIGGER IF EXISTS trg_namespace_configs_updated_at ON namespace_configs;

DROP TABLE IF EXISTS audit_logs CASCADE;
DROP TABLE IF EXISTS api_keys CASCADE;
DROP TABLE IF EXISTS namespace_configs CASCADE;

DELETE FROM schema_version WHERE version = 3;

COMMIT;
