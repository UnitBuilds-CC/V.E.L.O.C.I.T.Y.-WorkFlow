-- Rollback Migration 001: Remove Initial Schema
-- WARNING: This drops all core tables. Data will be lost.

BEGIN;

DROP TRIGGER IF EXISTS trg_search_attrs_updated_at ON search_attributes;
DROP TRIGGER IF EXISTS trg_namespaces_updated_at ON namespaces;
DROP TRIGGER IF EXISTS trg_workflows_updated_at ON workflows;

DROP FUNCTION IF EXISTS update_updated_at_column();

DROP TABLE IF EXISTS workflow_checkpoints CASCADE;
DROP TABLE IF EXISTS search_attributes CASCADE;
DROP TABLE IF EXISTS workflow_events CASCADE;
DROP TABLE IF EXISTS workflows CASCADE;
DROP TABLE IF EXISTS namespaces CASCADE;
DROP TABLE IF EXISTS schema_version CASCADE;

COMMIT;
