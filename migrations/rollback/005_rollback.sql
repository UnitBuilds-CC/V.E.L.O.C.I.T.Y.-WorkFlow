-- Rollback Migration 005: Remove Multi-Region Tables

BEGIN;

DROP TRIGGER IF EXISTS trg_regions_updated_at ON regions;

DROP TABLE IF EXISTS replication_queue CASCADE;
DROP TABLE IF EXISTS failover_events CASCADE;
DROP TABLE IF EXISTS regions CASCADE;

DELETE FROM schema_version WHERE version = 5;

COMMIT;
