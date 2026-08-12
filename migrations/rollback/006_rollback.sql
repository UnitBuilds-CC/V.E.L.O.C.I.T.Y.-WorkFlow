-- Rollback Migration 006: Remove Search Attribute Schema Tables

BEGIN;

DROP TABLE IF EXISTS search_attribute_indexes CASCADE;
DROP TABLE IF EXISTS custom_search_attributes CASCADE;

DELETE FROM schema_version WHERE version = 6;

COMMIT;
