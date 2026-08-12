-- Migration 003: Add Audit Tables
-- Creates tables for audit logging, API key management, and namespace
-- configuration storage. These support compliance, authentication, and
-- multi-tenant configuration features.

BEGIN;

-- ─── Audit Logs ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS audit_logs (
    id              BIGSERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor           TEXT NOT NULL DEFAULT '',
    action          TEXT NOT NULL,
    resource        TEXT NOT NULL DEFAULT '',
    result          TEXT NOT NULL DEFAULT 'success',
    ip_address      INET,
    user_agent      TEXT NOT NULL DEFAULT '',
    details         JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_timestamp ON audit_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource ON audit_logs (resource);
CREATE INDEX IF NOT EXISTS idx_audit_logs_actor ON audit_logs (actor);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs (action);

-- ─── API Keys ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS api_keys (
    id              BIGSERIAL PRIMARY KEY,
    key_hash        TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    namespace       TEXT NOT NULL DEFAULT 'default',
    permissions     JSONB NOT NULL DEFAULT '[]'::JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    last_used_at    TIMESTAMPTZ,
    created_by      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_api_keys_namespace ON api_keys (namespace);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_api_keys_expires ON api_keys (expires_at) WHERE expires_at IS NOT NULL;

-- ─── Namespace Configs ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS namespace_configs (
    name            TEXT PRIMARY KEY,
    owner_email     TEXT NOT NULL DEFAULT '',
    description     TEXT NOT NULL DEFAULT '',
    data_json       JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Trigger for namespace_configs updated_at
CREATE TRIGGER trg_namespace_configs_updated_at
    BEFORE UPDATE ON namespace_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Record migration
INSERT INTO schema_version (version, name, applied_at)
VALUES (3, 'add_audit_tables', NOW());

COMMIT;
