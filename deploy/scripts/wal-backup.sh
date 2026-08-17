#!/usr/bin/env bash
# WAL Backup Script — Velocity Workflow Engine
# Backs up Write-Ahead Log files with encryption, verification, and retention.
#
# Usage:
#   ./deploy/scripts/wal-backup.sh [OPTIONS]
#
# Options:
#   --wal-dir <path>       WAL directory (default: /data/velocity/wal)
#   --backup-dir <path>    Backup destination (default: /backups/velocity/wal)
#   --encrypt              Encrypt backup with AES-256-GPG (requires GPG_KEY)
#   --gpg-key <id>         GPG key ID for encryption
#   --retention <days>     Retain backups for N days (default: 30)
#   --verify               Verify backup integrity after creation
#   --s3-bucket <url>      Upload to S3-compatible storage (requires aws cli)
#   --slab-dir <path>      Slab directory to include (default: /data/velocity/slabs)
#   --include-slabs        Include slab files in backup
#   --quiet                Suppress non-error output
#
# Environment:
#   VELOCITY_WAL_DIR       Override default WAL directory
#   VELOCITY_BACKUP_DIR    Override default backup destination
#   VELOCITY_GPG_KEY       GPG key ID for encryption
#   VELOCITY_S3_BUCKET     S3 bucket for remote backup
#   PGHOST, PGUSER, etc.   PostgreSQL connection (for pg_dump)
#
# Cron Example (hourly backup):
#   0 * * * * root /opt/velocity/scripts/wal-backup.sh --encrypt --verify --retention 30

set -euo pipefail

# ─── Defaults ───────────────────────────────────────────────────────────────
WAL_DIR="${VELOCITY_WAL_DIR:-/data/velocity/wal}"
SLAB_DIR="${VELOCITY_SLAB_DIR:-/data/velocity/slabs}"
BACKUP_DIR="${VELOCITY_BACKUP_DIR:-/backups/velocity/wal}"
ENCRYPT=false
GPG_KEY="${VELOCITY_GPG_KEY:-}"
RETENTION=30
VERIFY=false
S3_BUCKET="${VELOCITY_S3_BUCKET:-}"
INCLUDE_SLABS=false
QUIET=false

# ─── Parse Arguments ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --wal-dir)        WAL_DIR="$2"; shift 2 ;;
    --slab-dir)       SLAB_DIR="$2"; shift 2 ;;
    --backup-dir)     BACKUP_DIR="$2"; shift 2 ;;
    --encrypt)        ENCRYPT=true; shift ;;
    --gpg-key)        GPG_KEY="$2"; shift 2 ;;
    --retention)      RETENTION="$2"; shift 2 ;;
    --verify)         VERIFY=true; shift ;;
    --s3-bucket)      S3_BUCKET="$2"; shift 2 ;;
    --include-slabs)  INCLUDE_SLABS=true; shift ;;
    --quiet)          QUIET=true; shift ;;
    -h|--help)
      head -24 "$0" | tail -18
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ─── Logging ────────────────────────────────────────────────────────────────
log_info()  { [[ "$QUIET" = true ]] || echo "[INFO]  $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }
log_warn()  { echo "[WARN]  $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*" >&2; }
log_error() { echo "[ERROR] $(date -u '+%Y-%m-%dT%H:%M:%SZ') $*" >&2; }

# ─── Pre-flight ─────────────────────────────────────────────────────────────
preflight() {
  if [[ ! -d "$WAL_DIR" ]]; then
    log_error "WAL directory does not exist: $WAL_DIR"
    exit 1
  fi

  if [[ "$ENCRYPT" = true && -z "$GPG_KEY" ]]; then
    log_error "Encryption requested but no --gpg-key or VELOCITY_GPG_KEY set"
    exit 1
  fi

  mkdir -p "$BACKUP_DIR"
}

# ─── WAL Snapshot ───────────────────────────────────────────────────────────
# Creates a consistent WAL snapshot using copy-on-write semantics.
# The Velocity WAL is append-only, so a simple copy is crash-consistent.
backup_wal() {
  local timestamp
  timestamp=$(date +%Y%m%d-%H%M%S)
  local snapshot_name="wal-snapshot-${timestamp}"
  local snapshot_dir="${BACKUP_DIR}/${snapshot_name}"

  log_info "Starting WAL backup: ${snapshot_name}"

  # Copy WAL files (append-only, safe to copy while server is running)
  mkdir -p "$snapshot_dir"
  local wal_count=0
  for wal_file in "$WAL_DIR"/*.wal; do
    if [[ -f "$wal_file" ]]; then
      cp -a "$wal_file" "$snapshot_dir/"
      wal_count=$((wal_count + 1))
    fi
  done

  # Also copy WAL metadata if present
  for meta_file in "$WAL_DIR"/*.meta "$WAL_DIR"/*.offset; do
    if [[ -f "$meta_file" ]]; then
      cp -a "$meta_file" "$snapshot_dir/"
    fi
  done

  if [[ $wal_count -eq 0 ]]; then
    log_warn "No WAL files found in $WAL_DIR — creating empty marker"
    echo "no-wal-files" > "$snapshot_dir/EMPTY_MARKER"
  fi

  log_info "Copied $wal_count WAL file(s) to $snapshot_dir"

  # Include slab files if requested
  if [[ "$INCLUDE_SLABS" = true && -d "$SLAB_DIR" ]]; then
    log_info "Including slab files from $SLAB_DIR"
    mkdir -p "$snapshot_dir/slabs"
    cp -a "$SLAB_DIR"/* "$snapshot_dir/slabs/" 2>/dev/null || true
    local slab_count
    slab_count=$(find "$snapshot_dir/slabs" -type f | wc -l)
    log_info "Copied $slab_count slab file(s)"
  fi

  # Record metadata
  cat > "$snapshot_dir/BACKUP_METADATA" <<EOF
timestamp=${timestamp}
wal_files=${wal_count}
wal_dir=${WAL_DIR}
encrypted=${ENCRYPT}
hostname=$(hostname)
pid=$$
EOF

  # Compute checksums for verification
  (cd "$snapshot_dir" && find . -type f ! -name "CHECKSUMS" -exec sha256sum {} \; > CHECKSUMS)
  log_info "Checksums computed: $(wc -l < "$snapshot_dir/CHECKSUMS") file(s)"

  echo "$snapshot_dir"
}

# ─── Encrypt Backup ────────────────────────────────────────────────────────
encrypt_backup() {
  local snapshot_dir="$1"
  local archive="${snapshot_dir}.tar.gz"
  local encrypted="${archive}.gpg"

  log_info "Creating encrypted archive: ${encrypted}"

  # Create tarball
  tar czf "$archive" -C "$(dirname "$snapshot_dir")" "$(basename "$snapshot_dir")"

  # Encrypt with GPG (AES-256)
  gpg --batch --yes --trust-model always \
    --recipient "$GPG_KEY" \
    --cipher-algo AES256 \
    --output "$encrypted" \
    --encrypt "$archive"

  # Remove unencrypted archive
  rm -f "$archive"

  log_info "Encrypted archive created: $encrypted ($(du -sh "$encrypted" | cut -f1))"

  # Remove the plaintext snapshot directory
  rm -rf "$snapshot_dir"

  echo "$encrypted"
}

# ─── Verify Backup ──────────────────────────────────────────────────────────
verify_backup() {
  local target="$1"

  log_info "Verifying backup integrity..."

  if [[ -f "$target" && "$target" == *.gpg ]]; then
    # For encrypted backups, verify the archive can be decrypted
    local temp_dir
    temp_dir=$(mktemp -d)
    if gpg --batch --quiet --output "$temp_dir/archive.tar.gz" --decrypt "$target" 2>/dev/null; then
      if tar tzf "$temp_dir/archive.tar.gz" >/dev/null 2>&1; then
        log_info "Encrypted backup verification: PASSED"
      else
        log_error "Encrypted archive is corrupted"
        rm -rf "$temp_dir"
        return 1
      fi
    else
      log_error "Failed to decrypt backup (wrong key?)"
      rm -rf "$temp_dir"
      return 1
    fi
    rm -rf "$temp_dir"
  elif [[ -d "$target" ]]; then
    # For plaintext backups, verify checksums
    if [[ -f "$target/CHECKSUMS" ]]; then
      local failed
      failed=$(cd "$target" && sha256sum --check CHECKSUMS 2>&1 | grep -c "FAILED" || true)
      if [[ "$failed" -eq 0 ]]; then
        log_info "Checksum verification: PASSED ($(wc -l < "$target/CHECKSUMS") files)"
      else
        log_error "Checksum verification: FAILED ($failed files mismatch)"
        return 1
      fi
    else
      log_warn "No CHECKSUMS file found — skipping integrity check"
    fi
  fi
}

# ─── PostgreSQL Dump ────────────────────────────────────────────────────────
backup_postgresql() {
  local timestamp
  timestamp=$(date +%Y%m%d-%H%M%S)
  local dump_file="${BACKUP_DIR}/pg-dump-${timestamp}.sql.gz"

  if [[ -z "${PGHOST:-}" ]]; then
    log_warn "PGHOST not set — skipping PostgreSQL backup"
    return 0
  fi

  log_info "Backing up PostgreSQL database..."

  pg_dump -h "${PGHOST}" -U "${PGUSER:-velocity}" -d "${PGDATABASE:-velocity}" \
    --format=custom --compress=6 \
    -f "$dump_file" 2>/dev/null || {
    log_warn "pg_dump failed — PostgreSQL may not be available"
    return 0
  }

  log_info "PostgreSQL dump created: $dump_file ($(du -sh "$dump_file" | cut -f1))"
  echo "$dump_file"
}

# ─── Upload to S3 ───────────────────────────────────────────────────────────
upload_s3() {
  local target="$1"

  if [[ -z "$S3_BUCKET" ]]; then
    return 0
  fi

  if ! command -v aws &>/dev/null; then
    log_warn "AWS CLI not found — skipping S3 upload"
    return 0
  fi

  log_info "Uploading to S3: s3://${S3_BUCKET}/wal-backups/"

  if [[ -d "$target" ]]; then
    aws s3 sync "$target" "s3://${S3_BUCKET}/wal-backups/$(basename "$target")/" --quiet
  else
    aws s3 cp "$target" "s3://${S3_BUCKET}/wal-backups/$(basename "$target")" --quiet
  fi

  log_info "S3 upload complete"
}

# ─── Retention Cleanup ──────────────────────────────────────────────────────
cleanup_old_backups() {
  log_info "Cleaning up backups older than ${RETENTION} days..."

  local deleted=0
  # Remove old snapshot directories
  for dir in "$BACKUP_DIR"/wal-snapshot-*; do
    if [[ -d "$dir" ]]; then
      local dir_age_days=$(( ($(date +%s) - $(stat -c %Y "$dir" 2>/dev/null || echo 0)) / 86400 ))
      if [[ $dir_age_days -gt $RETENTION ]]; then
        rm -rf "$dir"
        deleted=$((deleted + 1))
      fi
    fi
  done

  # Remove old archives
  find "$BACKUP_DIR" -name "wal-snapshot-*.tar.gz*" -mtime "+${RETENTION}" -delete 2>/dev/null || true
  find "$BACKUP_DIR" -name "pg-dump-*.sql.gz" -mtime "+${RETENTION}" -delete 2>/dev/null || true

  log_info "Cleanup complete: $deleted old snapshot(s) removed"
}

# ─── Main ───────────────────────────────────────────────────────────────────
main() {
  log_info "=== Velocity WAL Backup ==="
  log_info "WAL dir: $WAL_DIR | Backup dir: $BACKUP_DIR"

  preflight

  # Step 1: WAL snapshot
  local snapshot_dir
  snapshot_dir=$(backup_wal)

  # Step 2: Encrypt if requested
  local backup_target="$snapshot_dir"
  if [[ "$ENCRYPT" = true ]]; then
    backup_target=$(encrypt_backup "$snapshot_dir")
  fi

  # Step 3: Verify
  if [[ "$VERIFY" = true ]]; then
    verify_backup "$backup_target"
  fi

  # Step 4: PostgreSQL dump (optional, best-effort)
  backup_postgresql || true

  # Step 5: Upload to S3 (optional)
  upload_s3 "$backup_target"

  # Step 6: Cleanup old backups
  cleanup_old_backups

  log_info "=== Backup complete ==="
}

main "$@"
