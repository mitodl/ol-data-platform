#!/usr/bin/env bash
#
# Drop orphaned/stale warehouse tables from the Glue Data Catalog and delete their
# backing S3 data.
#
# Motivation: dbt does NOT drop the table for a model that is renamed or removed, so
# the old table lingers in Glue/S3 — never refreshed, but still queryable. Superset
# datasets (or ad-hoc queries) pointing at it then show silently stale data. The
# `ol-superset validate --dbt-dir` gate flags such datasets; this script cleans up
# the abandoned tables behind them.
#
# SAFETY:
#   - Dry-run by default. Pass --apply to actually delete.
#   - Refuses to drop a table updated within --max-age-days (default 30) unless
#     --force is given, so an actively-maintained table can't be removed by mistake.
#   - Fetches the S3 location from Glue at runtime and verifies it contains the table
#     name before deleting, so a bad/empty location never triggers a wide `s3 rm`.
#
# Prereq for datasets you are virtualizing (e.g. discussion_detail_report): deploy the
# virtual-dataset change to prod FIRST, so nothing still reads the physical table when
# you drop it.
#
# Usage:
#   scripts/drop_stale_warehouse_tables.sh                 # dry-run, default table list
#   scripts/drop_stale_warehouse_tables.sh --apply         # actually delete
#   scripts/drop_stale_warehouse_tables.sh --database DB table_a table_b
#   scripts/drop_stale_warehouse_tables.sh --apply --force stale_but_recent_table

set -euo pipefail

DATABASE="ol_warehouse_production_reporting"
APPLY=false
FORCE=false
MAX_AGE_DAYS=30
TABLES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY=true; shift ;;
    --force) FORCE=true; shift ;;
    --database) DATABASE="$2"; shift 2 ;;
    --max-age-days) MAX_AGE_DAYS="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "Unknown flag: $1" >&2; exit 2 ;;
    *) TABLES+=("$1"); shift ;;
  esac
done

# Default to the two known stale reporting orphans (see the dbt CI/QA hardening work).
if [[ ${#TABLES[@]} -eq 0 ]]; then
  TABLES=(discussion_detail_report video_engagements_report)
fi

$APPLY || echo "== DRY RUN (pass --apply to delete) =="
echo "Database: ${DATABASE}"
echo

now_epoch=$(date +%s)
max_age_seconds=$((MAX_AGE_DAYS * 86400))

for table in "${TABLES[@]}"; do
  echo "── ${table} ──"

  info=$(aws glue get-table --database-name "${DATABASE}" --name "${table}" \
    --query 'Table.{Location:StorageDescriptor.Location, UpdateTime:UpdateTime, CreateTime:CreateTime}' \
    --output json 2>/dev/null || true)

  if [[ -z "${info}" || "${info}" == "null" ]]; then
    echo "  not found in Glue — skipping"
    echo
    continue
  fi

  location=$(echo "${info}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("Location") or "")')
  update_time=$(echo "${info}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("UpdateTime") or "")')
  create_time=$(echo "${info}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("CreateTime") or "")')
  echo "  created: ${create_time}"
  echo "  updated: ${update_time}"
  echo "  s3:      ${location}"

  # Freshness guard: refuse to drop a table touched within the age window.
  if [[ -n "${update_time}" ]]; then
    update_epoch=$(date -d "${update_time}" +%s 2>/dev/null || echo 0)
    age_seconds=$((now_epoch - update_epoch))
    if [[ ${update_epoch} -gt 0 && ${age_seconds} -lt ${max_age_seconds} ]]; then
      if ! $FORCE; then
        echo "  ⚠️  updated $((age_seconds / 86400))d ago (< ${MAX_AGE_DAYS}d) — refusing without --force"
        echo
        continue
      fi
      echo "  ⚠️  updated recently but --force given — proceeding"
    fi
  fi

  # Location sanity: must be a non-empty s3:// path containing the table name.
  if [[ "${location}" != s3://*"${table}"* ]]; then
    echo "  ⚠️  location does not look like this table's data — skipping S3 delete"
    location=""
  fi

  if $APPLY; then
    if [[ -n "${location}" ]]; then
      echo "  deleting S3 data..."
      aws s3 rm --recursive "${location}"
    fi
    echo "  deleting Glue table..."
    aws glue delete-table --database-name "${DATABASE}" --name "${table}"
    echo "  ✅ dropped"
  else
    [[ -n "${location}" ]] && echo "  would: aws s3 rm --recursive '${location}'"
    echo "  would: aws glue delete-table --database-name '${DATABASE}' --name '${table}'"
  fi
  echo
done

$APPLY && echo "Done." || echo "Dry run complete — re-run with --apply to delete."
