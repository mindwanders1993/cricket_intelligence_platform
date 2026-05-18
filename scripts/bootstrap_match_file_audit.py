#!/usr/bin/env python3
# scripts/bootstrap_match_file_audit.py
#
# One-shot script that seeds control.match_file_audit from the existing
# bronze.match_data Iceberg table. Run once after applying the new DDL
# (PR 1 of the match-data pipeline rework).
#
# For each (match_id, revision) row in bronze.match_data, the script:
#   1. Reads the source JSON bytes from MinIO landing.
#   2. Computes sha256.
#   3. Inserts one audit row with:
#         landing_loaded_at = bronze_loaded_at = silver_loaded_at = _ingested_at
#         archive_file      = 'bootstrap'
#         loaded_by_pipeline= 'bootstrap'
#         archive_path      = NULL (archive copy only applies forward)
#         gold_loaded_at    = NULL (Gold incremental DAG will stamp on first run)
#
# Bootstrap stamps silver_loaded_at because Silver is already built from this
# Bronze data — the first post-deploy Silver incremental run should be a no-op.
# Gold runs separately (manual + Metabase outage) so we leave gold_loaded_at
# NULL — the first Gold run picks up everything as pending.
#
# Idempotent: ON CONFLICT (file_name, content_hash) DO NOTHING.
#
# Usage:
#   poetry run python scripts/bootstrap_match_file_audit.py
#   poetry run python scripts/bootstrap_match_file_audit.py --dry-run
#   poetry run python scripts/bootstrap_match_file_audit.py --max-workers 30

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger("bootstrap_match_file_audit")


BRONZE_TABLE = "bronze.match_data"
BOOTSTRAP_ARCHIVE_FILE = "bootstrap"
BOOTSTRAP_PIPELINE_LABEL = "bootstrap"
DEFAULT_MAX_WORKERS = 20

# Bronze rows can come from three historical landing layouts. We try them in
# this order — first hit wins. The new per-archive prefix is most likely for
# data ingested since the per-archive partitioning landed.
_LANDING_PREFIXES = (
    "match_data/json/snapshot_date={date}/archive=all_json/{file}",
    "match_data/json/snapshot_date={date}/archive=recently_added_2_json/{file}",
    "match_data/json/snapshot_date={date}/{file}",  # legacy, pre-per-archive
)


@dataclass(frozen=True)
class _BronzeRow:
    file_name: str
    match_id: str
    snapshot_date: str  # ISO format
    ingested_at: datetime
    pipeline_run_id: str
    revision: int


@dataclass(frozen=True)
class _AuditPayload:
    bronze: _BronzeRow
    content_hash: str
    landing_path: str  # s3://... the path we found the JSON at


# ---------------------------------------------------------------------------
# Step 1: read existing Bronze rows
# ---------------------------------------------------------------------------


def _scan_bronze_rows() -> list[_BronzeRow]:
    from cip.common.contracts.naming import META
    from cip.transform.shared.readers import PolarsIcebergReader

    reader = PolarsIcebergReader.from_settings()
    df = reader.read_table(
        BRONZE_TABLE,
        columns=[
            "match_id",
            "revision",
            META.SNAPSHOT_DATE,
            META.INGESTED_AT,
            META.PIPELINE_RUN_ID,
        ],
    )

    rows: list[_BronzeRow] = []
    for r in df.iter_rows(named=True):
        match_id = r["match_id"]
        if match_id is None:
            continue
        match_id_str = str(match_id)
        # Cricsheet match files are named "{match_id}.json".
        # Bronze stores the ARCHIVE filename (e.g. all_json.zip) in _source_file,
        # so we cannot use META.SOURCE_FILE to identify the source JSON.
        file_name = f"{match_id_str}.json"

        snapshot_value = r[META.SNAPSHOT_DATE]
        snapshot_date = snapshot_value.isoformat() if hasattr(snapshot_value, "isoformat") else str(snapshot_value)
        ingested_at = r[META.INGESTED_AT]
        if isinstance(ingested_at, str):
            ingested_at = datetime.fromisoformat(ingested_at)
        rows.append(
            _BronzeRow(
                file_name=file_name,
                match_id=match_id_str,
                snapshot_date=snapshot_date,
                ingested_at=ingested_at,
                pipeline_run_id=str(r[META.PIPELINE_RUN_ID] or ""),
                revision=int(r["revision"]) if r["revision"] is not None else 1,
            )
        )

    logger.info("Scanned Bronze rows", extra={"count": len(rows)})
    return rows


# ---------------------------------------------------------------------------
# Step 2: fetch JSON bytes from MinIO and compute sha256
# ---------------------------------------------------------------------------


def _fetch_and_hash(row: _BronzeRow, minio, bucket: str) -> _AuditPayload | None:
    """Try each known landing prefix until we find the file. Returns None if
    the JSON has been deleted from landing (retention policy etc.)."""
    from cip.common.exceptions import ObjectNotFoundError, StorageError

    for prefix in _LANDING_PREFIXES:
        key = prefix.format(date=row.snapshot_date, file=row.file_name)
        try:
            data = minio.read_bytes(bucket, key)
        except ObjectNotFoundError:
            continue
        except StorageError:
            continue
        content_hash = hashlib.sha256(data).hexdigest()
        return _AuditPayload(
            bronze=row,
            content_hash=content_hash,
            landing_path=f"s3://{bucket}/{key}",
        )

    logger.warning(
        "Source JSON not found in any landing prefix — skipping audit row",
        extra={"file_name": row.file_name, "snapshot_date": row.snapshot_date},
    )
    return None


def _build_payloads_parallel(rows: list[_BronzeRow], max_workers: int) -> list[_AuditPayload]:
    from cip.common.settings import get_settings
    from cip.ingestion.io.minio import MinIOClient

    minio = MinIOClient.from_settings()
    bucket = get_settings().storage.bucket_source_files

    payloads: list[_AuditPayload] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_and_hash, row, minio, bucket): row for row in rows}
        for n, fut in enumerate(as_completed(futures), start=1):
            result = fut.result()
            if result is not None:
                payloads.append(result)
            if n % 500 == 0:
                logger.info("Hash progress", extra={"hashed": n, "total": len(rows)})

    logger.info("Hashing complete", extra={"found": len(payloads), "skipped": len(rows) - len(payloads)})
    return payloads


# ---------------------------------------------------------------------------
# Step 3: bulk INSERT audit rows
# ---------------------------------------------------------------------------


def _insert_audit_rows(payloads: list[_AuditPayload], dry_run: bool) -> int:
    if not payloads:
        return 0

    import psycopg2
    from psycopg2.extras import execute_values

    from cip.common.settings import get_settings

    if dry_run:
        logger.info("DRY RUN — would insert audit rows", extra={"count": len(payloads)})
        return 0

    cfg = get_settings()
    pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")

    values = [
        (
            p.bronze.file_name,
            p.content_hash,
            p.bronze.match_id,
            "json",
            BOOTSTRAP_ARCHIVE_FILE,
            None,  # archive_download_id
            p.landing_path,
            BOOTSTRAP_PIPELINE_LABEL,
            p.bronze.ingested_at,  # landing_loaded_at
            p.bronze.ingested_at,  # bronze_loaded_at
            p.bronze.ingested_at,  # silver_loaded_at — Silver already built
            p.bronze.revision,
            p.bronze.pipeline_run_id or "bootstrap",
        )
        for p in payloads
    ]

    with psycopg2.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO control.match_file_audit (
                    file_name, content_hash, match_id, file_type,
                    archive_file, archive_download_id, landing_path,
                    loaded_by_pipeline,
                    landing_loaded_at, bronze_loaded_at, silver_loaded_at,
                    revision, pipeline_run_id
                ) VALUES %s
                ON CONFLICT (file_name, content_hash) DO NOTHING
                """,
                values,
                page_size=500,
            )
            inserted = cur.rowcount
        conn.commit()

    logger.info("Audit rows inserted", extra={"requested": len(values), "inserted": inserted})
    return inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed control.match_file_audit from existing bronze.match_data.")
    p.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Thread pool size for MinIO fetches (default: {DEFAULT_MAX_WORKERS}).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan + hash but do not INSERT into Postgres.",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )

    args = _parse_args()

    rows = _scan_bronze_rows()
    if not rows:
        logger.info("No Bronze rows to bootstrap — exiting")
        return

    payloads = _build_payloads_parallel(rows, max_workers=args.max_workers)
    inserted = _insert_audit_rows(payloads, dry_run=args.dry_run)

    logger.info(
        "Bootstrap complete",
        extra={
            "bronze_rows": len(rows),
            "audit_payloads": len(payloads),
            "rows_inserted": inserted,
            "dry_run": args.dry_run,
        },
    )


if __name__ == "__main__":
    main()
