# platform/transform/shared/iceberg.py
#
# Iceberg catalog management for the Cricket Intelligence Platform.
#
# Responsibilities:
#   - Namespace (database) creation and validation
#   - Table existence checks and property management
#   - Schema version registration → cricket_control.schema_version
#   - Snapshot expiry (table maintenance)
#   - Catalog health checks
#
# Usage:
#   from cip.transform.shared.iceberg import IcebergCatalogManager
#   mgr = IcebergCatalogManager.from_settings()
#   mgr.ensure_namespace(Layer.BRONZE)
#   mgr.register_schema_version(fqn, df_schema)

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from cip.common.contracts.enums import Layer, SchemaChangeType
from cip.common.exceptions import IcebergError, SnapshotError, TableNotFoundError
from cip.common.logging import get_context, get_logger
from cip.common.settings import get_settings

logger = get_logger(__name__)


# ===========================================================================
# Schema hash utility
# ===========================================================================


def compute_schema_hash(fields: list[dict[str, str]]) -> str:
    """
    Compute a deterministic SHA-256 hash from a list of {name, type} dicts.
    Field list is sorted by name before hashing — order-independent.

    Args:
        fields: List of {"name": "col_name", "type": "string"} dicts

    Returns:
        40-char hex SHA-256 string
    """
    normalised = sorted(fields, key=lambda f: f["name"])
    raw = json.dumps(normalised, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def detect_schema_change(
    old_fields: list[dict[str, str]],
    new_fields: list[dict[str, str]],
) -> tuple[SchemaChangeType, list[dict[str, Any]]]:
    """
    Compare two schema field lists and return the change type + diff.

    Returns:
        (SchemaChangeType, changed_columns)
        changed_columns is a list of {name, change, old_type, new_type} dicts
    """
    old_map = {f["name"]: f["type"] for f in old_fields}
    new_map = {f["name"]: f["type"] for f in new_fields}

    added = [{"name": n, "change": "added", "type": t} for n, t in new_map.items() if n not in old_map]
    dropped = [{"name": n, "change": "dropped", "type": t} for n, t in old_map.items() if n not in new_map]
    changed = [
        {"name": n, "change": "type_changed", "old_type": old_map[n], "new_type": new_map[n]}
        for n in old_map
        if n in new_map and old_map[n] != new_map[n]
    ]

    all_changes = added + dropped + changed

    if not all_changes:
        return SchemaChangeType.NO_CHANGE, []
    if added and not dropped and not changed:
        return SchemaChangeType.ADD_COLUMN, added
    if dropped and not added and not changed:
        return SchemaChangeType.DROP_COLUMN, dropped
    if changed and not added and not dropped:
        return SchemaChangeType.TYPE_CHANGE, changed
    return SchemaChangeType.ADD_COLUMN, all_changes  # mixed — treat as additive


# ===========================================================================
# IcebergCatalogManager
# ===========================================================================


class IcebergCatalogManager:
    """
    Manages Iceberg catalog operations for the Cricket Intelligence Platform.

    Wraps PyIceberg catalog with:
        - Namespace lifecycle (create, list, validate)
        - Table property management
        - Schema version registration to PostgreSQL control schema
        - Snapshot expiry for table maintenance

    One instance per job — create via IcebergCatalogManager.from_settings().
    """

    def __init__(
        self,
        catalog_props: dict[str, str] | None = None,
        pg_dsn: str | None = None,
    ) -> None:
        from pyiceberg.catalog import load_catalog

        from cip.transform.shared.readers import _build_pyiceberg_catalog_props

        props = catalog_props or _build_pyiceberg_catalog_props()
        cfg = get_settings()
        self._catalog_name = cfg.iceberg.catalog_name
        self._catalog = load_catalog(self._catalog_name, **props)
        self._pg_dsn = pg_dsn or cfg.postgres.dsn

        logger.debug(
            "IcebergCatalogManager initialised",
            extra={
                "catalog": self._catalog_name,
                "rest_uri": props.get("uri"),
            },
        )

    @classmethod
    def from_settings(cls) -> "IcebergCatalogManager":
        return cls()

    # -----------------------------------------------------------------------
    # Namespace operations
    # -----------------------------------------------------------------------

    def namespace_exists(self, namespace: str) -> bool:
        """Return True if a namespace exists in the catalog."""
        try:
            existing = [str(n) for ns in self._catalog.list_namespaces() for n in ns]
            return namespace in existing
        except Exception as exc:
            raise IcebergError(
                f"Failed to list namespaces: {exc}",
                catalog=self._catalog_name,
            ) from exc

    def ensure_namespace(self, layer: Layer) -> None:
        """
        Create a namespace for the given layer if it does not exist.
        Idempotent — safe to call on every job startup.

        Args:
            layer: Layer enum — creates namespace matching layer.value
                   e.g. Layer.BRONZE → namespace "bronze"

        Example:
            mgr.ensure_namespace(Layer.BRONZE)   # creates "bronze" if absent
            mgr.ensure_namespace(Layer.SILVER)   # creates "silver" if absent
        """
        namespace = str(layer)
        if self.namespace_exists(namespace):
            logger.debug(
                "Namespace already exists",
                extra={"namespace": namespace},
            )
            return

        try:
            self._catalog.create_namespace(
                namespace,
                properties={
                    "platform.layer": namespace,
                    "platform.created-by": "cricket-platform",
                    "platform.created-at": datetime.now(tz=timezone.utc).isoformat(),
                },
            )
            logger.info("Created Iceberg namespace", extra={"namespace": namespace})
        except Exception as exc:
            # Race condition on parallel job startup — re-check existence
            if self.namespace_exists(namespace):
                logger.debug(
                    "Namespace created by concurrent job — ignoring",
                    extra={"namespace": namespace},
                )
                return
            raise IcebergError(
                f"Failed to create namespace '{namespace}': {exc}",
                namespace=namespace,
            ) from exc

    def ensure_all_namespaces(self) -> None:
        """
        Ensure all platform namespaces exist.
        Called once during bootstrap and at DAG startup.
        """
        for layer in [Layer.BRONZE, Layer.SILVER, Layer.GOLD]:
            self.ensure_namespace(layer)
        logger.info("All Iceberg namespaces verified")

    def list_namespaces(self) -> list[str]:
        """Return a flat list of namespace names."""
        return [str(n) for ns in self._catalog.list_namespaces() for n in ns]

    # -----------------------------------------------------------------------
    # Table operations
    # -----------------------------------------------------------------------

    def table_exists(self, fqn: str) -> bool:
        """Return True if a table exists in the catalog."""
        try:
            self._catalog.load_table(fqn)
            return True
        except Exception:
            return False

    def list_tables(self, layer: Layer) -> list[str]:
        """
        List all tables in a namespace.
        Returns FQNs: ["bronze.match_data", ...]
        """
        namespace = str(layer)
        try:
            tables = self._catalog.list_tables(namespace)
            return [f"{self._catalog_name}.{namespace}.{t[1]}" for t in tables]
        except Exception as exc:
            raise IcebergError(
                f"Failed to list tables in namespace '{namespace}': {exc}",
                namespace=namespace,
            ) from exc

    def get_table_properties(self, fqn: str) -> dict[str, str]:
        """Return the current table properties for an Iceberg table."""
        table = self._load_table(fqn)
        return dict(table.properties)

    def set_table_properties(self, fqn: str, properties: dict[str, str]) -> None:
        """Update table properties. Existing properties not in the dict are unchanged."""
        table = self._load_table(fqn)
        with table.update_properties() as updater:
            for k, v in properties.items():
                updater[k] = v
        logger.info(
            "Updated table properties",
            extra={"table": fqn, "properties": properties},
        )

    def get_current_schema(self, fqn: str) -> list[dict[str, str]]:
        """
        Return the current schema of an Iceberg table as a list of
        {"name": ..., "type": ..., "nullable": ...} dicts.
        """
        table = self._load_table(fqn)
        schema = table.schema()
        return [
            {
                "name": field.name,
                "type": str(field.field_type),
                "nullable": str(field.optional),
            }
            for field in schema.fields
        ]

    def get_current_snapshot(self, fqn: str) -> dict[str, Any] | None:
        """Return metadata for the latest Iceberg snapshot."""
        table = self._load_table(fqn)
        snap = table.current_snapshot()
        if not snap:
            return None
        return {
            "snapshot_id": snap.snapshot_id,
            "timestamp_ms": snap.timestamp_ms,
            "operation": snap.summary.get("operation", ""),
            "added_records": int(snap.summary.get("added-records", 0)),
            "deleted_records": int(snap.summary.get("deleted-records", 0)),
            "total_records": int(snap.summary.get("total-records", 0)),
            "added_files": int(snap.summary.get("added-data-files", 0)),
        }

    def get_snapshot_history(self, fqn: str) -> list[dict[str, Any]]:
        """Return full snapshot history for a table."""
        table = self._load_table(fqn)
        return [
            {
                "snapshot_id": s.snapshot_id,
                "timestamp_ms": s.timestamp_ms,
                "operation": s.summary.get("operation", ""),
                "total_records": int(s.summary.get("total-records", 0)),
                "added_records": int(s.summary.get("added-records", 0)),
            }
            for s in table.history()
        ]

    # -----------------------------------------------------------------------
    # Schema version registration
    # -----------------------------------------------------------------------

    def register_schema_version(
        self,
        fqn: str,
        fields: list[dict[str, str]],
        pipeline_name: str | None = None,
        dag_run_id: str | None = None,
        iceberg_snapshot_id: int | None = None,
    ) -> SchemaChangeType:
        """
        Register the current schema in cricket_control.schema_version.
        Detects drift against the previous registered schema for this table.

        Returns the SchemaChangeType so callers can react to unexpected changes.

        Args:
            fqn:                 Iceberg table FQN
            fields:              Current schema as [{name, type, nullable}] list
            pipeline_name:       Calling pipeline name
            dag_run_id:          Airflow dag_run_id for correlation
            iceberg_snapshot_id: Iceberg snapshot at write time

        Example:
            change_type = mgr.register_schema_version(
                fqn=TableName.bronze("match_data"),
                fields=mgr.get_current_schema(TableName.bronze("match_data")),
                pipeline_name="parse_bronze_match_documents",
            )
            if change_type == SchemaChangeType.DROP_COLUMN:
                raise SchemaEvolutionError(...)
        """
        import psycopg2

        ctx = get_context()
        pipeline = pipeline_name or ctx.get("pipeline_name", "")
        dag_run = dag_run_id or ctx.get("dag_run_id", "")

        schema_hash = compute_schema_hash(fields)

        # Fetch previous version for diff
        previous_fields = self._fetch_previous_schema(fqn)
        if previous_fields is None:
            change_type = SchemaChangeType.INITIAL
            changed_cols: list[dict[str, Any]] = []
        else:
            prev_hash = compute_schema_hash(previous_fields)
            if prev_hash == schema_hash:
                logger.debug(
                    "Schema unchanged — skipping registration",
                    extra={"table": fqn},
                )
                return SchemaChangeType.NO_CHANGE
            change_type, changed_cols = detect_schema_change(previous_fields, fields)

        logger.info(
            "Registering schema version",
            extra={
                "table": fqn,
                "change_type": change_type,
                "column_count": len(fields),
                "changed_columns": len(changed_cols),
            },
        )

        sql = """
            INSERT INTO cricket_control.schema_version (
                version_id, table_fqn, schema_hash, iceberg_snapshot_id,
                column_count, columns_json, change_type, changed_columns,
                detected_at, pipeline_name, dag_run_id
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                NOW(), %s, %s
            )
            ON CONFLICT (table_fqn, schema_hash) DO NOTHING
        """

        try:
            conn = psycopg2.connect(self._pg_dsn)
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            str(uuid.uuid4()),
                            fqn,
                            schema_hash,
                            iceberg_snapshot_id,
                            len(fields),
                            json.dumps(fields),
                            str(change_type),
                            json.dumps(changed_cols),
                            pipeline,
                            dag_run,
                        ),
                    )
            conn.close()
        except Exception as exc:
            # Schema registration failure is non-fatal — log and continue
            logger.warning(
                "Failed to register schema version — continuing",
                extra={"table": fqn, "error": str(exc)},
            )

        return change_type

    def _fetch_previous_schema(self, fqn: str) -> list[dict[str, str]] | None:
        """Fetch the most recently registered schema for a table from PostgreSQL."""
        import psycopg2

        sql = """
            SELECT columns_json
            FROM cricket_control.schema_version
            WHERE table_fqn = %s
            ORDER BY detected_at DESC
            LIMIT 1
        """
        try:
            conn = psycopg2.connect(self._pg_dsn)
            with conn.cursor() as cur:
                cur.execute(sql, (fqn,))
                row = cur.fetchone()
            conn.close()
            if row:
                return json.loads(row[0])
            return None
        except Exception:
            return None

    # -----------------------------------------------------------------------
    # Snapshot expiry (table maintenance)
    # -----------------------------------------------------------------------

    def expire_snapshots(
        self,
        fqn: str,
        older_than_ms: int | None = None,
        retain_last: int = 3,
    ) -> dict[str, int]:
        """
        Expire old Iceberg snapshots to control metadata file growth.
        Called by the maintenance task in dag_run_quality_checks.

        Args:
            fqn:           Iceberg table FQN
            older_than_ms: Expire snapshots older than this epoch ms
                           (default: 7 days ago)
            retain_last:   Always keep at least this many snapshots

        Returns:
            {"expired": N} dict for logging

        Example:
            mgr.expire_snapshots(TableName.bronze("match_data"), retain_last=5)
        """
        from datetime import timedelta

        if older_than_ms is None:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
            older_than_ms = int(cutoff.timestamp() * 1000)

        table = self._load_table(fqn)
        history = table.history()

        # Count snapshots newer than cutoff to satisfy retain_last
        recent = [s for s in history if s.timestamp_ms >= older_than_ms]
        if len(recent) < retain_last:
            logger.info(
                "Skipping snapshot expiry — not enough recent snapshots",
                extra={
                    "table": fqn,
                    "recent_count": len(recent),
                    "retain_last": retain_last,
                },
            )
            return {"expired": 0}

        expired_count = 0
        try:
            table.expire_snapshots().expire_older_than(older_than_ms).commit()
            # Count difference in history
            new_history = self._load_table(fqn).history()
            expired_count = len(history) - len(new_history)
        except Exception as exc:
            raise SnapshotError(fqn, "expire", reason=str(exc)) from exc

        logger.info(
            "Snapshots expired",
            extra={"table": fqn, "expired": expired_count},
        )
        return {"expired": expired_count}

    def expire_all_bronze_snapshots(self, retain_last: int = 3) -> None:
        """
        Expire old snapshots across all Bronze tables.
        Run periodically as a maintenance step.
        """
        for fqn in self.list_tables(Layer.BRONZE):
            try:
                result = self.expire_snapshots(fqn, retain_last=retain_last)
                logger.info(
                    "Bronze snapshot expiry",
                    extra={"table": fqn, "expired": result["expired"]},
                )
            except Exception as exc:
                logger.warning(
                    "Snapshot expiry failed — skipping table",
                    extra={"table": fqn, "error": str(exc)},
                )

    # -----------------------------------------------------------------------
    # Catalog health check
    # -----------------------------------------------------------------------

    def health_check(self) -> bool:
        """
        Verify the Iceberg REST catalog is reachable and all namespaces exist.
        Called on job startup and by `make bootstrap`.
        """
        try:
            namespaces = self.list_namespaces()
        except Exception as exc:
            raise IcebergError(
                f"Iceberg REST catalog unreachable: {exc}",
                rest_uri=get_settings().iceberg.rest_uri,
            ) from exc

        required = [str(layer) for layer in [Layer.BRONZE, Layer.SILVER, Layer.GOLD]]
        missing = [ns for ns in required if ns not in namespaces]

        if missing:
            logger.warning(
                "Missing Iceberg namespaces — run: make bootstrap",
                extra={"missing": missing},
            )
            return False

        logger.info(
            "Iceberg catalog health check passed",
            extra={"namespaces": namespaces},
        )
        return True

    # -----------------------------------------------------------------------
    # Internal
    # -----------------------------------------------------------------------

    def _load_table(self, fqn: str):
        """Load a PyIceberg Table, raising TableNotFoundError if absent."""
        try:
            return self._catalog.load_table(fqn)
        except Exception as exc:
            parts = fqn.split(".")
            ns = parts[1] if len(parts) >= 3 else ""
            tbl = parts[2] if len(parts) >= 3 else fqn
            raise TableNotFoundError(ns, tbl) from exc
