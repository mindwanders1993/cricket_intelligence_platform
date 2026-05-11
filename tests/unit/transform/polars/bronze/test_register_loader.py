# tests/unit/transform/polars/bronze/test_register_loader.py
"""
Unit tests for RegisterLoader.

PolarsIcebergWriter is mocked — no real MinIO or catalog connection.
Tests verify:
    - Correct table FQNs are targeted
    - create_and_append() is called (not raw append)
    - source_file is correct per frame
    - partition_cols includes _snapshot_date
    - LoadResult counts match frame sizes
    - Empty frames are skipped
    - overwrite_snapshot() calls delete before write
    - LoadResult.total_rows sums correctly
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
from cip.common.contracts.naming import META
from cip.ingestion.register.parse import ParsedRegister
from cip.transform.polars.bronze.register_loader import (
    PARTITION_COL,
    TABLE_NAME_VARIATIONS,
    TABLE_PERSON_IDENTIFIERS,
    TABLE_PERSONS,
    LoadResult,
    RegisterLoader,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SNAPSHOT = "2026-05-11"
_RUN_ID = "run-001"
_INGESTED_AT = datetime(2026, 5, 11, 0, 0, 0, tzinfo=timezone.utc)


def _meta(n: int) -> dict:
    return {
        "_snapshot_date": [_SNAPSHOT] * n,
        "_ingested_at": [_INGESTED_AT] * n,
        "_pipeline_run_id": [_RUN_ID] * n,
        "_row_hash": [f"h{i}" for i in range(n)],
    }


def _make_parsed(p=3, i=7, n=5) -> ParsedRegister:
    persons = pl.DataFrame(
        {
            "identifier": [f"p{j:03d}" for j in range(p)],
            "name": [f"Player {j}" for j in range(p)],
            "unique_name": [f"player-{j}" for j in range(p)],
            **_meta(p),
        }
    )
    ids = pl.DataFrame(
        {
            "identifier": [f"p{j:03d}" for j in range(i)],
            "key_source": ["cricinfo"] * i,
            "key_value": [f"ID{j}" for j in range(i)],
            **_meta(i),
        }
    )
    names = pl.DataFrame(
        {
            "identifier": [f"p{j:03d}" for j in range(n)],
            "name": [f"Alias {j}" for j in range(n)],
            **_meta(n),
        }
    )
    return ParsedRegister(
        persons=persons.lazy(),
        person_identifiers=ids.lazy(),
        name_variations=names.lazy(),
        snapshot_date=_SNAPSHOT,
        pipeline_run_id=_RUN_ID,
    )


def _mock_loader(row_count: int = 5) -> tuple[RegisterLoader, MagicMock]:
    mock_writer = MagicMock()
    mock_writer.create_and_append.return_value = row_count
    loader = RegisterLoader(writer=mock_writer)
    return loader, mock_writer


# ---------------------------------------------------------------------------
# 1. Table name constants — validated at import time via TableName.bronze()
# ---------------------------------------------------------------------------


class TestTableNameConstants:

    def test_persons_table_fqn(self):
        assert TABLE_PERSONS == "cricket.bronze.register_people"

    def test_identifiers_table_fqn(self):
        assert TABLE_PERSON_IDENTIFIERS == "cricket.bronze.register_identifiers"

    def test_name_variations_table_fqn(self):
        assert TABLE_NAME_VARIATIONS == "cricket.bronze.register_name_variations"

    def test_partition_col_is_snapshot_date(self):
        assert PARTITION_COL == META.SNAPSHOT_DATE
        assert PARTITION_COL == "_snapshot_date"


# ---------------------------------------------------------------------------
# 2. LoadResult
# ---------------------------------------------------------------------------


class TestLoadResult:

    def test_total_rows_sums_all_three(self):
        r = LoadResult(
            persons_rows=3,
            identifiers_rows=7,
            name_variations_rows=5,
            snapshot_date=_SNAPSHOT,
            pipeline_run_id=_RUN_ID,
        )
        assert r.total_rows == 15

    def test_tables_list_contains_all_three(self):
        r = LoadResult(
            persons_rows=1,
            identifiers_rows=1,
            name_variations_rows=1,
            snapshot_date=_SNAPSHOT,
            pipeline_run_id=_RUN_ID,
        )
        assert TABLE_PERSONS in r.tables
        assert TABLE_PERSON_IDENTIFIERS in r.tables
        assert TABLE_NAME_VARIATIONS in r.tables


# ---------------------------------------------------------------------------
# 3. load() — writer delegation
# ---------------------------------------------------------------------------


class TestLoad:

    def test_create_and_append_called_three_times(self):
        loader, mock_writer = _mock_loader()
        loader.load(_make_parsed())
        assert mock_writer.create_and_append.call_count == 3

    def test_raw_append_never_called(self):
        loader, mock_writer = _mock_loader()
        loader.load(_make_parsed())
        mock_writer.append.assert_not_called()

    def test_all_three_table_fqns_targeted(self):
        loader, mock_writer = _mock_loader()
        loader.load(_make_parsed())
        fqns = [c.kwargs["fqn"] for c in mock_writer.create_and_append.call_args_list]
        assert TABLE_PERSONS in fqns
        assert TABLE_PERSON_IDENTIFIERS in fqns
        assert TABLE_NAME_VARIATIONS in fqns

    def test_snapshot_date_passed_to_writer(self):
        loader, mock_writer = _mock_loader()
        loader.load(_make_parsed())
        for c in mock_writer.create_and_append.call_args_list:
            assert c.kwargs["snapshot_date"] == _SNAPSHOT

    def test_pipeline_run_id_passed_to_writer(self):
        loader, mock_writer = _mock_loader()
        loader.load(_make_parsed())
        for c in mock_writer.create_and_append.call_args_list:
            assert c.kwargs["pipeline_run_id"] == _RUN_ID

    def test_partition_col_in_partition_cols(self):
        loader, mock_writer = _mock_loader()
        loader.load(_make_parsed())
        for c in mock_writer.create_and_append.call_args_list:
            assert PARTITION_COL in c.kwargs["partition_cols"]

    def test_source_file_people_csv_for_persons(self):
        loader, mock_writer = _mock_loader()
        loader.load(_make_parsed())
        calls_by_fqn = {c.kwargs["fqn"]: c.kwargs["source_file"] for c in mock_writer.create_and_append.call_args_list}
        assert calls_by_fqn[TABLE_PERSONS] == "people.csv"
        assert calls_by_fqn[TABLE_PERSON_IDENTIFIERS] == "people.csv"
        assert calls_by_fqn[TABLE_NAME_VARIATIONS] == "names.csv"

    def test_load_result_row_counts(self):
        loader, mock_writer = _mock_loader(row_count=10)
        result = loader.load(_make_parsed())
        # mock returns 10 for every call
        assert result.persons_rows == 10
        assert result.identifiers_rows == 10
        assert result.name_variations_rows == 10
        assert result.total_rows == 30

    def test_load_result_snapshot_and_run_id(self):
        loader, _ = _mock_loader()
        result = loader.load(_make_parsed())
        assert result.snapshot_date == _SNAPSHOT
        assert result.pipeline_run_id == _RUN_ID

    def test_duration_seconds_positive(self):
        loader, _ = _mock_loader()
        result = loader.load(_make_parsed())
        assert result.duration_seconds >= 0


# ---------------------------------------------------------------------------
# 4. Empty frame handling
# ---------------------------------------------------------------------------


class TestEmptyFrames:

    def test_empty_persons_skips_write(self):
        loader, mock_writer = _mock_loader()
        parsed = _make_parsed(p=0, i=5, n=3)
        loader.load(parsed)
        fqns = [c.kwargs["fqn"] for c in mock_writer.create_and_append.call_args_list]
        assert TABLE_PERSONS not in fqns

    def test_empty_persons_zero_rows_in_result(self):
        loader, mock_writer = _mock_loader(row_count=5)
        parsed = _make_parsed(p=0, i=5, n=3)
        result = loader.load(parsed)
        assert result.persons_rows == 0

    def test_non_empty_frames_still_written_when_one_empty(self):
        loader, mock_writer = _mock_loader(row_count=5)
        parsed = _make_parsed(p=0, i=5, n=3)
        loader.load(parsed)
        # identifiers and name_variations should still be written
        fqns = [c.kwargs["fqn"] for c in mock_writer.create_and_append.call_args_list]
        assert TABLE_PERSON_IDENTIFIERS in fqns
        assert TABLE_NAME_VARIATIONS in fqns


# ---------------------------------------------------------------------------
# 5. overwrite_snapshot() — delete before write
# ---------------------------------------------------------------------------


class TestOverwriteSnapshot:

    def _make_loader_with_mock_catalog(self, row_count=5):
        """
        Returns (loader, mock_writer) with:
          - mock_writer.create_and_append returning row_count
          - loader._writer._catalog pointing to a MagicMock catalog
        """
        loader, mock_writer = _mock_loader(row_count=row_count)
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = MagicMock()
        loader._writer._catalog = mock_catalog
        return loader, mock_writer, mock_catalog

    def test_delete_called_before_create_and_append(self):
        loader, mock_writer, mock_catalog = self._make_loader_with_mock_catalog()
        call_order = []

        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_table.delete.side_effect = lambda *a, **kw: call_order.append("delete")
        mock_writer.create_and_append.side_effect = lambda **kw: call_order.append("write") or 5

        with patch.dict("sys.modules", {"pyiceberg.expressions": MagicMock()}):
            loader.overwrite_snapshot(_make_parsed())

        first_write = next(i for i, x in enumerate(call_order) if x == "write")
        last_delete = max(i for i, x in enumerate(call_order) if x == "delete")
        assert last_delete < first_write, f"Delete must precede all writes. Order: {call_order}"

    def test_delete_called_for_all_three_tables(self):
        loader, mock_writer, mock_catalog = self._make_loader_with_mock_catalog()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        with patch.dict("sys.modules", {"pyiceberg.expressions": MagicMock()}):
            loader.overwrite_snapshot(_make_parsed())

        assert mock_table.delete.call_count == 3

    def test_overwrite_returns_load_result(self):
        loader, mock_writer, mock_catalog = self._make_loader_with_mock_catalog(row_count=3)

        with patch.dict("sys.modules", {"pyiceberg.expressions": MagicMock()}):
            result = loader.overwrite_snapshot(_make_parsed())

        assert isinstance(result, LoadResult)

    def test_delete_graceful_on_missing_table(self):
        """First-run: table doesn't exist yet — delete must not raise."""
        loader, mock_writer = _mock_loader()
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("Table not found")
        loader._writer._catalog = mock_catalog

        with patch.dict("sys.modules", {"pyiceberg.expressions": MagicMock()}):
            result = loader.overwrite_snapshot(_make_parsed())

        assert isinstance(result, LoadResult)


# ---------------------------------------------------------------------------
# 6. from_settings factory
# ---------------------------------------------------------------------------


class TestFromSettings:

    def test_from_settings_returns_register_loader(self):
        with patch("cip.transform.polars.bronze.register_loader.PolarsIcebergWriter") as mock_cls:
            mock_cls.from_settings.return_value = MagicMock()
            loader = RegisterLoader.from_settings()
            assert isinstance(loader, RegisterLoader)
            mock_cls.from_settings.assert_called_once()
