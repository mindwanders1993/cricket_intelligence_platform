# tests/unit/ingestion/register/test_normalize.py
"""
Unit tests for RegisterNormalizer.
All MinIO I/O is mocked — no network or Docker required.
"""
from __future__ import annotations

import hashlib
import io
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cip.ingestion.register.normalize import (
    RegisterNormalizer,
    NormalizedRegister,
    _PEOPLE_FILE,
    _NAMES_FILE,
)

# ---------------------------------------------------------------------------
# Fixtures — minimal valid CSVs matching Cricsheet Register schema
# ---------------------------------------------------------------------------

PEOPLE_CSV = b"""identifier,name,unique_name,gender,dob
p001,Virat Kohli,kohli-virat,male,1988-11-05
p002,Rohit Sharma,sharma-rohit,male,1987-04-30
p003,,,female,
"""

NAMES_CSV = b"""identifier,name
p001,Virat Kohli
p001,Kohli
p002,Rohit Sharma
"""

PEOPLE_CSV_EXTRA_KEY_COL = b"""identifier,name,unique_name,gender,dob,key_cricinfo
p001,Virat Kohli,kohli-virat,male,1988-11-05,253802
"""


def _make_normalizer(people_bytes=PEOPLE_CSV, names_bytes=NAMES_CSV):
    """Return a RegisterNormalizer with a mocked MinIOClient."""
    mock_minio = MagicMock()

    def read_side_effect(object_key: str) -> bytes:
        if "people.csv" in object_key:
            return people_bytes
        if "names.csv" in object_key:
            return names_bytes
        return b""

    mock_minio.read_object.side_effect = read_side_effect
    return RegisterNormalizer(minio_client=mock_minio)


# ---------------------------------------------------------------------------
# 1. Return type and structure
# ---------------------------------------------------------------------------

class TestNormalizedRegisterStructure:

    def test_returns_normalized_register_dataclass(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        assert isinstance(result, NormalizedRegister)

    def test_people_and_names_are_lazy_frames(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        assert isinstance(result.people, pl.LazyFrame)
        assert isinstance(result.names, pl.LazyFrame)

    def test_snapshot_date_and_run_id_on_result(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        assert result.snapshot_date == "2026-05-11"
        assert result.pipeline_run_id == "run-001"


# ---------------------------------------------------------------------------
# 2. All-string schema enforcement
# ---------------------------------------------------------------------------

class TestAllStringSchema:

    def test_all_source_columns_are_utf8(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        source_cols = [c for c in df.columns if not c.startswith("_")]
        for col in source_cols:
            assert df[col].dtype == pl.Utf8, (
                f"Column '{col}' has dtype {df[col].dtype}, expected Utf8. "
                "All columns must be all-string for source fidelity."
            )

    def test_empty_fields_are_null_not_empty_string(self):
        """Cricsheet CSVs have empty strings for optional fields — must become null."""
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        # Row 3 in PEOPLE_CSV has empty name and unique_name
        assert df["name"][2] is None
        assert df["unique_name"][2] is None


# ---------------------------------------------------------------------------
# 3. Metadata columns
# ---------------------------------------------------------------------------

class TestMetadataColumns:
    EXPECTED_META_COLS = {"_snapshot_date", "_ingested_at", "_pipeline_run_id", "_row_hash"}

    def test_all_metadata_columns_present(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        for col in self.EXPECTED_META_COLS:
            assert col in df.columns, f"Missing metadata column: {col}"

    def test_snapshot_date_value(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        assert all(df["_snapshot_date"] == "2026-05-11")

    def test_pipeline_run_id_value(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        assert all(df["_pipeline_run_id"] == "run-001")

    def test_ingested_at_is_datetime(self):
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        assert df["_ingested_at"].dtype in (pl.Datetime, pl.Date)

    def test_row_hash_is_hex_string_of_64_chars(self):
        """SHA-256 hex digest is always 64 characters."""
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        for h in df["_row_hash"]:
            assert len(h) == 64, f"row_hash length is {len(h)}, expected 64"
            assert all(c in "0123456789abcdef" for c in h), "row_hash is not valid hex"

    def test_row_hash_is_deterministic(self):
        """Same data + same metadata → same hash every time."""
        n = _make_normalizer()
        r1 = n.run("2026-05-11", "run-001").people.collect()
        r2 = n.run("2026-05-11", "run-001").people.collect()
        assert list(r1["_row_hash"]) == list(r2["_row_hash"])

    def test_row_hash_differs_for_different_rows(self):
        """Two different data rows must not produce the same hash."""
        result = _make_normalizer().run("2026-05-11", "run-001")
        df = result.people.collect()
        hashes = df["_row_hash"].to_list()
        assert len(hashes) == len(set(hashes)), "Duplicate row hashes detected — hash collision or identity rows"

    def test_metadata_columns_come_after_source_columns(self):
        """Source columns must precede metadata columns in column order."""
        result = _make_normalizer().run("2026-05-11", "run-001")
        cols = result.people.collect().columns
        meta_indices = [i for i, c in enumerate(cols) if c.startswith("_")]
        source_indices = [i for i, c in enumerate(cols) if not c.startswith("_")]
        assert max(source_indices) < min(meta_indices), (
            "Metadata columns must appear AFTER all source columns"
        )


# ---------------------------------------------------------------------------
# 4. Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:

    def test_raises_file_not_found_when_landing_is_empty(self):
        mock_minio = MagicMock()
        mock_minio.read_object.return_value = b""
        normalizer = RegisterNormalizer(minio_client=mock_minio)
        with pytest.raises(FileNotFoundError, match="Landing object not found"):
            normalizer.run("2026-05-11", "run-001")

    def test_raises_value_error_on_empty_csv(self):
        mock_minio = MagicMock()
        mock_minio.read_object.return_value = b"identifier,name\n"  # header only, no rows
        normalizer = RegisterNormalizer(minio_client=mock_minio)
        with pytest.raises(ValueError, match="empty DataFrame"):
            normalizer.run("2026-05-11", "run-001")

    def test_names_frame_populated_independently(self):
        """names.csv failure must not affect people.csv read path."""
        mock_minio = MagicMock()

        def side_effect(key):
            if "people.csv" in key:
                return PEOPLE_CSV
            return b""  # names missing

        mock_minio.read_object.side_effect = side_effect
        normalizer = RegisterNormalizer(minio_client=mock_minio)
        with pytest.raises(FileNotFoundError):
            normalizer.run("2026-05-11", "run-001")


# ---------------------------------------------------------------------------
# 5. Schema drift resilience
# ---------------------------------------------------------------------------

class TestSchemaDriftResilience:

    def test_extra_key_column_is_preserved_as_utf8(self):
        """New key_* columns from Cricsheet must pass through without error."""
        result = _make_normalizer(
            people_bytes=PEOPLE_CSV_EXTRA_KEY_COL
        ).run("2026-05-11", "run-001")
        df = result.people.collect()
        assert "key_cricinfo" in df.columns
        assert df["key_cricinfo"].dtype == pl.Utf8


# ---------------------------------------------------------------------------
# 6. Object key utility
# ---------------------------------------------------------------------------

class TestLandingObjectKey:

    def test_object_key_format(self):
        key = RegisterNormalizer.landing_object_key("people.csv", "2026-05-11")
        assert key == "register_csv/snapshot_date=2026-05-11/people.csv"