# tests/unit/ingestion/audit/test_match_file_audit.py
#
# Unit tests for MatchFileAudit — the psycopg2 wrapper over
# control.match_file_audit.
#
# All tests mock psycopg2.connect so no real DB access occurs. The mock
# captures SQL + params per execute() call; assertions inspect both.

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from cip.ingestion.audit.match_file_audit import AuditRow, MatchFileAudit

# ---------------------------------------------------------------------------
# psycopg2 mock helper
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Minimal psycopg2 cursor stand-in.

    `fetch_responses` is consumed in execute-order: each execute() pops the
    next response off the queue and exposes it via fetchall/fetchone.
    """

    def __init__(self, fetch_responses: list[list[tuple]] | None = None):
        self._fetch_responses = list(fetch_responses or [])
        self.executed: list[tuple[str, tuple]] = []  # (sql, params)
        self.rowcount = 0
        self._last_response: list[tuple] | None = None
        # psycopg2.extras.execute_values reads cur.connection.encoding to
        # encode the SQL — wired by _FakeConn in its constructor.
        self.connection: "_FakeConn | None" = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql: str, params: tuple = ()):
        # psycopg2's encoded SQL arrives as bytes when called by execute_values.
        if isinstance(sql, bytes):
            sql = sql.decode("utf-8")
        self.executed.append((sql, params))
        if self._fetch_responses:
            self._last_response = self._fetch_responses.pop(0)
            self.rowcount = len(self._last_response)
        else:
            self._last_response = []
            # rowcount stays at whatever was set last — tests can override.

    def fetchall(self):
        return self._last_response or []

    def fetchone(self):
        if self._last_response:
            return self._last_response[0]
        return None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        cursor.connection = self
        self.encoding = "UTF8"
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


def _patched_connect(cursor: _FakeCursor):
    """Return a context manager that patches psycopg2.connect to yield _FakeConn(cursor)."""
    return patch(
        "psycopg2.connect",
        return_value=_FakeConn(cursor),
    )


_DSN = "postgresql://user:pass@host/db"
_TS = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# from_settings
# ---------------------------------------------------------------------------


class TestFromSettings:
    def test_strips_sqlalchemy_dialect_prefix(self):
        with patch("cip.common.settings.get_settings") as mock_get:
            mock_get.return_value = MagicMock(postgres=MagicMock(dsn="postgresql+psycopg2://u:p@h/db"))
            audit = MatchFileAudit.from_settings()

        assert audit._pg_dsn == "postgresql://u:p@h/db"


# ---------------------------------------------------------------------------
# lookup_bronze_loaded
# ---------------------------------------------------------------------------


class TestLookupBronzeLoaded:
    def test_empty_input_returns_empty_set_without_db_call(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()

        with _patched_connect(cur):
            result = audit.lookup_bronze_loaded(set())

        assert result == set()
        assert cur.executed == []

    def test_returns_subset_of_bronze_loaded_hashes(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor(fetch_responses=[[("abc",), ("def",)]])

        with _patched_connect(cur):
            result = audit.lookup_bronze_loaded({"abc", "def", "ghi"})

        assert result == {"abc", "def"}
        sql, params = cur.executed[0]
        assert "content_hash = ANY(%s)" in sql
        assert "bronze_loaded_at IS NOT NULL" in sql
        assert sorted(params[0]) == ["abc", "def", "ghi"]

    def test_no_matches_returns_empty(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor(fetch_responses=[[]])

        with _patched_connect(cur):
            result = audit.lookup_bronze_loaded({"abc"})

        assert result == set()


# ---------------------------------------------------------------------------
# insert_landing
# ---------------------------------------------------------------------------


class TestInsertLanding:
    def _row(self, file_name="12345.json", content_hash="abc"):
        return AuditRow(
            file_name=file_name,
            content_hash=content_hash,
            match_id=file_name.removesuffix(".json"),
            archive_file="all_json.zip",
            landing_path=f"s3://bucket/{file_name}",
            loaded_by_pipeline="full",
            pipeline_run_id="run-1",
            landing_loaded_at=_TS,
        )

    def test_empty_input_skips_db(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()

        with _patched_connect(cur):
            count = audit.insert_landing([])

        assert count == 0
        assert cur.executed == []

    def test_inserts_rows_with_on_conflict(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()
        cur.rowcount = 2  # set by execute_values mock

        rows = [self._row("1.json", "h1"), self._row("2.json", "h2")]

        def _fake_execute_values(cursor, sql, values, **_kwargs):
            cursor.executed.append((sql, values))

        with (
            _patched_connect(cur),
            patch("psycopg2.extras.execute_values", side_effect=_fake_execute_values),
        ):
            count = audit.insert_landing(rows)

        assert count == 2
        sql, values = cur.executed[0]
        assert "ON CONFLICT (file_name, content_hash) DO NOTHING" in sql
        assert len(values) == 2
        # Each row tuple has the 10 fields the SQL expects
        assert values[0] == (
            "1.json",
            "h1",
            "1",
            "json",
            "all_json.zip",
            None,  # archive_download_id
            "s3://bucket/1.json",
            "full",
            _TS,
            "run-1",
        )


# ---------------------------------------------------------------------------
# mark_bronze_loaded
# ---------------------------------------------------------------------------


class TestMarkBronzeLoaded:
    def test_empty_input_skips_db(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()

        with _patched_connect(cur):
            count = audit.mark_bronze_loaded(
                rows=[],
                pipeline_run_id="run-1",
                archive_file="all_json.zip",
                ts=_TS,
            )

        assert count == 0
        assert cur.executed == []

    def test_updates_with_revision_per_row(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()
        cur.rowcount = 3

        rows = [
            ("1.json", "h1", 1),
            ("2.json", "h2", 1),
            ("1.json", "h1-corrected", 2),  # correction of match_id=1
        ]

        with _patched_connect(cur):
            count = audit.mark_bronze_loaded(
                rows=rows,
                pipeline_run_id="run-1",
                archive_file="recently_added_2_json.zip",
                ts=_TS,
            )

        assert count == 3
        sql, params = cur.executed[0]
        # SET clause params (ts, archive_file, pipeline_run_id) come first,
        # then the three arrays for UNNEST.
        assert params[0] == _TS
        assert params[1] == "recently_added_2_json.zip"
        assert params[2] == "run-1"
        assert params[3] == ["1.json", "2.json", "1.json"]
        assert params[4] == ["h1", "h2", "h1-corrected"]
        assert params[5] == [1, 1, 2]
        assert "UNNEST" in sql


# ---------------------------------------------------------------------------
# mark_archived
# ---------------------------------------------------------------------------


class TestMarkArchived:
    def test_empty_input_skips_db(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()

        with _patched_connect(cur):
            count = audit.mark_archived({}, _TS)

        assert count == 0
        assert cur.executed == []

    def test_updates_archive_path_and_timestamp(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()
        cur.rowcount = 1

        mapping = {
            ("1.json", "h1"): "s3://bucket/archive/processed_date=2026-05-18/1.json",
        }

        with _patched_connect(cur):
            count = audit.mark_archived(mapping, _TS)

        assert count == 1
        sql, params = cur.executed[0]
        assert params[0] == _TS
        assert params[1] == ["1.json"]
        assert params[2] == ["h1"]
        assert params[3] == ["s3://bucket/archive/processed_date=2026-05-18/1.json"]


# ---------------------------------------------------------------------------
# pending_silver_match_ids / mark_silver_loaded
# ---------------------------------------------------------------------------


class TestSilverPending:
    def test_pending_silver_match_ids_returns_distinct_list(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor(fetch_responses=[[("12345",), ("67890",)]])

        with _patched_connect(cur):
            result = audit.pending_silver_match_ids()

        assert result == ["12345", "67890"]
        sql, _ = cur.executed[0]
        assert "bronze_loaded_at IS NOT NULL" in sql
        assert "silver_loaded_at IS NULL" in sql

    def test_mark_silver_loaded_with_empty_list_skips_db(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()

        with _patched_connect(cur):
            count = audit.mark_silver_loaded([], _TS)

        assert count == 0
        assert cur.executed == []

    def test_mark_silver_loaded_updates_only_pending_rows(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()
        cur.rowcount = 2

        with _patched_connect(cur):
            count = audit.mark_silver_loaded(["12345", "67890"], _TS)

        assert count == 2
        sql, params = cur.executed[0]
        assert "silver_loaded_at IS NULL" in sql
        assert params[0] == _TS
        assert params[1] == ["12345", "67890"]


# ---------------------------------------------------------------------------
# pending_gold_match_ids / mark_gold_loaded_pending / mark_gold_loaded_all_silver
# ---------------------------------------------------------------------------


class TestGoldPending:
    def test_pending_gold_match_ids(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor(fetch_responses=[[("12345",)]])

        with _patched_connect(cur):
            result = audit.pending_gold_match_ids()

        assert result == ["12345"]
        sql, _ = cur.executed[0]
        assert "silver_loaded_at IS NOT NULL" in sql
        assert "gold_loaded_at IS NULL" in sql

    def test_mark_gold_loaded_pending_empty(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()

        with _patched_connect(cur):
            count = audit.mark_gold_loaded_pending([], _TS)

        assert count == 0
        assert cur.executed == []

    def test_mark_gold_loaded_pending_updates(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()
        cur.rowcount = 1

        with _patched_connect(cur):
            count = audit.mark_gold_loaded_pending(["12345"], _TS)

        assert count == 1
        sql, params = cur.executed[0]
        assert "gold_loaded_at IS NULL" in sql
        assert params[1] == ["12345"]

    def test_mark_gold_loaded_all_silver_updates_every_silver_ready_row(self):
        audit = MatchFileAudit(_DSN)
        cur = _FakeCursor()
        cur.rowcount = 100

        with _patched_connect(cur):
            count = audit.mark_gold_loaded_all_silver(_TS)

        assert count == 100
        sql, params = cur.executed[0]
        assert "silver_loaded_at IS NOT NULL" in sql
        # No match_id filter — full refresh stamps everything
        assert "match_id" not in sql.split("WHERE")[1]
        assert params == (_TS,)


# ---------------------------------------------------------------------------
# AuditRow dataclass
# ---------------------------------------------------------------------------


class TestAuditRow:
    def test_frozen_dataclass_defaults(self):
        row = AuditRow(
            file_name="1.json",
            content_hash="h1",
            match_id="1",
            archive_file="all_json.zip",
            landing_path="s3://b/1.json",
            loaded_by_pipeline="full",
            pipeline_run_id="run-1",
            landing_loaded_at=_TS,
        )
        assert row.file_type == "json"
        assert row.archive_download_id is None

        # Frozen — mutation raises FrozenInstanceError (subclass of AttributeError).
        from dataclasses import FrozenInstanceError

        with pytest.raises(FrozenInstanceError):
            row.file_name = "other.json"  # type: ignore[misc]
