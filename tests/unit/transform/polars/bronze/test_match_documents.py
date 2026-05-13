# tests/unit/transform/polars/bronze/test_match_documents.py
#
# Unit tests for MatchBronzeLoader and _parse_json_file helper.

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import polars as pl


# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------

_SNAPSHOT = "2026-05-01"
_RUN_ID = "test-run-bronze"


def _make_loader(minio=None, writer=None, pg_dsn="postgresql://u:p@h/db"):
    from cip.transform.polars.bronze.match_documents import MatchBronzeLoader

    return MatchBronzeLoader(
        minio=minio or MagicMock(),
        writer=writer or MagicMock(),
        pg_dsn=pg_dsn,
    )


def _make_match_json(
    match_id: str = "12345",
    match_type: str = "T20",
    gender: str = "male",
    season: str | int = "2026",
    dates: list | None = None,
    teams: list | None = None,
    venue: str = "Eden Gardens",
    city: str = "Kolkata",
) -> bytes:
    data = {
        "info": {
            "match_type": match_type,
            "gender": gender,
            "season": season,
            "dates": ["2026-05-01"] if dates is None else dates,
            "teams": ["India", "England"] if teams is None else teams,
            "venue": venue,
            "city": city,
        },
        "innings": [],
    }
    return json.dumps(data).encode("utf-8")


# ---------------------------------------------------------------------------
# Tests: _parse_json_file
# ---------------------------------------------------------------------------


class TestParseJsonFile:
    def test_basic_parse(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        content = _make_match_json(match_id="12345")
        result = _parse_json_file("12345.json", content)

        assert result is not None
        assert result["match_id"] == "12345"
        assert result["match_type"] == "T20"
        assert result["gender"] == "male"
        assert result["season"] == "2026"
        assert result["match_date"] == "2026-05-01"
        assert result["team_a"] == "India"
        assert result["team_b"] == "England"
        assert result["venue"] == "Eden Gardens"
        assert result["city"] == "Kolkata"
        assert "raw_json" in result
        assert len(result["raw_json"]) > 0

    def test_match_id_strips_json_extension(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        result = _parse_json_file("match_99999.json", _make_match_json())
        assert result["match_id"] == "match_99999"

    def test_season_as_integer_coerced_to_string(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        content = _make_match_json(season=2007)
        result = _parse_json_file("m.json", content)
        assert result["season"] == "2007"

    def test_season_with_slash_preserved(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        content = _make_match_json(season="2011/12")
        result = _parse_json_file("m.json", content)
        assert result["season"] == "2011/12"

    def test_missing_dates_returns_empty_string(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        content = _make_match_json(dates=[])
        result = _parse_json_file("m.json", content)
        assert result["match_date"] == ""

    def test_missing_teams_returns_empty_strings(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        content = _make_match_json(teams=[])
        result = _parse_json_file("m.json", content)
        assert result["team_a"] == ""
        assert result["team_b"] == ""

    def test_single_team_fills_team_b_empty(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        content = _make_match_json(teams=["India"])
        result = _parse_json_file("m.json", content)
        assert result["team_a"] == "India"
        assert result["team_b"] == ""

    def test_null_venue_handled(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        content = _make_match_json(venue=None, city=None)
        result = _parse_json_file("m.json", content)
        assert result["venue"] == ""
        assert result["city"] == ""

    def test_invalid_json_returns_none(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        result = _parse_json_file("bad.json", b"{not valid json")
        assert result is None

    def test_empty_bytes_returns_none(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        result = _parse_json_file("empty.json", b"")
        assert result is None

    def test_revision_placeholder_is_one(self):
        from cip.transform.polars.bronze.match_documents import _parse_json_file

        result = _parse_json_file("m.json", _make_match_json())
        assert result["revision"] == "1"


# ---------------------------------------------------------------------------
# Tests: _attach_revisions
# ---------------------------------------------------------------------------


class TestAttachRevisions:
    def _run(self, records, existing):

        loader = _make_loader()
        return loader._attach_revisions(records, existing)

    def test_new_match_gets_revision_one(self):
        records = [{"match_id": "new_match", "revision": "1"}]
        result = self._run(records, existing={})
        assert result[0]["revision"] == "1"

    def test_existing_match_gets_incremented(self):
        records = [{"match_id": "old_match", "revision": "1"}]
        result = self._run(records, existing={"old_match": 3})
        assert result[0]["revision"] == "4"

    def test_mixed_new_and_existing(self):
        records = [
            {"match_id": "a", "revision": "1"},
            {"match_id": "b", "revision": "1"},
            {"match_id": "c", "revision": "1"},
        ]
        existing = {"a": 1, "b": 5}
        result = self._run(records, existing)

        rev_map = {r["match_id"]: r["revision"] for r in result}
        assert rev_map["a"] == "2"
        assert rev_map["b"] == "6"
        assert rev_map["c"] == "1"

    def test_empty_records(self):
        result = self._run([], {})
        assert result == []


# ---------------------------------------------------------------------------
# Tests: _fetch_existing_revisions
# ---------------------------------------------------------------------------


class TestFetchExistingRevisions:
    def test_returns_empty_when_table_not_found(self):
        loader = _make_loader()
        loader._writer._catalog.load_table.side_effect = Exception("table not found")

        result = loader._fetch_existing_revisions()
        assert result == {}

    def test_returns_empty_on_empty_table(self):
        import pyarrow as pa

        loader = _make_loader()
        mock_table = MagicMock()
        loader._writer._catalog.load_table.return_value = mock_table

        empty_arrow = pa.table({"match_id": pa.array([], type=pa.string()), "revision": pa.array([], type=pa.string())})
        mock_table.scan.return_value.to_arrow.return_value = empty_arrow

        result = loader._fetch_existing_revisions()
        assert result == {}

    def test_returns_max_revision_per_match_id(self):
        import pyarrow as pa

        loader = _make_loader()
        mock_table = MagicMock()
        loader._writer._catalog.load_table.return_value = mock_table

        arrow_data = pa.table({
            "match_id": pa.array(["m1", "m1", "m2"]),
            "revision": pa.array(["1", "2", "1"]),
        })
        mock_table.scan.return_value.to_arrow.return_value = arrow_data

        result = loader._fetch_existing_revisions()
        assert result == {"m1": 2, "m2": 1}


# ---------------------------------------------------------------------------
# Tests: MatchBronzeLoader.load idempotency
# ---------------------------------------------------------------------------


class TestMatchBronzeLoaderIdempotency:
    def test_skips_if_already_loaded(self):
        loader = _make_loader()

        with patch.object(loader, "_check_idempotency", return_value=77):
            result = loader.load(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                force=False,
            )

        assert result.files_attempted == 0
        assert result.rows_written == 0

    def test_force_deletes_partition_before_load(self):
        loader = _make_loader()

        with (
            patch.object(loader, "_check_idempotency", return_value=77),
            patch.object(loader, "_delete_partition") as mock_delete,
            patch.object(loader, "_insert_log_row", return_value=1),
            patch.object(loader, "_run_load") as mock_run,
            patch.object(loader, "_update_log_success"),
        ):
            from cip.transform.polars.bronze.match_documents import MatchLoadResult

            mock_run.return_value = MatchLoadResult(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                files_attempted=10,
                files_succeeded=10,
                files_failed=0,
                rows_written=10,
                duration_seconds=1.0,
            )

            loader.load(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID, force=True)

        mock_delete.assert_called_once_with(_SNAPSHOT)


# ---------------------------------------------------------------------------
# Tests: Bronze schema
# ---------------------------------------------------------------------------


class TestBronzeSchema:
    def test_schema_all_string_types(self):
        from cip.transform.polars.bronze.match_documents import _bronze_schema

        schema = _bronze_schema()
        for col_name, dtype in schema.items():
            assert dtype == pl.Utf8, f"Column {col_name!r} should be Utf8 but got {dtype}"

    def test_schema_has_required_columns(self):
        from cip.transform.polars.bronze.match_documents import _bronze_schema

        schema = _bronze_schema()
        required = {"match_id", "revision", "match_type", "gender", "season",
                    "match_date", "team_a", "team_b", "venue", "city", "raw_json"}
        assert required.issubset(set(schema.keys()))
