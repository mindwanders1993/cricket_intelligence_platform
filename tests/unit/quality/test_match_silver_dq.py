# tests/unit/quality/test_match_silver_dq.py
#
# Unit tests for MatchDataSilverDQChecker (MAT-SLV-001..012).

from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl

from cip.quality.checks.match_silver_dq import MatchDataSilverDQChecker

_SNAPSHOT = "2026-05-17"
_RUN_ID = "dq-test-run"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_checker(reader=None, pg_dsn="postgresql://u:p@h/db") -> MatchDataSilverDQChecker:
    return MatchDataSilverDQChecker(reader=reader or MagicMock(), pg_dsn=pg_dsn)


def _df(rows: list[dict], schema: dict[str, pl.DataType] | None = None) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema or {})
    return pl.DataFrame(rows, schema=schema) if schema else pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# MAT-SLV-001: matches.match_id NOT NULL
# ---------------------------------------------------------------------------


class TestCheckMatchesNotNull:
    def test_passes_when_all_match_ids_present(self):
        df = _df([{"match_id": "a"}, {"match_id": "b"}])
        result = _make_checker()._check_matches_not_null(df)
        assert result.status == "PASSED"
        assert result.severity == "BLOCK"
        assert result.check_id == "MAT-SLV-001"

    def test_fails_when_any_null(self):
        df = _df([{"match_id": "a"}, {"match_id": None}, {"match_id": "c"}])
        result = _make_checker()._check_matches_not_null(df)
        assert result.status == "FAILED"
        assert result.failure_row_count == 1


# ---------------------------------------------------------------------------
# MAT-SLV-002: matches.match_id unique
# ---------------------------------------------------------------------------


class TestCheckMatchesUnique:
    def test_passes_on_unique_ids(self):
        df = _df([{"match_id": "a"}, {"match_id": "b"}, {"match_id": "c"}])
        result = _make_checker()._check_matches_unique(df)
        assert result.status == "PASSED"
        assert result.check_id == "MAT-SLV-002"

    def test_fails_on_duplicate(self):
        df = _df([{"match_id": "a"}, {"match_id": "a"}, {"match_id": "b"}])
        result = _make_checker()._check_matches_unique(df)
        assert result.status == "FAILED"
        assert result.failure_row_count == 1


# ---------------------------------------------------------------------------
# Generic grain uniqueness — covers MAT-SLV-003/004/008/009/010/011
# ---------------------------------------------------------------------------


class TestCheckGrainUnique:
    def test_passes_on_unique_grain(self):
        df = _df(
            [
                {"match_id": "a", "innings_number": 1},
                {"match_id": "a", "innings_number": 2},
                {"match_id": "b", "innings_number": 1},
            ]
        )
        result = _make_checker()._check_grain_unique(
            df, "MAT-SLV-003", "silver.innings", ["match_id", "innings_number"]
        )
        assert result.status == "PASSED"
        assert result.severity == "BLOCK"

    def test_fails_on_duplicate_grain(self):
        df = _df(
            [
                {"match_id": "a", "innings_number": 1},
                {"match_id": "a", "innings_number": 1},  # dup
                {"match_id": "b", "innings_number": 1},
            ]
        )
        result = _make_checker()._check_grain_unique(
            df, "MAT-SLV-003", "silver.innings", ["match_id", "innings_number"]
        )
        assert result.status == "FAILED"
        assert result.failure_row_count == 1

    def test_check_id_and_grain_columns_propagate(self):
        df = _df([{"a": 1, "b": 2}])
        result = _make_checker()._check_grain_unique(df, "MAT-SLV-999", "silver.x", ["a", "b"])
        assert result.check_id == "MAT-SLV-999"
        assert result.table_name == "silver.x"
        assert "(a, b)" in result.check_name


# ---------------------------------------------------------------------------
# MAT-SLV-005: unmatched rate
# ---------------------------------------------------------------------------


class TestCheckUnmatchedRate:
    def test_passes_under_threshold(self):
        audit = _df([{"match_id": "a"}, {"match_id": "b"}])  # 2 unmatched
        players = _df([{"x": i} for i in range(98)])  # 98 + 0 = 98 denominator
        officials = _df([{"x": i} for i in range(2)])
        result = _make_checker()._check_unmatched_rate(audit, players, officials)
        # 2 / 100 = 2.0% <= 5% → PASSED
        assert result.status == "PASSED"
        assert result.severity == "WARN"

    def test_warns_over_threshold(self):
        audit = _df([{"match_id": str(i)} for i in range(10)])  # 10 unmatched
        players = _df([{"x": i} for i in range(50)])
        officials = _df([{"x": i} for i in range(50)])
        result = _make_checker()._check_unmatched_rate(audit, players, officials)
        # 10 / 100 = 10% > 5% → WARNING
        assert result.status == "WARNING"
        assert result.failure_row_count == 10

    def test_skipped_when_denominator_zero(self):
        audit = _df([])
        players = _df([])
        officials = _df([])
        result = _make_checker()._check_unmatched_rate(audit, players, officials)
        assert result.status == "SKIPPED"


# ---------------------------------------------------------------------------
# MAT-SLV-006: wickets.fielders non-empty for catch/run-out/stumped/c&b
# ---------------------------------------------------------------------------


class TestCheckWicketsFielders:
    def _df_with_fielders(self, rows):
        # explicit schema needed for list[str] columns when input may have NULLs
        return pl.DataFrame(
            rows,
            schema={
                "match_id": pl.Utf8,
                "kind": pl.Utf8,
                "fielders": pl.List(pl.Utf8),
            },
        )

    def test_passes_when_all_fielder_required_have_fielders(self):
        df = self._df_with_fielders(
            [
                {"match_id": "a", "kind": "caught", "fielders": ["F1"]},
                {"match_id": "b", "kind": "run out", "fielders": ["F1", "F2"]},
                {"match_id": "c", "kind": "bowled", "fielders": None},  # not fielder-required
            ]
        )
        result = _make_checker()._check_wickets_fielders(df)
        assert result.status == "PASSED"
        assert result.severity == "WARN"

    def test_warns_when_caught_has_empty_fielders(self):
        df = self._df_with_fielders(
            [
                {"match_id": "a", "kind": "caught", "fielders": None},
                {"match_id": "b", "kind": "caught", "fielders": []},
                {"match_id": "c", "kind": "caught", "fielders": ["F1"]},
            ]
        )
        result = _make_checker()._check_wickets_fielders(df)
        assert result.status == "WARNING"
        assert result.failure_row_count == 2

    def test_skipped_when_no_fielder_required_wickets(self):
        df = self._df_with_fielders(
            [
                {"match_id": "a", "kind": "bowled", "fielders": None},
                {"match_id": "b", "kind": "lbw", "fielders": None},
            ]
        )
        result = _make_checker()._check_wickets_fielders(df)
        assert result.status == "SKIPPED"


# ---------------------------------------------------------------------------
# MAT-SLV-007: match_players.person_id referential
# ---------------------------------------------------------------------------


class TestCheckMatchPlayersPersonIdReferential:
    def test_passes_when_all_person_ids_in_persons(self):
        players = _df(
            [
                {"match_id": "a", "team": "X", "player_name": "P1", "person_id": "ID1"},
                {"match_id": "a", "team": "X", "player_name": "P2", "person_id": "ID2"},
                {"match_id": "b", "team": "Y", "player_name": "P3", "person_id": None},  # null skipped
            ]
        )
        persons = _df([{"person_id": "ID1"}, {"person_id": "ID2"}])
        result = _make_checker()._check_match_players_person_id_referential(players, persons)
        assert result.status == "PASSED"

    def test_warns_when_orphan_exists(self):
        players = _df(
            [
                {"match_id": "a", "team": "X", "player_name": "P1", "person_id": "ID1"},
                {"match_id": "a", "team": "X", "player_name": "P2", "person_id": "ORPHAN"},
            ]
        )
        persons = _df([{"person_id": "ID1"}])
        result = _make_checker()._check_match_players_person_id_referential(players, persons)
        assert result.status == "WARNING"
        assert result.failure_row_count == 1

    def test_skipped_when_no_resolved_person_ids(self):
        players = _df(
            [
                {"match_id": "a", "team": "X", "player_name": "P1", "person_id": None},
                {"match_id": "b", "team": "Y", "player_name": "P2", "person_id": None},
            ]
        )
        persons = _df([{"person_id": "ID1"}])
        result = _make_checker()._check_match_players_person_id_referential(players, persons)
        assert result.status == "SKIPPED"


# ---------------------------------------------------------------------------
# MAT-SLV-012: deliveries batter/bowler null rate
# ---------------------------------------------------------------------------


class TestCheckDeliveriesMetadataCoverage:
    def test_passes_when_no_nulls(self):
        df = _df(
            [
                {"batter": "X", "bowler": "Y"},
                {"batter": "X", "bowler": "Z"},
            ]
        )
        result = _make_checker()._check_deliveries_metadata_coverage(df)
        assert result.status == "PASSED"

    def test_warns_when_null_rate_exceeds_threshold(self):
        # 5 rows total, 2 with a null → 40% > 1%
        df = _df(
            [
                {"batter": "X", "bowler": "Y"},
                {"batter": None, "bowler": "Y"},
                {"batter": "X", "bowler": None},
                {"batter": "X", "bowler": "Y"},
                {"batter": "X", "bowler": "Y"},
            ]
        )
        result = _make_checker()._check_deliveries_metadata_coverage(df)
        assert result.status == "WARNING"
        assert result.failure_row_count == 2

    def test_skipped_when_empty(self):
        df = _df([], schema={"batter": pl.Utf8, "bowler": pl.Utf8})
        result = _make_checker()._check_deliveries_metadata_coverage(df)
        assert result.status == "SKIPPED"


# ---------------------------------------------------------------------------
# _read fallback when table missing
# ---------------------------------------------------------------------------


class TestReadFallback:
    def test_returns_empty_df_when_table_not_found(self):
        from cip.common.exceptions import TableNotFoundError

        reader = MagicMock()
        reader.read_table.side_effect = TableNotFoundError(namespace="silver", table_name="foo")
        checker = _make_checker(reader=reader)

        df = checker._read("silver.foo", ["a", "b"], "_snapshot_date = '2026-05-17'")
        assert df.height == 0
        assert df.columns == ["a", "b"]
