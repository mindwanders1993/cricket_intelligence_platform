# tests/unit/ingestion/register/test_parse.py
"""Unit tests for RegisterParser."""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
from cip.ingestion.register.normalize import NormalizedRegister


class TestParseFromDfs:
    """parse_from_dfs() must produce identical output to parse(NormalizedRegister)."""

    @staticmethod
    def _make_people_df() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "identifier": ["p001", "p002", "p003"],
                "name": ["Virat Kohli", "Rohit Sharma", "MS Dhoni"],
                "unique_name": ["virat-kohli", "rohit-sharma", "ms-dhoni"],
                "key_cricinfo": ["253802", "34102", "28081"],
                "_snapshot_date": ["2026-05-11"] * 3,
                "_ingested_at": ["2026-05-11T00:00:00+00:00"] * 3,
                "_pipeline_run_id": ["test-run-001"] * 3,
                "_row_hash": ["h001", "h002", "h003"],
            }
        )

    @staticmethod
    def _make_names_df() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "identifier": ["p001", "p002"],
                "name": ["V Kohli", "RG Sharma"],
                "_snapshot_date": ["2026-05-11"] * 2,
                "_ingested_at": ["2026-05-11T00:00:00+00:00"] * 2,
                "_pipeline_run_id": ["test-run-001"] * 2,
                "_row_hash": ["h004", "h005"],
            }
        )

    @staticmethod
    def _make_normalized(people_df: pl.DataFrame, names_df: pl.DataFrame) -> NormalizedRegister:
        return NormalizedRegister(
            people=people_df.lazy(),
            names=names_df.lazy(),
            snapshot_date="2026-05-11",
            pipeline_run_id="test-run-001",
            ingested_at=datetime.now(tz=timezone.utc),
        )

    def test_parse_from_dfs_matches_parse(self):
        from cip.ingestion.register.parse import RegisterParser

        people_df = self._make_people_df()
        names_df = self._make_names_df()
        normalized = self._make_normalized(people_df, names_df)

        result_a = RegisterParser.parse(normalized)
        result_b = RegisterParser.parse_from_dfs(
            people_df=people_df,
            names_df=names_df,
            snapshot_date="2026-05-11",
            pipeline_run_id="test-run-001",
        )

        assert result_a.persons.collect().height == result_b.persons.collect().height
        assert result_a.person_identifiers.collect().height == result_b.person_identifiers.collect().height
        assert result_a.name_variations.collect().height == result_b.name_variations.collect().height

    def test_parse_from_dfs_returns_parsed_register(self):
        from cip.ingestion.register.parse import ParsedRegister, RegisterParser

        result = RegisterParser.parse_from_dfs(
            people_df=self._make_people_df(),
            names_df=self._make_names_df(),
            snapshot_date="2026-05-11",
            pipeline_run_id="test-run-001",
        )
        assert isinstance(result, ParsedRegister)

    def test_parse_from_dfs_preserves_snapshot_date(self):
        from cip.ingestion.register.parse import RegisterParser

        result = RegisterParser.parse_from_dfs(
            people_df=self._make_people_df(),
            names_df=self._make_names_df(),
            snapshot_date="2026-05-11",
            pipeline_run_id="test-run-001",
        )
        assert result.snapshot_date == "2026-05-11"

    def test_parse_from_dfs_preserves_pipeline_run_id(self):
        from cip.ingestion.register.parse import RegisterParser

        result = RegisterParser.parse_from_dfs(
            people_df=self._make_people_df(),
            names_df=self._make_names_df(),
            snapshot_date="2026-05-11",
            pipeline_run_id="test-run-001",
        )
        assert result.pipeline_run_id == "test-run-001"
