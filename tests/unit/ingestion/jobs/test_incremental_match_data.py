# tests/unit/ingestion/jobs/test_incremental_match_data.py
#
# Smoke tests for the daily incremental match-data job module. Confirms each
# task callable threads the right archive constants into the underlying
# match_data helpers. The helpers themselves (downloader, extractor,
# Bronze loader, DQ checker, Silver pipeline) carry their own unit tests.

from __future__ import annotations

from unittest.mock import MagicMock, patch


_SNAPSHOT = "2026-05-18"
_RUN_ID = "incr-test-run"


class TestPipelineIdentity:
    def test_archive_constants(self):
        from cip.ingestion.jobs import incremental_match_data as mod

        assert mod.ARCHIVE_FILE == "recently_added_2_json.zip"
        assert mod.ARCHIVE_URL == "https://cricsheet.org/downloads/recently_added_2_json.zip"
        assert mod.DAG_ID == "ingest_two_day_match_data_bronze"
        assert mod.LOADED_BY_PIPELINE == "incremental"
        assert mod.MIN_EXPECTED_BYTES == 50 * 1024


class TestTaskDownloadArchive:
    def test_threads_incremental_constants_into_downloader(self):
        from cip.ingestion.jobs import incremental_match_data as mod

        record = MagicMock(
            id=11,
            landing_path="s3://bucket/r2j.zip",
            file_size_bytes=300_000,
            checksum_sha256="abc",
            status="SUCCESS",
        )

        with patch("cip.ingestion.match_data.download.MatchDataDownloader") as MockCls:
            instance = MockCls.from_settings.return_value
            instance.download.return_value = record

            payload = mod.task_download_archive(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                force=False,
            )

        kwargs = MockCls.from_settings.call_args.kwargs
        assert kwargs["archive_file"] == "recently_added_2_json.zip"
        assert kwargs["archive_url"] == "https://cricsheet.org/downloads/recently_added_2_json.zip"
        assert kwargs["min_expected_bytes"] == 50 * 1024
        assert kwargs["dag_id"] == "ingest_two_day_match_data_bronze"
        assert payload["archive_download_id"] == 11


class TestTaskExtractArchive:
    def test_threads_incremental_archive_and_pipeline_label(self):
        from cip.ingestion.jobs import incremental_match_data as mod

        with patch("cip.ingestion.match_data.extract.MatchDataExtractor") as MockCls:
            instance = MockCls.from_settings.return_value
            instance.extract.return_value = MagicMock(
                file_count=30,
                extracted_prefix="s3://bucket/match_data/json/snapshot_date=…/archive=recently_added_2_json/",
            )

            mod.task_extract_archive(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID, force=False)

        kwargs = MockCls.from_settings.call_args.kwargs
        assert kwargs["archive_file"] == "recently_added_2_json.zip"
        assert kwargs["loaded_by_pipeline"] == "incremental"


class TestTaskLoadBronze:
    def test_threads_incremental_constants_into_loader(self):
        from cip.ingestion.jobs import incremental_match_data as mod

        with patch("cip.transform.polars.bronze.match_data.MatchBronzeLoader") as MockCls:
            instance = MockCls.from_settings.return_value
            instance.load.return_value = MagicMock(
                rows_written=5,
                files_attempted=30,
                files_succeeded=5,
                files_failed=0,
                files_skipped_by_audit=25,
            )

            payload = mod.task_load_bronze(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                force=False,
            )

        kwargs = MockCls.from_settings.call_args.kwargs
        assert kwargs["archive_file"] == "recently_added_2_json.zip"
        assert kwargs["dag_id"] == "ingest_two_day_match_data_bronze"
        assert payload["rows_written"] == 5
        assert payload["files_skipped_by_audit"] == 25


class TestTaskRunDq:
    def test_threads_incremental_archive_into_checker(self):
        from cip.ingestion.jobs import incremental_match_data as mod

        with patch("cip.quality.checks.match_bronze_dq.MatchBronzeDQChecker") as MockCls:
            instance = MockCls.from_settings.return_value
            instance.run_all.return_value = MagicMock(
                checks=[1, 2, 3, 4],
                passed_count=4,
                failed_count=0,
                blocking_failures=[],
            )

            payload = mod.task_run_dq(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        kwargs = MockCls.from_settings.call_args.kwargs
        assert kwargs["archive_file"] == "recently_added_2_json.zip"

        call = instance.run_all.call_args
        assert call.kwargs["dag_id"] == "ingest_two_day_match_data_bronze"
        assert payload["passed"] == 4


class TestTaskBuildSilver:
    def test_delegates_to_shared_silver_job(self):
        from cip.ingestion.jobs import incremental_match_data as mod

        with patch("cip.ingestion.jobs.build_silver_match_data.task_build_silver") as mock_build:
            mock_build.return_value = {"row_counts": {}, "total_rows": 0, "match_ids_scope": 0}

            mod.task_build_silver(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID, force=False)

        mock_build.assert_called_once()
        kwargs = mock_build.call_args.kwargs
        assert kwargs["snapshot_date"] == _SNAPSHOT
        assert kwargs["pipeline_run_id"] == _RUN_ID
