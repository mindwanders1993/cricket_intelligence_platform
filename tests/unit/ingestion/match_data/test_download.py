# tests/unit/ingestion/cricsheet/test_download.py
#
# Unit tests for MatchDataDownloader.
#
# All tests mock: MinIOClient, psycopg2, and urllib.request.urlretrieve
# so no real network or database access occurs.

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

_SNAPSHOT = "2026-05-01"
_RUN_ID = "test-run-001"
_ARCHIVE_FILE = "all_json.zip"
_ARCHIVE_URL = "https://cricsheet.org/downloads/all_json.zip"
_LANDING_PATH = f"s3://cricket-source-files/match_data/zip/snapshot_date={_SNAPSHOT}/{_ARCHIVE_FILE}"


def _fake_upload_result(s3_path: str = _LANDING_PATH):
    result = MagicMock()
    result.s3_path = s3_path
    return result


def _make_downloader(minio_mock, pg_dsn: str = "postgresql://user:pass@host/db"):
    from cip.ingestion.match_data.download import MatchDataDownloader

    return MatchDataDownloader(minio=minio_mock, pg_dsn=pg_dsn)


# ---------------------------------------------------------------------------
# Tests: checksum utility functions (used by downloader internally)
# ---------------------------------------------------------------------------


class TestChecksumUtils:
    def test_sha256_bytes_deterministic(self):
        from cip.ingestion.match_data.checksum import sha256_bytes

        data = b"hello world"
        h1 = sha256_bytes(data)
        h2 = sha256_bytes(data)
        assert h1 == h2

    def test_sha256_bytes_known_value(self):
        from cip.ingestion.match_data.checksum import sha256_bytes
        import hashlib

        data = b"cricsheet"
        expected = hashlib.sha256(data).hexdigest()
        assert sha256_bytes(data) == expected

    def test_sha256_file(self, tmp_path):
        from cip.ingestion.match_data.checksum import sha256_file
        import hashlib

        content = b"match_data" * 1000
        p = tmp_path / "test.zip"
        p.write_bytes(content)

        expected = hashlib.sha256(content).hexdigest()
        assert sha256_file(p) == expected
        assert sha256_file(str(p)) == expected

    def test_sha256_file_large_chunks(self, tmp_path):
        from cip.ingestion.match_data.checksum import sha256_file
        import hashlib

        content = b"x" * (1 << 17)  # 128 KB — spans multiple chunks
        p = tmp_path / "large.bin"
        p.write_bytes(content)
        assert sha256_file(p) == hashlib.sha256(content).hexdigest()


# ---------------------------------------------------------------------------
# Tests: MatchDataDownloader
# ---------------------------------------------------------------------------


class TestMatchDataDownloaderIdempotency:
    """Downloader skips when a SUCCESS row already exists."""

    def test_skips_if_already_downloaded(self):
        minio = MagicMock()
        downloader = _make_downloader(minio)

        existing_record = MagicMock()
        existing_record.status = "SUCCESS"
        existing_record.landing_path = _LANDING_PATH
        existing_record.id = 42

        with patch.object(downloader, "_check_idempotency", return_value=existing_record):
            result = downloader.download(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                force=False,
            )

        assert result is existing_record
        minio.upload_to_source_files.assert_not_called()

    def test_force_bypasses_idempotency(self, tmp_path):
        minio = MagicMock()
        minio.upload_to_source_files.return_value = _fake_upload_result()
        downloader = _make_downloader(minio)

        existing = MagicMock()
        existing.status = "SUCCESS"

        zip_content = b"PK\x03\x04" + b"0" * (15 * 1024 * 1024)  # fake 15 MB ZIP

        with (
            patch.object(downloader, "_check_idempotency", return_value=existing),
            patch.object(downloader, "_insert_log_row", return_value=99),
            patch.object(downloader, "_update_log_success") as mock_success,
            patch("cip.ingestion.match_data.download.sha256_file", return_value="abc123"),
            patch(
                "urllib.request.urlretrieve",
                side_effect=lambda url, dest: Path(dest).write_bytes(zip_content),
            ),
        ):
            mock_success.return_value = MagicMock(
                id=99,
                archive_file=_ARCHIVE_FILE,
                source_url=_ARCHIVE_URL,
                snapshot_date=_SNAPSHOT,
                landing_path=_LANDING_PATH,
                file_size_bytes=len(zip_content),
                checksum_sha256="abc123",
                status="SUCCESS",
            )
            result = downloader.download(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                force=True,
            )

        minio.upload_to_source_files.assert_called_once()
        assert result.status == "SUCCESS"


class TestMatchDataDownloaderSuccess:
    """Happy-path download."""

    def test_download_creates_success_record(self, tmp_path):
        minio = MagicMock()
        minio.upload_to_source_files.return_value = _fake_upload_result()
        downloader = _make_downloader(minio)

        zip_content = b"PK\x03\x04" + b"Z" * (20 * 1024 * 1024)  # 20 MB

        with (
            patch.object(downloader, "_check_idempotency", return_value=None),
            patch.object(downloader, "_insert_log_row", return_value=7),
            patch.object(downloader, "_update_log_success") as mock_success,
            patch("cip.ingestion.match_data.download.sha256_file", return_value="deadbeef"),
            patch(
                "urllib.request.urlretrieve",
                side_effect=lambda url, dest: Path(dest).write_bytes(zip_content),
            ),
        ):
            expected_record = MagicMock(
                id=7,
                archive_file=_ARCHIVE_FILE,
                source_url=_ARCHIVE_URL,
                snapshot_date=_SNAPSHOT,
                landing_path=_LANDING_PATH,
                file_size_bytes=len(zip_content),
                checksum_sha256="deadbeef",
                status="SUCCESS",
            )
            mock_success.return_value = expected_record
            result = downloader.download(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
            )

        assert result.status == "SUCCESS"
        assert result.checksum_sha256 == "deadbeef"
        assert result.id == 7
        assert minio.upload_to_source_files.call_count == 1
        call_kwargs = minio.upload_to_source_files.call_args.kwargs
        assert call_kwargs["prefix"] == "match_data/zip"
        assert call_kwargs["snapshot_date"] == _SNAPSHOT

    def test_upload_uses_match_data_zip_prefix(self, tmp_path):
        minio = MagicMock()
        minio.upload_to_source_files.return_value = _fake_upload_result()
        downloader = _make_downloader(minio)

        zip_content = b"PK\x03\x04" + b"A" * (12 * 1024 * 1024)

        with (
            patch.object(downloader, "_check_idempotency", return_value=None),
            patch.object(downloader, "_insert_log_row", return_value=1),
            patch.object(downloader, "_update_log_success") as mock_success,
            patch("cip.ingestion.match_data.download.sha256_file", return_value="aabbcc"),
            patch(
                "urllib.request.urlretrieve",
                side_effect=lambda url, dest: Path(dest).write_bytes(zip_content),
            ),
        ):
            mock_success.return_value = MagicMock(status="SUCCESS")
            downloader.download(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        _call = minio.upload_to_source_files.call_args
        assert _call.kwargs["prefix"] == "match_data/zip"
        assert _call.kwargs["snapshot_date"] == _SNAPSHOT


class TestMatchDataDownloaderArchiveOverride:
    """Downloader respects custom archive_file / archive_url overrides — required
    by the last-2-days incremental pipeline which reuses the same class."""

    _CUSTOM_FILE = "recently_added_2_json.zip"
    _CUSTOM_URL = "https://cricsheet.org/downloads/recently_added_2_json.zip"
    _CUSTOM_MIN_BYTES = 50 * 1024  # 50 KB

    def _make_custom_downloader(self, minio_mock):
        from cip.ingestion.match_data.download import MatchDataDownloader

        return MatchDataDownloader(
            minio=minio_mock,
            pg_dsn="postgresql://user:pass@host/db",
            archive_file=self._CUSTOM_FILE,
            archive_url=self._CUSTOM_URL,
            min_expected_bytes=self._CUSTOM_MIN_BYTES,
            dag_id="ingest_two_day_match_data_bronze",
        )

    def test_uses_custom_url_for_download(self, tmp_path):
        """urllib.request.urlretrieve must be called with the custom URL."""
        minio = MagicMock()
        minio.upload_to_source_files.return_value = _fake_upload_result()
        downloader = self._make_custom_downloader(minio)

        zip_content = b"PK\x03\x04" + b"R" * (self._CUSTOM_MIN_BYTES + 100)
        captured = {}

        def _capture(url, dest):
            captured["url"] = url
            Path(dest).write_bytes(zip_content)

        with (
            patch.object(downloader, "_check_idempotency", return_value=None),
            patch.object(downloader, "_insert_log_row", return_value=11),
            patch.object(downloader, "_update_log_success") as mock_success,
            patch("cip.ingestion.match_data.download.sha256_file", return_value="cafe"),
            patch("urllib.request.urlretrieve", side_effect=_capture),
        ):
            mock_success.return_value = MagicMock(status="SUCCESS")
            downloader.download(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        assert captured["url"] == self._CUSTOM_URL

    def test_smaller_archive_passes_when_below_full_default_but_above_custom_min(self, tmp_path):
        """A ~60 KB archive would fail the default 10 MB minimum but should pass
        when min_expected_bytes is configured for the incremental pipeline."""
        minio = MagicMock()
        minio.upload_to_source_files.return_value = _fake_upload_result()
        downloader = self._make_custom_downloader(minio)

        zip_content = b"PK\x03\x04" + b"Q" * (60 * 1024)  # 60 KB — below default 10 MB

        with (
            patch.object(downloader, "_check_idempotency", return_value=None),
            patch.object(downloader, "_insert_log_row", return_value=12),
            patch.object(downloader, "_update_log_success") as mock_success,
            patch("cip.ingestion.match_data.download.sha256_file", return_value="abcd"),
            patch(
                "urllib.request.urlretrieve",
                side_effect=lambda url, dest: Path(dest).write_bytes(zip_content),
            ),
        ):
            mock_success.return_value = MagicMock(status="SUCCESS")
            result = downloader.download(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        assert result.status == "SUCCESS"

    def test_from_settings_threads_overrides_into_instance(self):
        """The factory must accept overrides and forward them to the instance."""
        from cip.ingestion.match_data.download import MatchDataDownloader

        with (
            patch("cip.ingestion.match_data.download.MinIOClient.from_settings") as mock_minio,
            patch("cip.common.settings.get_settings") as mock_get,
        ):
            mock_minio.return_value = MagicMock()
            mock_get.return_value = MagicMock(
                postgres=MagicMock(dsn="postgresql+psycopg2://u:p@h/db")
            )

            downloader = MatchDataDownloader.from_settings(
                archive_file=self._CUSTOM_FILE,
                archive_url=self._CUSTOM_URL,
                min_expected_bytes=self._CUSTOM_MIN_BYTES,
                dag_id="ingest_two_day_match_data_bronze",
            )

        assert downloader._archive_file == self._CUSTOM_FILE
        assert downloader._archive_url == self._CUSTOM_URL
        assert downloader._min_expected_bytes == self._CUSTOM_MIN_BYTES
        assert downloader._dag_id == "ingest_two_day_match_data_bronze"


class TestMatchDataDownloaderFailures:
    """Error handling."""

    def test_too_small_raises_value_error(self, tmp_path):
        minio = MagicMock()
        downloader = _make_downloader(minio)

        tiny_zip = b"PK\x03\x04"  # only 4 bytes — way below 10 MB minimum

        with (
            patch.object(downloader, "_check_idempotency", return_value=None),
            patch.object(downloader, "_insert_log_row", return_value=5),
            patch.object(downloader, "_update_log_failure") as mock_fail,
            patch(
                "urllib.request.urlretrieve",
                side_effect=lambda url, dest: Path(dest).write_bytes(tiny_zip),
            ),
        ):
            with pytest.raises(ValueError, match="too small"):
                downloader.download(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        mock_fail.assert_called_once()
        log_id_arg, msg_arg = mock_fail.call_args.args
        assert log_id_arg == 5
        assert "too small" in msg_arg.lower() or "small" in msg_arg.lower()

    def test_download_failure_updates_log(self, tmp_path):
        minio = MagicMock()
        downloader = _make_downloader(minio)

        with (
            patch.object(downloader, "_check_idempotency", return_value=None),
            patch.object(downloader, "_insert_log_row", return_value=3),
            patch.object(downloader, "_update_log_failure") as mock_fail,
            patch(
                "urllib.request.urlretrieve",
                side_effect=ConnectionError("timeout"),
            ),
        ):
            with pytest.raises(ConnectionError):
                downloader.download(snapshot_date=_SNAPSHOT, pipeline_run_id=_RUN_ID)

        mock_fail.assert_called_once()
        log_id, error_msg = mock_fail.call_args.args
        assert log_id == 3
        assert "timeout" in error_msg
