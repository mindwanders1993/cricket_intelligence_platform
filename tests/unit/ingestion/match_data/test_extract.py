# tests/unit/ingestion/cricsheet/test_extract.py
#
# Unit tests for MatchDataExtractor.
#
# Creates an in-memory ZIP containing:
#   - 2 JSON files (should be extracted)
#   - 1 YAML file (should be ignored)
#   - 1 txt file  (should be ignored)

from __future__ import annotations

import io
import json
import zipfile
from unittest.mock import MagicMock, patch


_SNAPSHOT = "2026-05-01"
_RUN_ID = "extract-run-001"


def _make_fake_zip(json_names: list[str], extra_names: list[str] | None = None) -> bytes:
    """Build an in-memory ZIP with given JSON files + optional non-JSON extras."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in json_names:
            content = json.dumps({"info": {"match_type": "T20"}, "match_id": name}).encode()
            zf.writestr(name, content)
        for name in extra_names or []:
            zf.writestr(name, b"not json content")
    return buf.getvalue()


def _make_extractor(minio_mock):
    from cip.ingestion.match_data.extract import MatchDataExtractor

    return MatchDataExtractor(minio=minio_mock, pg_dsn="postgresql://user:pass@host/db")


# ---------------------------------------------------------------------------
# Tests: ExtractionManifest
# ---------------------------------------------------------------------------


class TestExtractionManifest:
    def test_round_trip_json(self):
        from cip.ingestion.match_data.manifest import ExtractionManifest, ManifestEntry

        entries = [
            ManifestEntry(file_name="12345.json", size_bytes=500, checksum_sha256="aaa"),
            ManifestEntry(file_name="67890.json", size_bytes=600, checksum_sha256="bbb"),
        ]
        manifest = ExtractionManifest(
            snapshot_date=_SNAPSHOT,
            archive_file="all_json.zip",
            file_count=2,
            entries=entries,
        )

        json_str = manifest.to_json()
        loaded = ExtractionManifest.from_json(json_str)

        assert loaded.snapshot_date == _SNAPSHOT
        assert loaded.file_count == 2
        assert len(loaded.entries) == 2
        assert loaded.entries[0].file_name == "12345.json"
        assert loaded.entries[1].checksum_sha256 == "bbb"

    def test_from_json_bytes(self):
        from cip.ingestion.match_data.manifest import ExtractionManifest

        raw = b'{"snapshot_date": "2026-01-01", "archive_file": "all_json.zip", "file_count": 0, "entries": []}'
        manifest = ExtractionManifest.from_json(raw)
        assert manifest.file_count == 0
        assert manifest.entries == []

    def test_manifest_object_key(self):
        from cip.ingestion.match_data.manifest import manifest_object_key

        key = manifest_object_key("2026-05-01")
        assert key == "match_data/json/snapshot_date=2026-05-01/_manifest.json"


class TestWriteReadManifest:
    def test_write_manifest_calls_upload_bytes(self):
        from cip.ingestion.match_data.manifest import ExtractionManifest, write_manifest

        minio = MagicMock()
        minio.upload_bytes.return_value = MagicMock()

        manifest = ExtractionManifest(
            snapshot_date=_SNAPSHOT,
            archive_file="all_json.zip",
            file_count=3,
            entries=[],
        )

        with patch("cip.common.settings.get_settings") as mock_cfg:
            mock_cfg.return_value.storage.bucket_source_files = "cricket-source-files"
            write_manifest(minio, manifest)

        minio.upload_bytes.assert_called_once()
        call_kwargs = minio.upload_bytes.call_args.kwargs
        assert call_kwargs["bucket"] == "cricket-source-files"
        assert "_manifest.json" in call_kwargs["key"]
        assert call_kwargs["content_type"] == "application/json"


# ---------------------------------------------------------------------------
# Tests: MatchDataExtractor._extract_and_upload
# ---------------------------------------------------------------------------


class TestExtractAndUpload:
    def test_only_json_files_extracted(self, tmp_path):
        json_names = ["match_a.json", "match_b.json"]
        extra_names = ["readme.yaml", "info.txt"]
        zip_bytes = _make_fake_zip(json_names, extra_names)

        zip_path = tmp_path / "test.zip"
        zip_path.write_bytes(zip_bytes)

        minio = MagicMock()
        minio.upload_bytes.return_value = MagicMock()
        extractor = _make_extractor(minio)

        entries, failed = extractor._extract_and_upload(
            local_zip=zip_path,
            snapshot_date=_SNAPSHOT,
            source_files_bucket="cricket-source-files",
        )

        assert len(entries) == 2
        assert len(failed) == 0
        assert minio.upload_bytes.call_count == 2

        uploaded_names = {call_args.kwargs["key"].split("/")[-1] for call_args in minio.upload_bytes.call_args_list}
        assert uploaded_names == {"match_a.json", "match_b.json"}

    def test_failed_upload_counted_separately(self, tmp_path):
        json_names = ["good.json", "bad.json"]
        zip_bytes = _make_fake_zip(json_names)

        zip_path = tmp_path / "test.zip"
        zip_path.write_bytes(zip_bytes)

        minio = MagicMock()

        def _upload_bytes_side_effect(**kwargs):
            if "bad.json" in kwargs["key"]:
                raise ConnectionError("upload error")
            return MagicMock()

        minio.upload_bytes.side_effect = _upload_bytes_side_effect
        extractor = _make_extractor(minio)

        entries, failed = extractor._extract_and_upload(
            local_zip=zip_path,
            snapshot_date=_SNAPSHOT,
            source_files_bucket="cricket-source-files",
        )

        assert len(entries) == 1
        assert len(failed) == 1
        assert entries[0].file_name == "good.json"

    def test_manifest_entries_have_checksums(self, tmp_path):
        json_names = ["x.json"]
        zip_bytes = _make_fake_zip(json_names)

        zip_path = tmp_path / "test.zip"
        zip_path.write_bytes(zip_bytes)

        minio = MagicMock()
        minio.upload_bytes.return_value = MagicMock()
        extractor = _make_extractor(minio)

        entries, _ = extractor._extract_and_upload(
            local_zip=zip_path,
            snapshot_date=_SNAPSHOT,
            source_files_bucket="cricket-source-files",
        )

        assert len(entries) == 1
        assert len(entries[0].checksum_sha256) == 64  # SHA-256 hex = 64 chars
        assert entries[0].size_bytes > 0


# ---------------------------------------------------------------------------
# Tests: MatchDataExtractor.extract (idempotency)
# ---------------------------------------------------------------------------


class TestMatchDataExtractorIdempotency:
    def test_skips_if_already_extracted(self):
        minio = MagicMock()
        extractor = _make_extractor(minio)

        existing_path = f"s3://cricket-source-files/match_data/json/snapshot_date={_SNAPSHOT}/"

        with (
            patch.object(extractor, "_check_idempotency", return_value=existing_path),
            patch.object(extractor, "_read_existing_manifest") as mock_manifest,
        ):
            from cip.ingestion.match_data.manifest import ExtractionManifest

            mock_manifest.return_value = ExtractionManifest(
                snapshot_date=_SNAPSHOT,
                archive_file="all_json.zip",
                file_count=100,
                entries=[],
            )
            result = extractor.extract(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                force=False,
            )

        assert result.file_count == 100
        assert result.extracted_prefix == existing_path
        minio.download_file.assert_not_called()

    def test_force_bypasses_idempotency(self):
        minio = MagicMock()
        extractor = _make_extractor(minio)

        existing_path = f"s3://cricket-source-files/match_data/json/snapshot_date={_SNAPSHOT}/"

        with (
            patch.object(extractor, "_check_idempotency", return_value=existing_path),
            patch.object(extractor, "_get_log_id", return_value=10),
            patch.object(extractor, "_update_log_extracted"),
            patch.object(extractor, "_extract_and_upload", return_value=([], [])),
            patch("cip.ingestion.match_data.extract.write_manifest"),
            patch.object(
                extractor, "_get_storage_cfg", return_value=MagicMock(bucket_source_files="cricket-source-files")
            ),
        ):
            extractor.extract(
                snapshot_date=_SNAPSHOT,
                pipeline_run_id=_RUN_ID,
                force=True,
            )

        minio.download_file.assert_called_once()
