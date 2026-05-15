# src/cip/ingestion/match_data/manifest.py
#
# Extraction manifest — written alongside extracted JSON files.
#
# After MatchDataExtractor uploads all JSON files for a snapshot, it writes
# a _manifest.json object at:
#   match_data/json/snapshot_date={date}/_manifest.json
#
# MatchBronzeLoader and MAT-BRZ-003 DQ check read the manifest to verify
# that the number of Bronze rows matches the number of extracted files.

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cip.ingestion.io.minio import MinIOClient


@dataclass(frozen=True)
class ManifestEntry:
    file_name: str
    size_bytes: int
    checksum_sha256: str


@dataclass
class ExtractionManifest:
    snapshot_date: str
    archive_file: str
    file_count: int
    entries: list[ManifestEntry]

    def to_json(self) -> str:
        return json.dumps(
            {
                "snapshot_date": self.snapshot_date,
                "archive_file": self.archive_file,
                "file_count": self.file_count,
                "entries": [asdict(e) for e in self.entries],
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, data: str | bytes) -> "ExtractionManifest":
        raw = json.loads(data)
        return cls(
            snapshot_date=raw["snapshot_date"],
            archive_file=raw["archive_file"],
            file_count=raw["file_count"],
            entries=[ManifestEntry(**e) for e in raw["entries"]],
        )


def manifest_object_key(snapshot_date: str) -> str:
    return f"match_data/json/snapshot_date={snapshot_date}/_manifest.json"


def write_manifest(minio: "MinIOClient", manifest: ExtractionManifest) -> None:
    from cip.common.settings import get_settings

    cfg = get_settings().storage
    key = manifest_object_key(manifest.snapshot_date)
    minio.upload_bytes(
        data=manifest.to_json().encode("utf-8"),
        bucket=cfg.bucket_source_files,
        key=key,
        content_type="application/json",
    )


def read_manifest(minio: "MinIOClient", snapshot_date: str) -> ExtractionManifest:
    from cip.common.settings import get_settings

    cfg = get_settings().storage
    key = manifest_object_key(snapshot_date)
    data = minio.read_bytes(cfg.bucket_source_files, key)
    return ExtractionManifest.from_json(data)
