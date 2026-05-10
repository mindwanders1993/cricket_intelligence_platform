# platform/ingestion/io/filesystem.py
#
# Local filesystem utilities for the Cricket Intelligence Platform.
#
# Used by ingestion jobs to manage temporary files during the
# download → extract → upload → cleanup lifecycle.
#
# Usage:
#   from platform.ingestion.io.filesystem import TempWorkspace, extract_zip
#   with TempWorkspace(prefix="ingest_cricsheet") as ws:
#       zip_path = ws.path / "all_matches.zip"
#       extract_zip(zip_path, ws.extract_dir)

from __future__ import annotations

import hashlib
import shutil
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from platform.common.exceptions import ExtractionError
from platform.common.logging import get_logger

logger = get_logger(__name__)


# ===========================================================================
# Temp workspace — context manager for download/extract lifecycle
# ===========================================================================


@dataclass
class TempWorkspace:
    """
    Manages a temporary directory pair for a single ingestion task:
        workspace/
            downloads/   ← raw zip files land here
            extracted/   ← zip contents extracted here

    Cleans up on __exit__ unless keep=True (useful for debugging).

    Usage:
        with TempWorkspace(prefix="ingest_cricsheet") as ws:
            zip_path = ws.download_dir / "all_matches.zip"
            extract_zip(zip_path, ws.extract_dir)
            # ws cleaned up automatically
    """

    prefix: str = "cricket_platform"
    keep: bool = False  # set True to preserve dir after exit
    _root: Path = field(init=False)
    _download_dir: Path = field(init=False)
    _extract_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        self._root = Path(tempfile.mkdtemp(prefix=f"{self.prefix}_"))
        self._download_dir = self._root / "downloads"
        self._extract_dir = self._root / "extracted"
        self._download_dir.mkdir()
        self._extract_dir.mkdir()
        logger.debug("TempWorkspace created", extra={"root": str(self._root)})

    @property
    def root(self) -> Path:
        return self._root

    @property
    def download_dir(self) -> Path:
        return self._download_dir

    @property
    def extract_dir(self) -> Path:
        return self._extract_dir

    def cleanup(self) -> None:
        if self._root.exists():
            shutil.rmtree(self._root, ignore_errors=True)
            logger.debug("TempWorkspace cleaned up", extra={"root": str(self._root)})

    def __enter__(self) -> "TempWorkspace":
        return self

    def __exit__(self, *_) -> None:
        if not self.keep:
            self.cleanup()


# ===========================================================================
# Archive extraction
# ===========================================================================


@dataclass(frozen=True)
class ExtractionResult:
    """Result of a zip extraction operation."""

    archive_path: Path
    extract_dir: Path
    file_count: int
    total_size_bytes: int
    file_paths: list[Path]
    format_counts: dict[str, int]  # {"json": 410, "yaml": 2}

    @property
    def json_files(self) -> list[Path]:
        return [p for p in self.file_paths if p.suffix.lower() == ".json"]

    @property
    def yaml_files(self) -> list[Path]:
        return [p for p in self.file_paths if p.suffix.lower() in (".yaml", ".yml")]

    @property
    def csv_files(self) -> list[Path]:
        return [p for p in self.file_paths if p.suffix.lower() == ".csv"]


def extract_zip(
    archive_path: Path,
    extract_dir: Path,
    allowed_extensions: set[str] | None = None,
    max_files: int | None = None,
) -> ExtractionResult:
    """
    Extract a zip archive to a target directory.

    Args:
        archive_path:        Path to the zip file
        extract_dir:         Directory to extract into
        allowed_extensions:  If set, only extract files with these extensions
                             (e.g. {".json", ".yaml"})
        max_files:           Safety cap on extracted file count

    Returns:
        ExtractionResult with file list and format summary

    Raises:
        ExtractionError on bad zip or missing archive
    """
    if not archive_path.exists():
        raise ExtractionError(str(archive_path), reason="Archive file not found")

    if not zipfile.is_zipfile(archive_path):
        raise ExtractionError(str(archive_path), reason="File is not a valid zip archive")

    extract_dir.mkdir(parents=True, exist_ok=True)
    extracted_paths: list[Path] = []
    total_bytes = 0
    format_counts: dict[str, int] = {}

    logger.info(
        "Extracting archive",
        extra={"archive": str(archive_path), "extract_dir": str(extract_dir)},
    )

    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            members = zf.infolist()

            if max_files and len(members) > max_files:
                raise ExtractionError(
                    str(archive_path),
                    reason=f"Archive has {len(members)} files, exceeds max_files={max_files}",
                )

            for member in members:
                # Skip directories and hidden files
                if member.filename.endswith("/") or Path(member.filename).name.startswith("."):
                    continue

                ext = Path(member.filename).suffix.lower()
                if allowed_extensions and ext not in allowed_extensions:
                    continue

                # Prevent zip-slip attacks
                target_path = (extract_dir / Path(member.filename).name).resolve()
                if not str(target_path).startswith(str(extract_dir.resolve())):
                    logger.warning(
                        "Skipping zip-slip candidate",
                        extra={"member": member.filename},
                    )
                    continue

                zf.extract(member, extract_dir)
                full_path = extract_dir / member.filename

                # Flatten nested dirs — move file to extract_dir root
                if full_path.parent != extract_dir:
                    flat_path = extract_dir / full_path.name
                    full_path.rename(flat_path)
                    full_path = flat_path

                extracted_paths.append(full_path)
                total_bytes += member.file_size
                format_counts[ext] = format_counts.get(ext, 0) + 1

    except zipfile.BadZipFile as exc:
        raise ExtractionError(str(archive_path), reason=f"Corrupt zip: {exc}") from exc

    result = ExtractionResult(
        archive_path=archive_path,
        extract_dir=extract_dir,
        file_count=len(extracted_paths),
        total_size_bytes=total_bytes,
        file_paths=extracted_paths,
        format_counts=format_counts,
    )

    logger.info(
        "Extraction complete",
        extra={
            "archive": archive_path.name,
            "file_count": result.file_count,
            "total_size_bytes": total_bytes,
            "format_counts": format_counts,
        },
    )
    return result


# ===========================================================================
# Checksum utilities
# ===========================================================================


def sha256_file(path: Path, chunk_size: int = 65536) -> str:
    """Compute SHA-256 of a local file. Uses 64 KB chunks for memory efficiency."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def verify_checksum(path: Path, expected_sha256: str) -> bool:
    """
    Verify a file's SHA-256 matches the expected value.
    Returns True on match, False on mismatch.
    Does NOT raise — caller decides whether to raise ChecksumMismatchError.
    """
    actual = sha256_file(path)
    match = actual.lower() == expected_sha256.lower()
    if not match:
        logger.warning(
            "Checksum mismatch",
            extra={
                "file": str(path),
                "expected": expected_sha256,
                "actual": actual,
            },
        )
    return match


def safe_file_size(path: Path) -> int:
    """Return file size in bytes, 0 if file does not exist."""
    return path.stat().st_size if path.exists() else 0


def ensure_dir(path: Path) -> Path:
    """Create directory (and parents) if it does not exist. Returns the path."""
    path.mkdir(parents=True, exist_ok=True)
    return path
