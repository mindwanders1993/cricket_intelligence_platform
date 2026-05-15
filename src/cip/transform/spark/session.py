from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from cip.common.logging import get_logger
from cip.common.settings import get_settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


def _sanitize_spark_env() -> None:
    """
    Defensive: if SPARK_HOME / JAVA_HOME inherited from the shell point at
    paths that don't exist, repair them in-process before launching Spark.

    Why: the project ships pyspark as a poetry dependency, so a working
    Spark distribution is always present at the pyspark bundled location.
    Meanwhile a shell `~/.zshrc` may export SPARK_HOME at a stale Homebrew
    path. The shell env is outside our control, but the JVM gateway respects
    `os.environ`, so we can correct it for *this process* without touching
    the user's dotfiles.

    Behaviour:
        SPARK_HOME → if invalid, delete the env var so pyspark falls back
                     to its bundled distribution (pyspark.find_spark_home).
        JAVA_HOME  → if invalid on macOS, re-derive from
                     `/usr/libexec/java_home`. If we can't, delete it so
                     spark-submit falls back to whatever `java` is on PATH.

    A no-op when the inherited env is already correct (Docker / CI).
    """
    spark_home = os.environ.get("SPARK_HOME")
    if spark_home and not (Path(spark_home) / "bin" / "spark-submit").exists():
        logger.warning(
            "SPARK_HOME points at a missing path — using pyspark's bundled " "Spark distribution instead",
            extra={"stale_spark_home": spark_home},
        )
        os.environ.pop("SPARK_HOME", None)

    java_home = os.environ.get("JAVA_HOME")
    if java_home and not (Path(java_home) / "bin" / "java").exists():
        replacement: str | None = None
        java_home_helper = Path("/usr/libexec/java_home")
        if java_home_helper.exists():
            try:
                out = subprocess.check_output([str(java_home_helper)], text=True).strip()
                if out and (Path(out) / "bin" / "java").exists():
                    replacement = out
            except subprocess.CalledProcessError:
                pass

        if replacement:
            logger.warning(
                "JAVA_HOME points at a missing path — re-deriving from /usr/libexec/java_home",
                extra={"stale_java_home": java_home, "replacement": replacement},
            )
            os.environ["JAVA_HOME"] = replacement
        else:
            logger.warning(
                "JAVA_HOME points at a missing path and no replacement found — " "unsetting and falling back to PATH",
                extra={"stale_java_home": java_home},
            )
            os.environ.pop("JAVA_HOME", None)


def get_or_create_spark(app_name_suffix: str = "cip") -> "SparkSession":
    """
    Return a SparkSession configured for the Iceberg REST catalog + MinIO.

    Reads all connection details from PlatformSettings so the same code
    works in dev (Docker) and prod without changes.

    Args:
        app_name_suffix: Appended to "cip-" to form the Spark app name.
                         Use a descriptive suffix so jobs are identifiable
                         in the Spark UI (e.g. "silver-register").

    Returns:
        Active SparkSession with Iceberg + S3A configuration applied.
    """
    _sanitize_spark_env()

    from pyspark.sql import SparkSession

    from cip.transform.shared.readers import _build_spark_iceberg_conf

    cfg = get_settings()
    app_name = f"cip-{app_name_suffix}"

    conf = _build_spark_iceberg_conf()
    conf["spark.app.name"] = app_name
    conf["spark.master"] = cfg.spark.master

    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    logger.info("SparkSession ready", extra={"app_name": app_name, "master": cfg.spark.master})
    return spark
