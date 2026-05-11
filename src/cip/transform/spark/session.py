from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.logging import get_logger
from cip.common.settings import get_settings

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


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
