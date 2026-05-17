"""Structural tests for MatchRegistrySilverTransform.

End-to-end correctness is verified by running build_silver_match_data
against a real Bronze snapshot — pyspark.sql.functions.col needs an
active SparkContext that pure mocks can't provide.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from cip.transform.spark.silver.match_registry import _SILVER_MATCH_REGISTRY, MatchRegistrySilverTransform


class TestSilverTarget:
    def test_target_fqn(self):
        assert _SILVER_MATCH_REGISTRY == "silver.match_registry"


class TestInstantiation:
    def test_constructor_accepts_spark_and_writer(self):
        spark = MagicMock()
        writer = MagicMock()
        transform = MatchRegistrySilverTransform(spark=spark, writer=writer)
        assert transform._spark is spark
        assert transform._writer is writer
