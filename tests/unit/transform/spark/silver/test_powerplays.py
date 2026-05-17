"""Structural tests for MatchPowerplaysSilverTransform.

End-to-end correctness for Spark match transforms is verified by running
the build_silver_match_data job against a real Bronze snapshot — pure
mocks can't exercise pyspark.sql.functions.col without an active
SparkContext. This file mirrors that convention and only asserts the
contract surface (target FQN, instantiation, pipeline wiring).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from cip.transform.spark.silver.powerplays import _SILVER_POWERPLAYS, MatchPowerplaysSilverTransform


class TestSilverTarget:
    def test_target_fqn(self):
        assert _SILVER_POWERPLAYS == "silver.match_powerplays"


class TestInstantiation:
    def test_constructor_accepts_spark_and_writer(self):
        spark = MagicMock()
        writer = MagicMock()
        transform = MatchPowerplaysSilverTransform(spark=spark, writer=writer)
        assert transform._spark is spark
        assert transform._writer is writer
