# src/cip/transform/spark/silver/normalize.py
#
# Pure-Spark normalization helpers shared across Match Silver transforms.
#
# Edge cases handled here:
#   - `season` polymorphism: Cricsheet emits string "2011/12", string "2026",
#     or integer 2007 — we coerce everything to string at Silver.

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column


def season_to_string(col: "Column") -> "Column":
    """
    Coerce a season value (int or string) to its canonical string form.

    Cricsheet emits any of:
        - "2011/12"  (split-year string)
        - "2026"     (single-year string)
        - 2007       (integer)

    Strategy: cast to string.  Spark cast of int → string drops the
    trailing `.0` of any accidentally-double-typed value.
    """
    from pyspark.sql import functions as F

    return F.when(col.isNull(), F.lit(None).cast("string")).otherwise(col.cast("string"))
