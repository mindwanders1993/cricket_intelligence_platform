# src/cip/transform/spark/silver/schema.py
#
# PySpark StructType for parsing Bronze `raw_json` into typed nested fields.
# Used by every Match Silver transform that needs to read into innings/
# deliveries/wickets/etc.
#
# Mirrors docs/silver_match_spec/spec.md Section 1.

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Cricsheet match JSON schema
# ---------------------------------------------------------------------------
#
# Notes:
#   - `info.season` is declared StringType.  Cricsheet emits string OR int
#     ("2007", "2011/12", or 2007).  We prefer Bronze's pre-stringified
#     `season` column for safety; this declaration is only used when callers
#     explicitly accept `null` for mismatched ints.
#   - `info.players` is MapType<String, Array<String>> — keys are team names.
#   - `info.registry.people` is MapType<String, String> — keys are display
#     names, values are Cricsheet identifiers.

MATCH_JSON_SCHEMA: StructType = StructType(
    [
        StructField(
            "meta",
            StructType(
                [
                    StructField("data_version", StringType(), True),
                    StructField("created", StringType(), True),
                    StructField("revision", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "info",
            StructType(
                [
                    StructField("balls_per_over", IntegerType(), True),
                    StructField("city", StringType(), True),
                    StructField("dates", ArrayType(StringType()), True),
                    StructField(
                        "event",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("match_number", StringType(), True),
                                StructField("group", StringType(), True),
                                StructField("stage", StringType(), True),
                                StructField("sub_name", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("gender", StringType(), True),
                    StructField("match_type", StringType(), True),
                    StructField("match_type_number", IntegerType(), True),
                    StructField("missing", ArrayType(StringType()), True),
                    StructField(
                        "officials",
                        StructType(
                            [
                                StructField("match_referees", ArrayType(StringType()), True),
                                StructField("reserve_umpires", ArrayType(StringType()), True),
                                StructField("tv_umpires", ArrayType(StringType()), True),
                                StructField("umpires", ArrayType(StringType()), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "outcome",
                        StructType(
                            [
                                StructField(
                                    "by",
                                    StructType(
                                        [
                                            StructField("innings", IntegerType(), True),
                                            StructField("runs", IntegerType(), True),
                                            StructField("wickets", IntegerType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                                StructField("winner", StringType(), True),
                                StructField("result", StringType(), True),
                                StructField("method", StringType(), True),
                                StructField("eliminator", StringType(), True),
                                StructField("bowl_out", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("overs", IntegerType(), True),
                    StructField("player_of_match", ArrayType(StringType()), True),
                    StructField("players", MapType(StringType(), ArrayType(StringType())), True),
                    StructField(
                        "registry",
                        StructType(
                            [
                                StructField("people", MapType(StringType(), StringType()), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("season", StringType(), True),
                    StructField("supersubs", MapType(StringType(), StringType()), True),
                    StructField("teams", ArrayType(StringType()), True),
                    StructField("team_type", StringType(), True),
                    StructField(
                        "toss",
                        StructType(
                            [
                                StructField("decision", StringType(), True),
                                StructField("winner", StringType(), True),
                                StructField("uncontested", BooleanType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("venue", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "innings",
            ArrayType(
                StructType(
                    [
                        StructField("team", StringType(), True),
                        StructField(
                            "overs",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("over", IntegerType(), True),
                                        StructField(
                                            "deliveries",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField("batter", StringType(), True),
                                                        StructField("bowler", StringType(), True),
                                                        StructField("non_striker", StringType(), True),
                                                        StructField(
                                                            "runs",
                                                            StructType(
                                                                [
                                                                    StructField("batter", IntegerType(), True),
                                                                    StructField("extras", IntegerType(), True),
                                                                    StructField("total", IntegerType(), True),
                                                                    StructField("non_boundary", IntegerType(), True),
                                                                ]
                                                            ),
                                                            True,
                                                        ),
                                                        StructField(
                                                            "extras",
                                                            StructType(
                                                                [
                                                                    StructField("wides", IntegerType(), True),
                                                                    StructField("noballs", IntegerType(), True),
                                                                    StructField("byes", IntegerType(), True),
                                                                    StructField("legbyes", IntegerType(), True),
                                                                    StructField("penalty", IntegerType(), True),
                                                                ]
                                                            ),
                                                            True,
                                                        ),
                                                        StructField(
                                                            "wickets",
                                                            ArrayType(
                                                                StructType(
                                                                    [
                                                                        StructField("player_out", StringType(), True),
                                                                        StructField("kind", StringType(), True),
                                                                        StructField(
                                                                            "fielders",
                                                                            ArrayType(
                                                                                StructType(
                                                                                    [
                                                                                        StructField(
                                                                                            "name", StringType(), True
                                                                                        ),
                                                                                        StructField(
                                                                                            "substitute",
                                                                                            BooleanType(),
                                                                                            True,
                                                                                        ),
                                                                                    ]
                                                                                )
                                                                            ),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                )
                                                            ),
                                                            True,
                                                        ),
                                                    ]
                                                )
                                            ),
                                            True,
                                        ),
                                    ]
                                )
                            ),
                            True,
                        ),
                        StructField("absent_hurt", ArrayType(StringType()), True),
                        StructField(
                            "penalty_runs",
                            StructType(
                                [
                                    StructField("pre", IntegerType(), True),
                                    StructField("post", IntegerType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("declared", BooleanType(), True),
                        StructField("forfeited", BooleanType(), True),
                        StructField(
                            "target",
                            StructType(
                                [
                                    StructField("overs", DoubleType(), True),
                                    StructField("runs", IntegerType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("super_over", BooleanType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)
