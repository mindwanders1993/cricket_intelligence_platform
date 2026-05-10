# platform/common/contracts/enums.py
#
# Platform-wide enumerations for the Cricket Intelligence Platform.
#
# Rules:
#   1. No magic strings in pipeline code — always use these enums.
#   2. All enums are StrEnum (Python 3.11+) so they compare equal to
#      their string values and serialise cleanly to JSON / YAML.
#   3. Add new values here before using them anywhere else.
#
# Usage:
#   from cip.common.contracts.enums import Layer, MatchType, IngestionStatus
#   if layer == Layer.BRONZE:
#       ...

from __future__ import annotations

from enum import StrEnum

# ===========================================================================
# Data layer / medallion
# ===========================================================================


class Layer(StrEnum):
    """Medallion architecture layers."""

    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    SERVING = "serving"


# ===========================================================================
# Ingestion / pipeline lifecycle
# ===========================================================================


class IngestionStatus(StrEnum):
    """Status values for ingestion_run and register_sync_log rows."""

    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"  # file already ingested, dedup applied
    PARTIAL = "PARTIAL"  # some files succeeded, some failed


class ArchiveType(StrEnum):
    """
    Cricsheet archive categories from the downloads page.
    Maps directly to ingestion_run.archive_type.
    """

    ALL_MATCHES = "all_matches"  # the full all-format zip
    COMPETITION = "competition"  # competition-specific zip (e.g. ipl)
    MATCH_TYPE = "match_type"  # format-specific zip (e.g. t20s, odis)
    REGISTER = "register"  # Cricsheet Register CSV files


class FileFormat(StrEnum):
    """Source file format inside a Cricsheet archive."""

    JSON = "json"
    YAML = "yaml"
    CSV = "csv"
    XML = "xml"  # legacy, rarely used


# ===========================================================================
# Cricket match taxonomy
# ===========================================================================


class MatchType(StrEnum):
    """
    Cricsheet match types — used in file paths, partitions, and filters.
    Matches the 'match_type' field inside JSON info block.
    """

    TEST = "Test"
    ODI = "ODI"
    T20 = "T20"
    IT20 = "IT20"  # International T20 (distinct from franchise T20)
    MDM = "MDM"  # Multi-Day Match (domestic first-class)
    OTH = "OTH"  # Other / unknown


class Gender(StrEnum):
    """Match gender category."""

    MALE = "male"
    FEMALE = "female"


class InningsType(StrEnum):
    """Innings role in a match."""

    FIRST = "1st innings"
    SECOND = "2nd innings"
    THIRD = "3rd innings"  # Test matches
    FOURTH = "4th innings"  # Test matches
    SUPER_OVER = "super over"


class BattingPosition(StrEnum):
    """
    Phase-of-play classification for delivery analysis.
    T20 phasing — ODI equivalent mapped separately in dbt.
    """

    POWERPLAY = "powerplay"  # overs 1–6
    MIDDLE = "middle"  # overs 7–15
    DEATH = "death"  # overs 16–20


class WicketKind(StrEnum):
    """
    Dismissal types as they appear in Cricsheet JSON.
    Values match the 'kind' field in deliveries[].wickets[].
    """

    BOWLED = "bowled"
    CAUGHT = "caught"
    CAUGHT_AND_BOWLED = "caught and bowled"
    LBW = "lbw"
    RUN_OUT = "run out"
    STUMPED = "stumped"
    HIT_WICKET = "hit wicket"
    HANDLED_BALL = "handled the ball"
    OBSTRUCTING_FIELD = "obstructing the field"
    TIMED_OUT = "timed out"
    HIT_BALL_TWICE = "hit the ball twice"
    RETIRED_HURT = "retired hurt"
    RETIRED_OUT = "retired out"


class ExtraType(StrEnum):
    """Extra types as they appear in Cricsheet JSON deliveries[].extras."""

    WIDES = "wides"
    NOBALLS = "noballs"
    BYES = "byes"
    LEGBYES = "legbyes"
    PENALTY = "penalty"


class TossDecision(StrEnum):
    """Toss decision values from Cricsheet JSON info.toss.decision."""

    BAT = "bat"
    FIELD = "field"


class MatchResult(StrEnum):
    """
    Match outcome type.
    Derived from Cricsheet JSON info.outcome.
    """

    WIN = "win"
    TIE = "tie"
    DRAW = "draw"
    NO_RESULT = "no result"
    UNKNOWN = "unknown"


class WinByMethod(StrEnum):
    """How a win margin is expressed."""

    RUNS = "runs"
    WICKETS = "wickets"
    INNINGS = "innings"  # Test only


class OfficialRole(StrEnum):
    """
    Match official roles as they appear in Cricsheet JSON info.officials.
    Values match the key names in the officials dict.
    """

    UMPIRE = "umpires"
    TV_UMPIRE = "tv_umpires"
    RESERVE_UMPIRE = "reserve_umpires"
    MATCH_REFEREE = "match_referees"


# ===========================================================================
# Data quality
# ===========================================================================


class DQSeverity(StrEnum):
    """Severity levels for individual DQ checks."""

    ERROR = "ERROR"  # blocks layer promotion
    WARNING = "WARNING"  # logged, does not block
    INFO = "INFO"  # informational metric only


class DQCheckType(StrEnum):
    """Categories of DQ checks used in dq_result.check_type."""

    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    ACCEPTED = "accepted_values"
    REFERENTIAL = "referential_integrity"
    COMPLETENESS = "completeness"
    FRESHNESS = "freshness"
    RECONCILIATION = "reconciliation"  # cross-source totals match
    CUSTOM = "custom"


class DQStatus(StrEnum):
    """Result status of an individual DQ check."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"


# ===========================================================================
# Schema evolution
# ===========================================================================


class SchemaChangeType(StrEnum):
    """Types of Iceberg schema changes tracked in schema_version."""

    INITIAL = "INITIAL"
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    TYPE_CHANGE = "TYPE_CHANGE"
    RENAME = "RENAME"
    NO_CHANGE = "NO_CHANGE"


# ===========================================================================
# Pipeline / watermark
# ===========================================================================


class WatermarkType(StrEnum):
    """How a pipeline watermark value is interpreted."""

    SNAPSHOT_DATE = "snapshot_date"
    FILE_COUNT = "file_count"
    RUN_ID = "run_id"


# ===========================================================================
# Serving / AI
# ===========================================================================


class AIQueryMode(StrEnum):
    """How the AI assistant resolves a user question."""

    TEMPLATE = "template"  # matched to a pre-approved SQL template
    SEMANTIC = "semantic"  # resolved via semantic layer mapping
    FREEFORM = "freeform"  # full text-to-SQL (guarded, prod-gated)


class MetadataTag(StrEnum):
    """
    Tags applied to Gold mart columns for AI semantic layer registration.
    Used in platform/serving/ai/semantic_layer/ to guide LLM context.
    """

    METRIC = "metric"  # a numeric measure (runs, wickets, economy)
    DIMENSION = "dimension"  # a grouping attribute (team, venue, season)
    FILTER = "filter"  # commonly used as a WHERE predicate
    IDENTIFIER = "identifier"  # a key or ID column
    TEMPORAL = "temporal"  # date or time column
    DERIVED = "derived"  # calculated column, not raw source
