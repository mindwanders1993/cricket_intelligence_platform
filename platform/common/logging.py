# platform/common/logging.py
#
# Structured JSON logging for the Cricket Intelligence Platform.
#
# Design goals:
#   1. Every log line is valid JSON — parseable by Loki, CloudWatch, Datadog.
#   2. Correlation fields (pipeline_name, dag_run_id, run_id, layer) are
#      injected via contextvars — set once per task, flows through all callees.
#   3. A human-readable console formatter is used in dev; JSON in prod.
#   4. No global state mutation — callers use get_logger() and bind_context().
#
# Usage:
#   from platform.common.logging import get_logger, bind_context
#
#   bind_context(pipeline_name="ingest_cricsheet_archives", dag_run_id="run_123")
#   logger = get_logger(__name__)
#   logger.info("Download started", extra={"source_url": url, "file_size": size})

from __future__ import annotations

import json
import logging
import sys
import traceback
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Correlation context — set once at task/job entry point, inherited by callees
# ---------------------------------------------------------------------------

_CTX_PIPELINE_NAME: ContextVar[str] = ContextVar("pipeline_name", default="")
_CTX_DAG_RUN_ID: ContextVar[str] = ContextVar("dag_run_id", default="")
_CTX_RUN_ID: ContextVar[str] = ContextVar("run_id", default="")
_CTX_LAYER: ContextVar[str] = ContextVar("layer", default="")
_CTX_TASK_ID: ContextVar[str] = ContextVar("task_id", default="")


def bind_context(
    *,
    pipeline_name: str = "",
    dag_run_id: str = "",
    run_id: str = "",
    layer: str = "",
    task_id: str = "",
) -> None:
    """
    Bind correlation fields to the current execution context.
    Call once at the entry point of an Airflow task, Spark job, or CLI run.
    All subsequent get_logger() calls in the same thread/async context
    will automatically include these fields.

    Example:
        bind_context(
            pipeline_name="ingest_cricsheet_archives",
            dag_run_id="scheduled__2024-11-01T00:00:00+00:00",
            run_id=str(uuid4()),
            layer="landing",
        )
    """
    if pipeline_name:
        _CTX_PIPELINE_NAME.set(pipeline_name)
    if dag_run_id:
        _CTX_DAG_RUN_ID.set(dag_run_id)
    if run_id:
        _CTX_RUN_ID.set(run_id)
    if layer:
        _CTX_LAYER.set(layer)
    if task_id:
        _CTX_TASK_ID.set(task_id)


def get_context() -> dict[str, str]:
    """Return current correlation context as a plain dict."""
    return {
        "pipeline_name": _CTX_PIPELINE_NAME.get(),
        "dag_run_id": _CTX_DAG_RUN_ID.get(),
        "run_id": _CTX_RUN_ID.get(),
        "layer": _CTX_LAYER.get(),
        "task_id": _CTX_TASK_ID.get(),
    }


def clear_context() -> None:
    """Reset all correlation fields. Used in tests between cases."""
    _CTX_PIPELINE_NAME.set("")
    _CTX_DAG_RUN_ID.set("")
    _CTX_RUN_ID.set("")
    _CTX_LAYER.set("")
    _CTX_TASK_ID.set("")


# ===========================================================================
# JSON formatter — prod + CI
# ===========================================================================


class JsonFormatter(logging.Formatter):
    """
    Emits one JSON object per log line.

    Standard fields every line carries:
        timestamp, level, logger, message, pipeline_name,
        dag_run_id, run_id, layer, task_id

    Extra fields passed via logger.info("msg", extra={...}) are merged in.
    Exceptions are serialised as a structured 'exception' object.
    """

    # Fields that logging injects into LogRecord but we don't want at root level
    _SKIP_ATTRS = frozenset(
        {
            "args",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "taskName",
            "thread",
            "threadName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        # Base correlation context from contextvars
        ctx = get_context()

        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            # Correlation
            "pipeline_name": ctx["pipeline_name"] or record.__dict__.get("pipeline_name", ""),
            "dag_run_id": ctx["dag_run_id"] or record.__dict__.get("dag_run_id", ""),
            "run_id": ctx["run_id"] or record.__dict__.get("run_id", ""),
            "layer": ctx["layer"] or record.__dict__.get("layer", ""),
            "task_id": ctx["task_id"] or record.__dict__.get("task_id", ""),
            # Source location (useful for debugging)
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }

        # Merge any extra={} fields passed by the caller
        for key, value in record.__dict__.items():
            if key not in self._SKIP_ATTRS and not key.startswith("_"):
                if key not in payload:
                    payload[key] = value

        # Structured exception block
        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value),
                "traceback": traceback.format_tb(exc_tb),
            }

        # Strip None and empty-string values to keep lines lean
        payload = {k: v for k, v in payload.items() if v is not None and v != ""}

        return json.dumps(payload, default=str)


# ===========================================================================
# Human-readable formatter — local dev
# ===========================================================================


class DevFormatter(logging.Formatter):
    """
    Coloured, human-readable output for local development.
    Format: TIMESTAMP LEVEL     [pipeline_name] logger: message  {extra}
    """

    _LEVEL_COLOURS = {
        "DEBUG": "\033[0;36m",  # cyan
        "INFO": "\033[0;32m",  # green
        "WARNING": "\033[1;33m",  # yellow
        "ERROR": "\033[0;31m",  # red
        "CRITICAL": "\033[1;31m",  # bright red
    }
    _RESET = "\033[0m"

    _SKIP_ATTRS = JsonFormatter._SKIP_ATTRS | {
        "pipeline_name",
        "dag_run_id",
        "run_id",
        "layer",
        "task_id",
        "color_message",
    }

    def format(self, record: logging.LogRecord) -> str:
        ctx = get_context()
        colour = self._LEVEL_COLOURS.get(record.levelname, "")
        reset = self._RESET

        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%H:%M:%S")
        level = f"{colour}{record.levelname:<8}{reset}"
        pipeline = ctx["pipeline_name"] or record.__dict__.get("pipeline_name", "")
        context_tag = f"[{pipeline}] " if pipeline else ""

        # Collect extra fields for inline display
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in self._SKIP_ATTRS and not k.startswith("_") and v is not None
        }
        extras_str = f"  {extras}" if extras else ""

        base = f"{ts} {level} {context_tag}{record.name}: {record.getMessage()}{extras_str}"

        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)

        return base


# ===========================================================================
# Logger factory
# ===========================================================================

_CONFIGURED_LOGGERS: set[str] = set()
_ROOT_HANDLER_INSTALLED = False


def _install_root_handler(log_level: str, use_json: bool) -> None:
    """Install a single root handler if not already done. Idempotent."""
    global _ROOT_HANDLER_INSTALLED
    if _ROOT_HANDLER_INSTALLED:
        return

    root = logging.getLogger()
    root.setLevel(log_level)

    # Remove any default handlers (e.g. from basicConfig or Airflow)
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    handler.setFormatter(JsonFormatter() if use_json else DevFormatter())
    root.addHandler(handler)

    # Suppress noisy third-party loggers
    for noisy in (
        "urllib3",
        "boto3",
        "botocore",
        "s3transfer",
        "py4j",
        "pyspark",
        "fsspec",
        "aiobotocore",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _ROOT_HANDLER_INSTALLED = True


def get_logger(name: str, *, log_level: str | None = None) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    Reads log_level and env_name from PlatformSettings on first call.
    Subsequent calls return the cached logger without re-reading settings.

    Usage:
        logger = get_logger(__name__)
        logger.info("Ingestion started", extra={"file_count": 42})
        logger.warning("Checksum mismatch", extra={"expected": "abc", "got": "xyz"})
        logger.error("Download failed", exc_info=True)

    Args:
        name:       Module name — pass __name__ always.
        log_level:  Override log level for this specific logger only.
    """
    global _ROOT_HANDLER_INSTALLED

    if not _ROOT_HANDLER_INSTALLED:
        try:
            from platform.common.settings import get_settings

            cfg = get_settings()
            effective_level = log_level or cfg.log_level
            use_json = cfg.is_prod()
        except Exception:
            # Settings not yet available (bootstrap, early init) — safe fallback
            effective_level = log_level or "INFO"
            use_json = False

        _install_root_handler(effective_level, use_json)

    logger = logging.getLogger(name)
    if log_level:
        logger.setLevel(log_level)

    return logger


# ===========================================================================
# Convenience: context manager for scoped correlation binding
# ===========================================================================


class LogContext:
    """
    Context manager that binds correlation fields for the duration of a block
    and restores the previous values on exit.

    Usage:
        with LogContext(pipeline_name="ingest_cricsheet_archives", layer="landing"):
            logger.info("Inside task — correlation fields are set automatically")
        # Previous context restored here
    """

    def __init__(self, **kwargs: str) -> None:
        self._new = kwargs
        self._tokens: dict[str, Any] = {}
        self._var_map = {
            "pipeline_name": _CTX_PIPELINE_NAME,
            "dag_run_id": _CTX_DAG_RUN_ID,
            "run_id": _CTX_RUN_ID,
            "layer": _CTX_LAYER,
            "task_id": _CTX_TASK_ID,
        }

    def __enter__(self) -> "LogContext":
        for key, value in self._new.items():
            if key in self._var_map:
                self._tokens[key] = self._var_map[key].set(value)
        return self

    def __exit__(self, *_: Any) -> None:
        for key, token in self._tokens.items():
            self._var_map[key].reset(token)
