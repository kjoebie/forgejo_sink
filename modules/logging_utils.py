"""
Logging utilities with rotating file handler support.

Provides:
- File and console logging configuration
- Bronze and Silver processing log management
- Query helpers for log retrieval and analysis
"""

import logging
import os
import json
from datetime import datetime, date
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, List, Dict, Any
from uuid import uuid4
from pyspark.sql import DataFrame, SparkSession, Row, functions as F


DEFAULT_CLUSTER_FILES_ROOT = "/data/lakehouse/gh_b_avd/lh_gh_bronze/Files"


@lru_cache(maxsize=1)
def _resolve_log_directory() -> Path:
    """Determine the log directory based on the runtime environment.

    Uses an in-memory cache to avoid repeated filesystem checks during imports.
    """

    # Prefer explicit environment override
    env_override = os.getenv("NOTEBOOK_LOG_ROOT")
    if env_override:
        return Path(env_override) / "notebook_outputs" / "logs"

    # Fabric notebooks
    if os.path.exists("/lakehouse/default"):
        return Path("/lakehouse/default/Files/notebook_outputs/logs")

    # Cluster (OneLake mounted under /data)
    if os.path.exists("/data/lakehouse"):
        return Path(DEFAULT_CLUSTER_FILES_ROOT) / "notebook_outputs" / "logs"

    # Local execution
    return Path("notebook_outputs/logs")


def configure_logging(
    run_name: Optional[str] = None,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
    enable_console_logging: bool = True,
    log_level: int = logging.INFO,
    formatter: Optional[logging.Formatter] = None,
) -> Path:
    """Configure root logging with a rotating file handler.

    The log directory can be overridden with the ``NOTEBOOK_LOG_ROOT`` environment
    variable. If unset, it defaults to the Fabric lakehouse location when
    available, then the cluster path under ``/data/lakehouse`` or a local
    ``notebook_outputs/logs`` directory.

    Args:
        run_name: Optional name prefix for the log file.
        max_bytes: Maximum log file size before rotation occurs.
        backup_count: Number of rotated log files to keep.
        enable_console_logging: Whether to log to stdout in addition to the file.
        log_level: Logging level to apply to the root logger.
        formatter: Optional custom formatter; defaults to timestamp/level/message.

    Returns:
        Path: Full path to the log file for the current run.
    """
    formatter = formatter or logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s")

    if getattr(configure_logging, "_configured", False):
        # Add console logging on demand if it was disabled initially
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        if enable_console_logging and not _has_console_handler(root_logger):
            _add_console_handler(root_logger, formatter)

        return configure_logging._log_file  # type: ignore[attr-defined]

    log_dir = _resolve_log_directory()
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{run_name or 'notebook_run'}_{timestamp}.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    _ensure_file_handler(root_logger, log_file, formatter, max_bytes, backup_count)

    if enable_console_logging and not _has_console_handler(root_logger):
        _add_console_handler(root_logger, formatter)

    configure_logging._configured = True  # type: ignore[attr-defined]
    configure_logging._log_file = log_file  # type: ignore[attr-defined]

    return log_file


def _has_console_handler(root_logger: logging.Logger) -> bool:
    return any(
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, RotatingFileHandler)
        for handler in root_logger.handlers
    )


def _add_console_handler(root_logger: logging.Logger, formatter: logging.Formatter) -> None:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


def _ensure_file_handler(
    root_logger: logging.Logger,
    log_file: Path,
    formatter: logging.Formatter,
    max_bytes: int,
    backup_count: int,
) -> None:
    existing_handlers = [h for h in root_logger.handlers if isinstance(h, RotatingFileHandler)]
    if existing_handlers:
        existing_handlers[0].setFormatter(formatter)
        return

    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


# =============================================================================
# Data Pipeline Logging Functions
# =============================================================================

# Table names
BRONZE_LOG_TABLE = "logs.bronze_processing_log"
BRONZE_SUMMARY_TABLE = "logs.bronze_run_summary"
SILVER_LOG_TABLE = "logs.silver_processing_log"
SILVER_SUMMARY_TABLE = "logs.silver_run_summary"


def build_run_date(run_ts: str) -> date:
    """
    Convert a run_ts like '20251005T142752505' into a Python date(2025, 10, 5).

    This avoids Spark date parsing issues with ANSI mode.

    Args:
        run_ts: Run timestamp in yyyymmddThhmmss format

    Returns:
        Python date object

    Example:
        >>> run_date = build_run_date("20251105T142752505")
        >>> print(run_date)  # 2025-11-05
    """
    if not run_ts or len(run_ts) < 8:
        raise ValueError(f"run_ts '{run_ts}' is not in expected yyyymmddThhmmss format")

    y = int(run_ts[0:4])
    m = int(run_ts[4:6])
    d = int(run_ts[6:8])
    return date(y, m, d)


def truncate_error_message(error_msg: Optional[str], max_length: int = 1000) -> Optional[str]:
    """
    Truncate error messages to prevent bloating log tables.

    Args:
        error_msg: Error message to truncate
        max_length: Maximum length (default: 1000)

    Returns:
        Truncated error message with indicator if truncated

    Example:
        >>> long_error = "Error: " + "x" * 2000
        >>> truncated = truncate_error_message(long_error, 100)
        >>> len(truncated) <= 120  # 100 + "... [TRUNCATED]"
        True
    """
    if not error_msg:
        return None

    if len(error_msg) <= max_length:
        return error_msg

    return error_msg[:max_length] + "... [TRUNCATED]"


def get_bronze_logs_for_run(spark: SparkSession, run_ts: str) -> DataFrame:
    """
    Get all Bronze processing logs for a specific run_ts.

    Args:
        spark: Active SparkSession
        run_ts: Run timestamp

    Returns:
        DataFrame with Bronze processing logs

    Example:
        >>> logs = get_bronze_logs_for_run(spark, "20251105T142752505")
        >>> logs.show()
    """
    return spark.table(BRONZE_LOG_TABLE).where(F.col("run_ts") == run_ts)


def get_silver_logs_for_run(spark: SparkSession, run_ts: str) -> DataFrame:
    """
    Get all Silver processing logs for a specific run_ts.

    Args:
        spark: Active SparkSession
        run_ts: Run timestamp

    Returns:
        DataFrame with Silver processing logs

    Example:
        >>> logs = get_silver_logs_for_run(spark, "20251105T142752505")
        >>> logs.show()
    """
    return spark.table(SILVER_LOG_TABLE).where(F.col("run_ts") == run_ts)


def get_failed_tables(spark: SparkSession, run_ts: str, layer: str = "bronze") -> List[str]:
    """
    Get list of failed table names for a run_ts.

    Args:
        spark: Active SparkSession
        run_ts: Run timestamp
        layer: "bronze" or "silver"

    Returns:
        List of table names with status='FAILED'

    Example:
        >>> failed = get_failed_tables(spark, "20251105T142752505", "bronze")
        >>> print(f"Failed tables: {failed}")
    """
    table = BRONZE_LOG_TABLE if layer == "bronze" else SILVER_LOG_TABLE

    failed = spark.table(table) \
        .where(f"run_ts = '{run_ts}' AND status = 'FAILED'") \
        .select("table_name") \
        .distinct() \
        .collect()

    return [row.table_name for row in failed]


def get_successful_tables(spark: SparkSession, run_ts: str, layer: str = "bronze") -> List[str]:
    """
    Get list of successful table names for a run_ts.

    Args:
        spark: Active SparkSession
        run_ts: Run timestamp
        layer: "bronze" or "silver"

    Returns:
        List of table names with status='SUCCESS'

    Example:
        >>> success = get_successful_tables(spark, "20251105T142752505", "bronze")
        >>> print(f"Successful tables: {len(success)}")
    """
    table = BRONZE_LOG_TABLE if layer == "bronze" else SILVER_LOG_TABLE

    success = spark.table(table) \
        .where(f"run_ts = '{run_ts}' AND status = 'SUCCESS'") \
        .select("table_name") \
        .distinct() \
        .collect()

    return [row.table_name for row in success]


def is_table_processed(spark: SparkSession, run_ts: str, table_name: str, layer: str = "bronze") -> bool:
    """
    Check if a specific table was successfully processed for a run_ts.

    Args:
        spark: Active SparkSession
        run_ts: Run timestamp
        table_name: Table name to check
        layer: "bronze" or "silver"

    Returns:
        True if table has status='SUCCESS' for this run_ts

    Example:
        >>> if is_table_processed(spark, "20251105T142752505", "Dim_Relatie"):
        ...     print("Table was processed successfully")
    """
    table = BRONZE_LOG_TABLE if layer == "bronze" else SILVER_LOG_TABLE

    count = spark.table(table) \
        .where(f"run_ts = '{run_ts}' AND table_name = '{table_name}' AND status = 'SUCCESS'") \
        .count()

    return count > 0


def get_latest_run_summary(spark: SparkSession, source: str, layer: str = "bronze") -> Optional[Dict[str, Any]]:
    """
    Get the most recent run summary for a source.

    Args:
        spark: Active SparkSession
        source: Source system name
        layer: "bronze" or "silver"

    Returns:
        Dict with summary data, or None if no runs found

    Example:
        >>> summary = get_latest_run_summary(spark, "vizier", "bronze")
        >>> if summary:
        ...     print(f"Last run: {summary['run_ts']}, tables: {summary['total_tables']}")
    """
    table = BRONZE_SUMMARY_TABLE if layer == "bronze" else SILVER_SUMMARY_TABLE

    latest = spark.table(table) \
        .where(f"source = '{source}'") \
        .orderBy(F.col("run_ts").desc()) \
        .limit(1) \
        .collect()

    if not latest:
        return None

    return latest[0].asDict()


# =============================================================================
# Batch Logging Functions
# =============================================================================

def _prepare_bronze_rows(bronze_results: List[Dict[str, Any]], run_log_id: str):
    """
    Prepare Bronze log records for batch insertion.

    Args:
        bronze_results: List of Bronze processing results
        run_log_id: Run log ID for linking to summary

    Returns:
        List of tuples ready for DataFrame creation
    """
    rows = []
    for r in bronze_results:
        log_id = r.get("log_id") or f"{run_log_id}_{uuid4().hex[:8]}"
        partition_key = r.get("partition_key") or r.get("run_ts")
        error_msg = truncate_error_message(r.get("error_message"))

        rows.append(
            (
                log_id,
                run_log_id,
                r.get("run_id"),
                r.get("run_date"),
                r.get("run_ts"),
                r.get("source"),
                r.get("table_name"),
                partition_key,
                r.get("load_mode"),
                r.get("status"),
                r.get("rows_processed"),
                r.get("start_time"),
                r.get("end_time"),
                r.get("duration_seconds"),
                error_msg,
                r.get("parquet_path"),
                r.get("delta_table"),
            )
        )
    return rows


def _prepare_silver_rows(records: List[Dict[str, Any]]):
    """
    Prepare Silver log records for batch insertion.

    Args:
        records: List of Silver processing results

    Returns:
        List of Rows ready for DataFrame creation
    """
    rows = []
    for r in records:
        run_ts = r.get("run_ts")
        if not run_ts:
            raise ValueError("Silver log record is missing run_ts")

        run_date = r.get("run_date")
        if run_date is None:
            run_date = build_run_date(run_ts)

        error_msg = truncate_error_message(r.get("error_message"))

        rows.append(Row(
            log_id           = r.get("log_id"),
            run_id           = r.get("run_id"),
            run_date         = run_date,
            run_ts           = run_ts,
            source           = r.get("source"),
            table_name       = r.get("table_name"),
            load_mode        = r.get("load_mode"),
            status           = r.get("status"),
            rows_inserted    = r.get("rows_inserted"),
            rows_updated     = r.get("rows_updated"),
            rows_deleted     = r.get("rows_deleted"),
            rows_unchanged   = r.get("rows_unchanged"),
            total_silver_rows= r.get("total_silver_rows"),
            bronze_rows      = r.get("bronze_rows"),
            bronze_table     = r.get("bronze_table"),
            start_time       = r.get("start_time"),
            end_time         = r.get("end_time"),
            duration_seconds = r.get("duration_seconds"),
            error_message    = error_msg,
            silver_table     = r.get("silver_table"),
        ))
    return rows


def log_batch(
    spark: SparkSession,
    records: List[Dict[str, Any]],
    layer: str,
    run_log_id: Optional[str] = None
) -> None:
    """
    Write many log records in a single batch append for the given layer.

    Args:
        spark: Active SparkSession
        records: List of processing results to log
        layer: "bronze" or "silver"
        run_log_id: Required for Bronze logging (links to summary)

    Raises:
        ValueError: If layer is invalid or run_log_id missing for Bronze

    Example:
        >>> # Bronze logging
        >>> log_batch(spark, bronze_results, "bronze", run_log_id="abc123")

        >>> # Silver logging
        >>> log_batch(spark, silver_results, "silver")
    """
    # Import schemas here to avoid circular dependency
    from modules.log_schemas import (
        bronze_processing_log_schema,
        silver_processing_log_schema,
        BRONZE_LOG_TABLE_FULLNAME,
        SILVER_LOG_TABLE_FULLNAME
    )

    if not records:
        return

    layer = layer.lower()
    if layer == "bronze":
        if not run_log_id:
            raise ValueError("run_log_id is required for Bronze batch logging")
        rows = _prepare_bronze_rows(records, run_log_id)
        schema = bronze_processing_log_schema
        table = BRONZE_LOG_TABLE_FULLNAME
    elif layer == "silver":
        rows = _prepare_silver_rows(records)
        schema = silver_processing_log_schema
        table = SILVER_LOG_TABLE_FULLNAME
    else:
        raise ValueError("layer must be 'bronze' or 'silver'")

    df = spark.createDataFrame(rows, schema=schema)

    (df.write
        .format("delta")
        .mode("append")
        .saveAsTable(table))

    logger = logging.getLogger(__name__)
    logger.info(f"✓ Logged {len(records)} {layer.capitalize()} records to {table}")


def log_summary(
    spark: SparkSession,
    summary: Dict[str, Any],
    layer: str
) -> Optional[str]:
    """
    Write run summary for Bronze or Silver processing.

    Args:
        spark: Active SparkSession
        summary: Summary statistics dict
        layer: "bronze" or "silver"

    Returns:
        The Bronze run log_id for linking batch rows, or None for Silver

    Raises:
        ValueError: If layer is invalid or required fields missing

    Example:
        >>> bronze_summary = {
        ...     "run_ts": "20251105T142752505",
        ...     "run_date": date(2025, 11, 5),
        ...     "source": "vizier",
        ...     "total_tables": 50,
        ...     "tables_success": 48,
        ...     # ... more fields
        ... }
        >>> run_log_id = log_summary(spark, bronze_summary, "bronze")
    """
    # Import schemas here to avoid circular dependency
    from modules.log_schemas import (
        bronze_run_summary_schema,
        silver_run_summary_schema,
        BRONZE_SUMMARY_TABLE_FULLNAME,
        SILVER_SUMMARY_TABLE_FULLNAME
    )

    layer = layer.lower()

    if layer == "bronze":
        log_id = summary.get("log_id") or uuid4().hex
        run_ts = summary["run_ts"]
        run_id = summary.get("run_id") or f"{run_ts}_{log_id[:8]}"

        row = {
            "log_id":               log_id,
            "run_id":               run_id,
            "run_date":             summary["run_date"],
            "run_ts":               run_ts,
            "source":               summary.get("source"),
            "status":               summary.get("status", "SUCCESS"),
            "run_start":            summary["run_start"],
            "run_end":              summary["run_end"],
            "duration_seconds":     summary.get("duration_seconds"),
            "total_tables":         summary["total_tables"],
            "tables_success":       summary["tables_success"],
            "tables_empty":         summary["tables_empty"],
            "tables_failed":        summary["tables_failed"],
            "tables_skipped":       summary["tables_skipped"],
            "total_rows":           summary["total_rows"],
            "workers":              summary["workers"],
            "sum_task_seconds":     summary.get("sum_task_seconds"),
            "theoretical_min_sec":  summary.get("theoretical_min_sec"),
            "actual_time_sec":      summary.get("actual_time_sec"),
            "efficiency_pct":       summary.get("efficiency_pct"),
            "failed_tables":        summary.get("failed_tables"),
            "error_message":        summary.get("error_message"),
        }

        df = spark.createDataFrame([row], schema=bronze_run_summary_schema)
        table = BRONZE_SUMMARY_TABLE_FULLNAME

    elif layer == "silver":
        run_ts = summary.get("run_ts")
        if not run_ts:
            raise ValueError("Summary missing run_ts")

        run_date = summary.get("run_date")
        if run_date is None:
            run_date = build_run_date(run_ts)

        failed_tables = summary.get("failed_tables", [])
        failed_tables_json = json.dumps(failed_tables) if failed_tables else None

        row = Row(
            run_id              = summary.get("run_id"),
            source              = summary.get("source"),
            run_ts              = run_ts,
            run_date            = run_date,
            run_start           = summary.get("run_start"),
            run_end             = summary.get("run_end"),
            duration_seconds    = summary.get("duration_seconds"),
            total_tables        = summary.get("total_tables"),
            tables_success      = summary.get("tables_success"),
            tables_failed       = summary.get("tables_failed"),
            tables_skipped      = summary.get("tables_skipped"),
            total_inserts       = summary.get("total_inserts"),
            total_updates       = summary.get("total_updates"),
            total_deletes       = summary.get("total_deletes"),
            total_unchanged     = summary.get("total_unchanged"),
            failed_tables       = failed_tables_json,
        )

        df = spark.createDataFrame([row], schema=silver_run_summary_schema)
        table = SILVER_SUMMARY_TABLE_FULLNAME
        log_id = None

    else:
        raise ValueError("layer must be 'bronze' or 'silver'")

    (df.write
        .format("delta")
        .mode("append")
        .saveAsTable(table))

    logger = logging.getLogger(__name__)
    logger.info(f"✓ Logged {layer.capitalize()} summary to {table}")
    return log_id