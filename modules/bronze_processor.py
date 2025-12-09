"""
Bronze Layer Processing

Provides core Bronze processing logic for loading tables from parquet to Delta.

Architecture: Bronze History Pattern
- Snapshot/Window tables: Overwrite entire table
- Incremental tables: Append with _bronze_load_ts partition (for CDC)
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, input_file_name, col, year, month
from datetime import datetime, timezone
from typing import Dict, Any
from uuid import uuid4
import logging

from modules.path_utils import build_parquet_dir
from modules.error_utils import is_missing_path_error, is_probably_corrupt_delta
from modules.delta_utils import get_last_num_output_rows

logger = logging.getLogger(__name__)


def process_bronze_table(
    spark: SparkSession,
    table_def: Dict[str, Any],
    source_name: str,
    run_id: str,
    run_ts: str,
    run_date: str,
    base_files: str = "greenhouse_sources",
    debug: bool = False
) -> Dict[str, Any]:
    """
    Load a single table's parquet files for a given run_ts into Bronze Delta table.

    Architecture:
    - Snapshot/Window: Overwrite entire table
    - Incremental: Append with _bronze_load_ts partition (for CDC)

    Args:
        spark: Active SparkSession
        table_def: Table definition from DAG
        source_name: Source system name (e.g., "vizier")
        run_id: Unique run identifier
        run_ts: Run timestamp (e.g., "20251105T142752505")
        run_date: Run date (for partitioning logs)
        base_files: Base directory for files (default: "greenhouse_sources")
        debug: Enable debug output

    Returns:
        Dict with processing results:
        - log_id, run_id, run_ts, run_date, source, table_name, load_mode
        - status (SUCCESS, FAILED, SKIPPED, EMPTY)
        - rows_processed
        - start_time, end_time, duration_seconds
        - error_message, parquet_path, delta_table
    """

    # Validate table_def
    table_name = table_def.get("name")
    if not table_name:
        raise ValueError("table_def is missing 'name'")

    # Get load mode (default to snapshot)
    load_mode = (table_def.get("load_mode") or "snapshot").lower()
    supported_modes = {"snapshot", "window", "incremental"}

    # Initialize metrics
    log_id = f"{source_name}:{table_name}:{run_ts}:{uuid4().hex[:8]}"
    start_time = datetime.now(timezone.utc)
    end_time = None
    status = "RUNNING"
    error_message = None
    rows_processed = None

    # Early exit for unsupported load modes
    if load_mode not in supported_modes:
        end_time = datetime.now(timezone.utc)
        duration = int((end_time - start_time).total_seconds())

        if debug:
            logger.info(f"[{table_name}] SKIPPED: unsupported load_mode '{load_mode}'")

        return {
            "log_id": log_id,
            "run_id": run_id,
            "run_date": run_date,
            "run_ts": run_ts,
            "source": source_name,
            "table_name": table_name,
            "load_mode": load_mode,
            "status": "SKIPPED",
            "rows_processed": None,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "error_message": f"Unsupported load_mode '{load_mode}'",
            "parquet_path": None,
            "delta_table": None,
        }

    # Build target table name
    target_table = table_def.get("delta_table") or table_name
    delta_schema = table_def.get("delta_schema") or "bronze"

    # Handle schema.table format
    if "." not in target_table:
        delta_table_full = f"{delta_schema}.{target_table}"
    else:
        delta_table_full = target_table

    # Build parquet path
    parquet_dir = build_parquet_dir(base_files, source_name, run_ts, table_name, spark)
    parquet_glob = f"{parquet_dir}/*.parquet"

    if debug:
        logger.info(f"[{table_name}] Starting ({load_mode})")
        logger.info(f"  Parquet: {parquet_dir}")
        logger.info(f"  Target: {delta_table_full}")

    # ========================================================================
    # STEP 1: Read Parquet
    # ========================================================================

    try:
        df = spark.read.parquet(parquet_glob)

    except Exception as e:
        if is_missing_path_error(e):
            # No parquet files - table not exported in this run
            end_time = datetime.now(timezone.utc)
            duration = int((end_time - start_time).total_seconds())

            if debug:
                logger.info(f"[{table_name}] SKIPPED: No parquet files in {parquet_dir}")

            return {
                "log_id": log_id,
                "run_id": run_id,
                "run_date": run_date,
                "run_ts": run_ts,
                "source": source_name,
                "table_name": table_name,
                "load_mode": load_mode,
                "status": "SKIPPED",
                "rows_processed": 0,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "error_message": f"No parquet files found in {parquet_dir}",
                "parquet_path": parquet_dir,
                "delta_table": delta_table_full,
            }
        else:
            # Other read error
            end_time = datetime.now(timezone.utc)
            duration = int((end_time - start_time).total_seconds())

            if debug:
                logger.info(f"[{table_name}] FAILED reading parquet: {str(e)[:200]}")

            return {
                "log_id": log_id,
                "run_id": run_id,
                "run_date": run_date,
                "run_ts": run_ts,
                "source": source_name,
                "table_name": table_name,
                "load_mode": load_mode,
                "status": "FAILED",
                "rows_processed": None,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "error_message": f"Read parquet failed: {str(e)[:500]}",
                "parquet_path": parquet_dir,
                "delta_table": delta_table_full,
            }

    # ========================================================================
    # STEP 2: Add Metadata Columns
    # ========================================================================

    # Add Bronze metadata columns
    df_with_meta = df \
        .withColumn("_bronze_load_ts", lit(run_ts)) \
        .withColumn("_bronze_filename", input_file_name())

    # For window tables with partitioning config, add partition columns
    partitioning_config = table_def.get("partitioning")
    if partitioning_config and load_mode in ("window",):
        partition_type = partitioning_config.get("type")

        if partition_type == "year_month":
            year_col = partitioning_config.get("year_col", "p_year")
            month_col = partitioning_config.get("month_col", "p_month")

            # Get window column to extract year/month from
            window_config = table_def.get("window", {})
            window_col = window_config.get("column", "Boek_Datum")  # Default

            if window_col in df.columns:
                df_with_meta = df_with_meta \
                    .withColumn(year_col, year(col(window_col))) \
                    .withColumn(month_col, month(col(window_col)))

                if debug:
                    logger.info(f"  Added partitioning: {year_col}, {month_col} from {window_col}")

    # ========================================================================
    # STEP 3: Write to Delta
    # ========================================================================

    try:

        # Determine write mode based on load_mode
        if load_mode == "incremental":
            # APPEND with partition by _bronze_load_ts (CDC history)
            writer = df_with_meta.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("_bronze_load_ts")

            if debug:
                logger.info(f"  Mode: APPEND with partition by _bronze_load_ts (CDC history)")

        elif load_mode in ("snapshot", "window"):
            # OVERWRITE entire table

            if partitioning_config:
                # Partitioned overwrite
                partition_type = partitioning_config.get("type")
                if partition_type == "year_month":
                    year_col = partitioning_config.get("year_col", "p_year")
                    month_col = partitioning_config.get("month_col", "p_month")

                    writer = df_with_meta.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .partitionBy(year_col, month_col)

                    if debug:
                        logger.info(f"  Mode: OVERWRITE with partitioning by {year_col}, {month_col}")
                else:
                    # Unknown partition type, just overwrite
                    writer = df_with_meta.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true")
            else:
                # No partitioning, simple overwrite
                writer = df_with_meta.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true")

                if debug:
                    logger.info(f"  Mode: OVERWRITE")

        # Execute write
        writer.saveAsTable(delta_table_full)
        rows_processed = get_last_num_output_rows(spark, delta_table_full)

        # Check for empty result
        if rows_processed == 0:
            end_time = datetime.now(timezone.utc)
            duration = int((end_time - start_time).total_seconds())

            if debug:
                logger.info(f"[{table_name}] EMPTY: Parquet exists but contains 0 rows")

            return {
                "log_id": log_id,
                "run_id": run_id,
                "run_date": run_date,
                "run_ts": run_ts,
                "source": source_name,
                "table_name": table_name,
                "load_mode": load_mode,
                "status": "EMPTY",
                "rows_processed": 0,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "error_message": "Parquet exists but contains 0 rows",
                "parquet_path": parquet_dir,
                "delta_table": delta_table_full,
            }

        # Success!
        status = "SUCCESS"

    except Exception as e:
        # Try recovery if Delta table looks corrupt
        if is_probably_corrupt_delta(e):
            if debug:
                logger.info(f"[{table_name}] Write failed, attempting DROP+RECREATE: {str(e)[:200]}")

            try:
                # Drop and recreate
                spark.sql(f"DROP TABLE IF EXISTS {delta_table_full}")

                # Recreate with appropriate partitioning
                if load_mode == "incremental":
                    writer = df_with_meta.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .partitionBy("_bronze_load_ts")
                elif partitioning_config and partitioning_config.get("type") == "year_month":
                    year_col = partitioning_config.get("year_col", "p_year")
                    month_col = partitioning_config.get("month_col", "p_month")
                    writer = df_with_meta.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true") \
                        .partitionBy(year_col, month_col)
                else:
                    writer = df_with_meta.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("overwriteSchema", "true")

                writer.saveAsTable(delta_table_full)
                rows_processed = get_last_num_output_rows(spark, delta_table_full)

                status = "SUCCESS"
                error_message = f"Initial write failed but table was recreated. Original error: {str(e)[:300]}"

                if debug:
                    logger.info(f"[{table_name}] Recovery successful")

            except Exception as e2:
                status = "FAILED"
                error_message = f"Write failed and recovery failed: {str(e2)[:500]}"

                if debug:
                    logger.info(f"[{table_name}] Recovery FAILED: {str(e2)[:200]}")
        else:
            status = "FAILED"
            error_message = f"Write failed: {str(e)[:500]}"

            if debug:
                logger.info(f"[{table_name}] FAILED: {str(e)[:200]}")

    # ========================================================================
    # STEP 4: Return Results
    # ========================================================================

    end_time = datetime.now(timezone.utc)
    duration = int((end_time - start_time).total_seconds())

    if debug:
        logger.info(f"[{table_name}] {status} in {duration}s ({rows_processed:,} rows)")

    return {
        "log_id": log_id,
        "run_id": run_id,
        "run_date": run_date,
        "run_ts": run_ts,
        "source": source_name,
        "table_name": table_name,
        "load_mode": load_mode,
        "status": status,
        "rows_processed": rows_processed,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration,
        "error_message": error_message,
        "parquet_path": parquet_dir,
        "delta_table": delta_table_full,
    }
