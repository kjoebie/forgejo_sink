"""
Log Table Schema Definitions

Provides schema definitions for Bronze and Silver processing log tables.
These schemas are used for table creation and data validation.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, DateType, DoubleType
)


# =============================================================================
# Table Name Constants
# =============================================================================

LOG_SCHEMA = "logs"

# Bronze logging tables
BRONZE_LOG_TABLE = "bronze_processing_log"
BRONZE_LOG_TABLE_FULLNAME = f"{LOG_SCHEMA}.{BRONZE_LOG_TABLE}"

BRONZE_SUMMARY_TABLE = "bronze_run_summary"
BRONZE_SUMMARY_TABLE_FULLNAME = f"{LOG_SCHEMA}.{BRONZE_SUMMARY_TABLE}"

# Silver logging tables
SILVER_LOG_TABLE = "silver_processing_log"
SILVER_LOG_TABLE_FULLNAME = f"{LOG_SCHEMA}.{SILVER_LOG_TABLE}"

SILVER_SUMMARY_TABLE = "silver_run_summary"
SILVER_SUMMARY_TABLE_FULLNAME = f"{LOG_SCHEMA}.{SILVER_SUMMARY_TABLE}"


# =============================================================================
# Bronze Processing Log Schema
# =============================================================================

bronze_processing_log_schema = StructType([
    # Identifiers
    StructField("log_id",           StringType(),    False),  # Unique log entry ID
    StructField("run_log_id",       StringType(),    False),  # FK to bronze_run_summary.log_id
    StructField("run_id",           StringType(),    False),  # Run identifier
    StructField("run_date",         DateType(),      False),  # Partition key (derived from run_ts)
    StructField("run_ts",           StringType(),    False),  # Run timestamp (yyyyMMddTHHmmssSSS)

    # Source information
    StructField("source",           StringType(),    False),  # Source system name
    StructField("table_name",       StringType(),    False),  # Table name (partition key)
    StructField("partition_key",    StringType(),    True),   # Optional partition key
    StructField("load_mode",        StringType(),    True),   # snapshot, incremental, window

    # Processing results
    StructField("status",           StringType(),    False),  # SUCCESS, FAILED, SKIPPED, EMPTY
    StructField("rows_processed",   LongType(),      True),   # Rows processed from parquet to Bronze

    # Timing
    StructField("start_time",       TimestampType(), True),
    StructField("end_time",         TimestampType(), True),
    StructField("duration_seconds", LongType(),      True),

    # Error handling
    StructField("error_message",    StringType(),    True),   # Truncated to 1000 chars

    # Source/target paths
    StructField("parquet_path",     StringType(),    True),   # Source parquet folder
    StructField("delta_table",      StringType(),    True),   # Target Bronze table
])


# =============================================================================
# Bronze Run Summary Schema
# =============================================================================

bronze_run_summary_schema = StructType([
    # Run identifiers
    StructField("log_id",                   StringType(), False),  # run-level GUID
    StructField("run_id",                   StringType(), False),  # e.g. "{run_ts}_{guid8}"
    StructField("run_date",                 DateType(), False),
    StructField("run_ts",                   StringType(), False),
    StructField("source",                   StringType(), True),

    # Timing
    StructField("status",                   StringType(), False),  # SUCCESS / FAILED / PARTIAL
    StructField("run_start",                TimestampType(), False),
    StructField("run_end",                  TimestampType(), False),
    StructField("duration_seconds",         DoubleType(), True),

    # Table counts
    StructField("total_tables",             LongType(), False),
    StructField("tables_success",           LongType(), False),
    StructField("tables_empty",             LongType(), False),
    StructField("tables_failed",            LongType(), False),
    StructField("tables_skipped",           LongType(), False),
    StructField("total_rows",               LongType(), False),

    # Performance metrics
    StructField("workers",                  LongType(), True),       # Parallel workers used
    StructField("sum_task_seconds",         DoubleType(), True),     # Sum of all task durations
    StructField("theoretical_min_sec",      DoubleType(), True),     # Sum / workers
    StructField("actual_time_sec",          DoubleType(), True),     # Wall clock time
    StructField("efficiency_pct",           DoubleType(), True),     # (theoretical / actual) * 100

    # Failed tables list
    StructField("failed_tables",            StringType(), True),     # JSON array of failed table names
    StructField("error_message",            StringType(), True),     # batch-level error, if any
])


# =============================================================================
# Silver Processing Log Schema
# =============================================================================

silver_processing_log_schema = StructType([
    # Identifiers
    StructField("log_id",           StringType(),    False),
    StructField("run_id",           StringType(),    False),
    StructField("run_date",         DateType(),      False),
    StructField("run_ts",           StringType(),    False),

    # Source information
    StructField("source",           StringType(),    False),
    StructField("table_name",       StringType(),    False),
    StructField("load_mode",        StringType(),    True),

    # Processing results
    StructField("status",           StringType(),    False),  # SUCCESS, FAILED, SKIPPED

    # CDC statistics
    StructField("rows_inserted",    LongType(),      True),   # New rows added to Silver
    StructField("rows_updated",     LongType(),      True),   # Existing rows updated
    StructField("rows_deleted",     LongType(),      True),   # Rows marked as deleted
    StructField("rows_unchanged",   LongType(),      True),   # Rows with no changes
    StructField("total_silver_rows",LongType(),      True),   # Total rows in Silver after merge

    # Bronze source info
    StructField("bronze_rows",      LongType(),      True),   # Rows processed from Bronze
    StructField("bronze_table",     StringType(),    True),   # Source Bronze table

    # Timing
    StructField("start_time",       TimestampType(), True),
    StructField("end_time",         TimestampType(), True),
    StructField("duration_seconds", LongType(),      True),

    # Error handling
    StructField("error_message",    StringType(),    True),

    # Target
    StructField("silver_table",     StringType(),    True),   # Target Silver table
])


# =============================================================================
# Silver Run Summary Schema
# =============================================================================

silver_run_summary_schema = StructType([
    # Run identifiers
    StructField("run_id",              StringType(),    False),
    StructField("source",              StringType(),    False),
    StructField("run_ts",              StringType(),    False),
    StructField("run_date",            DateType(),      False),

    # Timing
    StructField("run_start",           TimestampType(), False),
    StructField("run_end",             TimestampType(), True),
    StructField("duration_seconds",    LongType(),      True),

    # Table counts
    StructField("total_tables",        LongType(),      False),
    StructField("tables_success",      LongType(),      True),
    StructField("tables_failed",       LongType(),      True),
    StructField("tables_skipped",      LongType(),      True),

    # Aggregate CDC statistics
    StructField("total_inserts",       LongType(),      True),
    StructField("total_updates",       LongType(),      True),
    StructField("total_deletes",       LongType(),      True),
    StructField("total_unchanged",     LongType(),      True),

    # Failed tables
    StructField("failed_tables",       StringType(),    True),
])
