"""
Test log table initialization and auto-creation functionality.

This test verifies that:
1. ensure_log_tables() creates all required tables
2. log_batch() and log_summary() automatically call ensure_log_tables()
3. Tables are created with correct schemas and partitioning
"""

import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from modules.logging_utils import ensure_log_tables, log_batch, log_summary, _log_tables_initialized
from modules.log_schemas import (
    LOG_SCHEMA,
    BRONZE_LOG_TABLE_FULLNAME,
    BRONZE_SUMMARY_TABLE_FULLNAME,
    SILVER_LOG_TABLE_FULLNAME,
    SILVER_SUMMARY_TABLE_FULLNAME
)


@pytest.fixture(scope="module")
def spark():
    """Create a test Spark session."""
    from modules.spark_session import get_or_create_spark_session

    spark = get_or_create_spark_session(
        app_name="Test_Log_Initialization",
        enable_hive=True
    )
    yield spark
    # Cleanup: drop test tables after tests
    try:
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_LOG_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SUMMARY_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {SILVER_LOG_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {SILVER_SUMMARY_TABLE_FULLNAME}")
    except Exception:
        pass


def test_ensure_log_tables_creates_schema(spark):
    """Test that ensure_log_tables creates the logs schema."""
    # Drop schema if exists (cleanup from previous test)
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {LOG_SCHEMA} CASCADE")
    except Exception:
        pass

    # Call ensure_log_tables
    ensure_log_tables(spark, debug=True)

    # Verify schema exists
    schemas = [row.namespace for row in spark.sql("SHOW SCHEMAS").collect()]
    assert LOG_SCHEMA in schemas, f"Schema '{LOG_SCHEMA}' was not created"


def test_ensure_log_tables_creates_all_tables(spark):
    """Test that ensure_log_tables creates all 4 log tables."""
    # Ensure tables exist
    ensure_log_tables(spark, debug=True)

    # Verify all tables exist
    tables = [
        BRONZE_LOG_TABLE_FULLNAME,
        BRONZE_SUMMARY_TABLE_FULLNAME,
        SILVER_LOG_TABLE_FULLNAME,
        SILVER_SUMMARY_TABLE_FULLNAME
    ]

    for table in tables:
        assert spark.catalog.tableExists(table), f"Table '{table}' was not created"


def test_ensure_log_tables_is_idempotent(spark):
    """Test that ensure_log_tables can be called multiple times safely."""
    # Call multiple times - should not raise error
    ensure_log_tables(spark, debug=True)
    ensure_log_tables(spark, debug=True)
    ensure_log_tables(spark, debug=True)

    # Verify tables still exist
    assert spark.catalog.tableExists(BRONZE_LOG_TABLE_FULLNAME)


def test_bronze_log_table_has_correct_partitioning(spark):
    """Test that bronze_processing_log has correct partitioning."""
    ensure_log_tables(spark, debug=True)

    # Get table description
    desc = spark.sql(f"DESCRIBE EXTENDED {BRONZE_LOG_TABLE_FULLNAME}").collect()
    desc_dict = {row.col_name.strip(): row.data_type for row in desc}

    # Check partition columns exist
    assert "# Partition Information" in [row.col_name for row in desc], \
        "bronze_processing_log should be partitioned"


def test_silver_log_table_has_correct_partitioning(spark):
    """Test that silver_processing_log has correct partitioning."""
    ensure_log_tables(spark, debug=True)

    # Get table description
    desc = spark.sql(f"DESCRIBE EXTENDED {SILVER_LOG_TABLE_FULLNAME}").collect()

    # Check partition columns exist
    assert "# Partition Information" in [row.col_name for row in desc], \
        "silver_processing_log should be partitioned"


def test_log_summary_auto_creates_tables(spark):
    """Test that log_summary automatically creates tables if they don't exist."""
    # Drop tables
    try:
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SUMMARY_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_LOG_TABLE_FULLNAME}")
    except Exception:
        pass

    # Reset initialization flag
    import modules.logging_utils as lu
    lu._log_tables_initialized = False

    # Call log_summary - should auto-create tables
    test_summary = {
        "run_ts": datetime.now().strftime("%Y%m%dT%H%M%S%f")[:-3],
        "run_date": date.today(),
        "source": "test_source",
        "status": "SUCCESS",
        "run_start": datetime.now(),
        "run_end": datetime.now(),
        "total_tables": 1,
        "tables_success": 1,
        "tables_empty": 0,
        "tables_failed": 0,
        "tables_skipped": 0,
        "total_rows": 0
    }

    run_log_id = log_summary(spark, test_summary, layer="bronze")

    # Verify tables were created
    assert spark.catalog.tableExists(BRONZE_SUMMARY_TABLE_FULLNAME), \
        "log_summary should auto-create bronze_run_summary table"
    assert spark.catalog.tableExists(BRONZE_LOG_TABLE_FULLNAME), \
        "ensure_log_tables should create all tables including bronze_processing_log"
    assert run_log_id is not None, "log_summary should return run_log_id for bronze"


def test_log_batch_auto_creates_tables(spark):
    """Test that log_batch automatically creates tables if they don't exist."""
    # Drop tables
    try:
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_LOG_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SUMMARY_TABLE_FULLNAME}")
    except Exception:
        pass

    # Reset initialization flag
    import modules.logging_utils as lu
    lu._log_tables_initialized = False

    # Call log_batch - should auto-create tables
    test_records = [{
        "run_ts": datetime.now().strftime("%Y%m%dT%H%M%S%f")[:-3],
        "run_date": date.today(),
        "run_id": "test_run_id",
        "source": "test_source",
        "table_name": "test_table",
        "status": "SUCCESS",
        "rows_processed": 100,
        "start_time": datetime.now(),
        "end_time": datetime.now(),
        "duration_seconds": 5,
        "load_mode": "snapshot"
    }]

    log_batch(spark, test_records, layer="bronze", run_log_id="test_log_id")

    # Verify tables were created
    assert spark.catalog.tableExists(BRONZE_LOG_TABLE_FULLNAME), \
        "log_batch should auto-create bronze_processing_log table"


def test_error_logging_without_existing_tables(spark):
    """
    Test the original problem scenario: logging an error when tables don't exist.

    This simulates the scenario where process_bronze_layer encounters an error
    and needs to log it, but the log tables don't exist yet.
    """
    # Drop all log tables to simulate fresh environment
    try:
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_LOG_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SUMMARY_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {SILVER_LOG_TABLE_FULLNAME}")
        spark.sql(f"DROP TABLE IF EXISTS {SILVER_SUMMARY_TABLE_FULLNAME}")
    except Exception:
        pass

    # Reset initialization flag
    import modules.logging_utils as lu
    lu._log_tables_initialized = False

    # Simulate error scenario: process_bronze_layer fails and tries to log
    error_records = [{
        "run_ts": datetime.now().strftime("%Y%m%dT%H%M%S%f")[:-3],
        "run_date": date.today(),
        "run_id": "error_test_run",
        "source": "test_source",
        "table_name": "failing_table",
        "status": "FAILED",
        "rows_processed": 0,
        "start_time": datetime.now(),
        "end_time": datetime.now(),
        "duration_seconds": 1,
        "load_mode": "snapshot",
        "error_message": "Simulated processing error - THIS SHOULD BE LOGGED!"
    }]

    # This should NOT raise an error about missing tables
    # Instead, it should create tables and log the error
    try:
        log_batch(spark, error_records, layer="bronze", run_log_id="error_log_id")

        # Verify the error was actually logged
        logged_df = spark.table(BRONZE_LOG_TABLE_FULLNAME)
        error_row = logged_df.filter("table_name = 'failing_table'").collect()

        assert len(error_row) > 0, "Error record should be logged"
        assert error_row[0].status == "FAILED", "Status should be FAILED"
        assert "Simulated processing error" in error_row[0].error_message, \
            "Error message should be preserved"

        print("✓ Error logging test passed: Errors can be logged even when tables don't exist!")

    except Exception as e:
        pytest.fail(f"log_batch should not fail when tables don't exist. Error: {e}")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
