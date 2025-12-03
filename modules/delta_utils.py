"""
Delta Lake Utilities

Provides helper functions for working with Delta Lake tables.
"""

from typing import Optional
from pyspark.sql import SparkSession, functions as F
import logging

logger = logging.getLogger(__name__)


def get_last_num_output_rows(spark: SparkSession, table_fullname: str) -> Optional[int]:
    """
    Get the number of written rows from the last Delta write operation.

    Uses DESCRIBE HISTORY to extract numOutputRows from the most recent version.

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)

    Returns:
        Row count from last write, or None if not available

    Example:
        >>> rows = get_last_num_output_rows(spark, "bronze.Dim_Relatie")
        >>> print(f"Last write: {rows:,} rows")
    """
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_fullname}")
        row = (
            history
            .orderBy(F.col("version").desc())
            .select(F.col("operationMetrics")["numOutputRows"].alias("rows"))
            .first()
        )
        return int(row["rows"]) if row and row["rows"] is not None else None
    except Exception as e:
        logger.warning(f"Failed to get numOutputRows for {table_fullname}: {e}")
        return None


def get_delta_version(spark: SparkSession, table_fullname: str) -> Optional[int]:
    """
    Get the current version number of a Delta table.

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)

    Returns:
        Current version number, or None if table doesn't exist

    Example:
        >>> version = get_delta_version(spark, "bronze.Dim_Relatie")
        >>> print(f"Current version: {version}")
    """
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_fullname}")
        latest = history.orderBy(F.col("version").desc()).first()
        return int(latest["version"]) if latest else None
    except Exception as e:
        logger.warning(f"Failed to get version for {table_fullname}: {e}")
        return None


def table_exists(spark: SparkSession, table_fullname: str) -> bool:
    """
    Check if a Delta table exists in the catalog.

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)

    Returns:
        True if table exists

    Example:
        >>> if table_exists(spark, "bronze.Dim_Relatie"):
        ...     print("Table exists")
    """
    return spark.catalog.tableExists(table_fullname)


def get_table_size(spark: SparkSession, table_fullname: str) -> Optional[int]:
    """
    Get the number of rows in a Delta table.

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)

    Returns:
        Row count, or None if table doesn't exist

    Example:
        >>> size = get_table_size(spark, "bronze.Dim_Relatie")
        >>> print(f"Table size: {size:,} rows")
    """
    try:
        if not table_exists(spark, table_fullname):
            return None
        return spark.table(table_fullname).count()
    except Exception as e:
        logger.warning(f"Failed to get table size for {table_fullname}: {e}")
        return None


def optimize_table(spark: SparkSession, table_fullname: str, zorder_cols: Optional[list] = None) -> None:
    """
    Optimize a Delta table (compact small files, optionally Z-ORDER).

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)
        zorder_cols: Optional list of columns for Z-ORDER BY

    Example:
        >>> # Basic optimize
        >>> optimize_table(spark, "bronze.Dim_Relatie")
        >>>
        >>> # With Z-ORDER
        >>> optimize_table(spark, "bronze.Dim_Relatie", ["Rel_Id", "Updatedatum"])
    """
    try:
        if zorder_cols:
            zorder_clause = ", ".join(zorder_cols)
            spark.sql(f"OPTIMIZE {table_fullname} ZORDER BY ({zorder_clause})")
            logger.info(f"Optimized {table_fullname} with ZORDER BY ({zorder_clause})")
        else:
            spark.sql(f"OPTIMIZE {table_fullname}")
            logger.info(f"Optimized {table_fullname}")
    except Exception as e:
        logger.error(f"Failed to optimize {table_fullname}: {e}")
        raise


def vacuum_table(spark: SparkSession, table_fullname: str, retention_hours: int = 168) -> None:
    """
    Vacuum a Delta table (remove old file versions).

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)
        retention_hours: Retention period in hours (default: 168 = 7 days)

    Example:
        >>> # Vacuum with default 7-day retention
        >>> vacuum_table(spark, "bronze.Dim_Relatie")
        >>>
        >>> # Vacuum with custom retention
        >>> vacuum_table(spark, "bronze.Dim_Relatie", retention_hours=24)
    """
    try:
        spark.sql(f"VACUUM {table_fullname} RETAIN {retention_hours} HOURS")
        logger.info(f"Vacuumed {table_fullname} (retention: {retention_hours}h)")
    except Exception as e:
        logger.error(f"Failed to vacuum {table_fullname}: {e}")
        raise


def get_table_location(spark: SparkSession, table_fullname: str) -> Optional[str]:
    """
    Get the storage location of a Delta table.

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)

    Returns:
        Table location path, or None if not found

    Example:
        >>> location = get_table_location(spark, "bronze.Dim_Relatie")
        >>> print(f"Table location: {location}")
    """
    try:
        desc = spark.sql(f"DESCRIBE EXTENDED {table_fullname}")
        location_row = desc.where("col_name = 'Location'").first()
        return location_row["data_type"] if location_row else None
    except Exception as e:
        logger.warning(f"Failed to get location for {table_fullname}: {e}")
        return None


def drop_table_if_exists(spark: SparkSession, table_fullname: str) -> bool:
    """
    Drop a Delta table if it exists.

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)

    Returns:
        True if table was dropped, False if table didn't exist

    Example:
        >>> if drop_table_if_exists(spark, "bronze.temp_table"):
        ...     print("Table dropped")
    """
    try:
        if table_exists(spark, table_fullname):
            spark.sql(f"DROP TABLE {table_fullname}")
            logger.info(f"Dropped table: {table_fullname}")
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to drop table {table_fullname}: {e}")
        raise


def analyze_table(spark: SparkSession, table_fullname: str, compute_stats: bool = True) -> None:
    """
    Analyze a Delta table to compute statistics.

    Args:
        spark: Active SparkSession
        table_fullname: Full table name (schema.table)
        compute_stats: If True, compute column statistics (default: True)

    Example:
        >>> analyze_table(spark, "bronze.Dim_Relatie")
    """
    try:
        if compute_stats:
            spark.sql(f"ANALYZE TABLE {table_fullname} COMPUTE STATISTICS FOR ALL COLUMNS")
            logger.info(f"Analyzed {table_fullname} with column statistics")
        else:
            spark.sql(f"ANALYZE TABLE {table_fullname} COMPUTE STATISTICS")
            logger.info(f"Analyzed {table_fullname}")
    except Exception as e:
        logger.error(f"Failed to analyze {table_fullname}: {e}")
        raise
