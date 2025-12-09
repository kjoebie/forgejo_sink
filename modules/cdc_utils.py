"""
Change Data Capture (CDC) Utilities

Provides helper functions for CDC operations between Bronze and Silver layers.
"""

from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


def reconstruct_bronze_current_state(
    spark: SparkSession,
    bronze_table: str,
    business_keys: List[str],
    run_ts: Optional[str] = None
) -> DataFrame:
    """
    Reconstruct the current state from Bronze history.

    For incremental tables, Bronze contains multiple versions per key (append-only history).
    This function gets the latest version of each key based on _bronze_load_ts.

    Args:
        spark: Active SparkSession
        bronze_table: Full Bronze table name (e.g., "bronze.Dim_Relatie")
        business_keys: List of business key columns
        run_ts: Optional run_ts to filter up to (for point-in-time reconstruction)

    Returns:
        DataFrame with current state (one row per business key)

    Example:
        >>> # Get current state
        >>> current = reconstruct_bronze_current_state(
        ...     spark,
        ...     "bronze.Dim_Relatie",
        ...     ["Rel_Id"]
        ... )
        >>>
        >>> # Get state at specific point in time
        >>> historical = reconstruct_bronze_current_state(
        ...     spark,
        ...     "bronze.Dim_Relatie",
        ...     ["Rel_Id"],
        ...     run_ts="20251115T060000"
        ... )
    """
    bronze_df = spark.table(bronze_table)

    # Filter by run_ts if provided (point-in-time reconstruction)
    if run_ts:
        bronze_df = bronze_df.where(F.col("_bronze_load_ts") <= run_ts)

    # Window: partition by business keys, order by _bronze_load_ts DESC
    window = Window.partitionBy(*business_keys).orderBy(F.col("_bronze_load_ts").desc())

    # Add row number and filter to rn=1 (latest per key)
    current_state = bronze_df \
        .withColumn("_rn", F.row_number().over(window)) \
        .where(F.col("_rn") == 1) \
        .drop("_rn")

    return current_state


def get_business_columns(df: DataFrame) -> List[str]:
    """
    Get business columns (exclude metadata columns).

    Excludes columns starting with:
    - _bronze_
    - _silver_
    - _metadata_

    Args:
        df: Input DataFrame

    Returns:
        List of business column names

    Example:
        >>> df = spark.table("bronze.Dim_Relatie")
        >>> business_cols = get_business_columns(df)
        >>> # Returns: ["Rel_Id", "Naam", "Updatedatum", ...]
        >>> # Excludes: ["_bronze_load_ts", "_bronze_filename"]
    """
    metadata_prefixes = ("_bronze_", "_silver_", "_metadata_")

    return [
        c for c in df.columns
        if not any(c.startswith(prefix) for prefix in metadata_prefixes)
    ]


def detect_deletes(
    spark: SparkSession,
    bronze_current: DataFrame,
    silver_table: str,
    business_keys: List[str]
) -> DataFrame:
    """
    Detect deleted records (keys in Silver but not in Bronze).

    Uses LEFT ANTI join to find keys that exist in Silver but are missing from Bronze.

    Args:
        spark: Active SparkSession
        bronze_current: Current Bronze state DataFrame
        silver_table: Full Silver table name
        business_keys: List of business key columns

    Returns:
        DataFrame with deleted keys (business keys only)

    Example:
        >>> bronze_current = reconstruct_bronze_current_state(
        ...     spark, "bronze.Dim_Relatie", ["Rel_Id"]
        ... )
        >>> deleted = detect_deletes(
        ...     spark,
        ...     bronze_current,
        ...     "silver.Dim_Relatie",
        ...     ["Rel_Id"]
        ... )
        >>> print(f"Found {deleted.count()} deleted records")
    """
    # Get active keys from Silver (not already deleted)
    silver_active = spark.table(silver_table).where("is_deleted = false")

    # Find keys in Silver but not in Bronze (LEFT ANTI join)
    deleted_keys = silver_active.select(*business_keys).join(
        bronze_current.select(*business_keys),
        business_keys,
        "left_anti"
    )

    return deleted_keys


def compare_row_hashes(
    bronze_df: DataFrame,
    silver_df: DataFrame,
    business_keys: List[str],
    hash_column: str = "row_hash"
) -> dict:
    """
    Compare row hashes between Bronze and Silver to identify changes.

    Returns:
        Dict with DataFrames for each CDC operation:
        - inserts: Keys in Bronze but not Silver
        - updates: Keys in both with different hash
        - unchanged: Keys in both with same hash
        - deletes: Keys in Silver but not Bronze (requires separate call to detect_deletes)

    Example:
        >>> result = compare_row_hashes(
        ...     bronze_with_hash,
        ...     silver_with_hash,
        ...     ["Rel_Id"]
        ... )
        >>> print(f"Inserts: {result['inserts'].count()}")
        >>> print(f"Updates: {result['updates'].count()}")
        >>> print(f"Unchanged: {result['unchanged'].count()}")
    """
    # INSERTS: Keys in Bronze but not Silver
    inserts = bronze_df.select(*business_keys, hash_column).join(
        silver_df.select(*business_keys),
        business_keys,
        "left_anti"
    )

    # UPDATES: Keys in both with different hash
    bronze_keys_hash = bronze_df.select(*business_keys, F.col(hash_column).alias("bronze_hash"))
    silver_keys_hash = silver_df.select(*business_keys, F.col(hash_column).alias("silver_hash"))

    compared = bronze_keys_hash.join(silver_keys_hash, business_keys, "inner")
    updates = compared.where("bronze_hash != silver_hash").select(*business_keys)

    # UNCHANGED: Keys in both with same hash
    unchanged = compared.where("bronze_hash = silver_hash").select(*business_keys)

    return {
        "inserts": inserts,
        "updates": updates,
        "unchanged": unchanged,
    }


def get_cdc_statistics(
    inserts_count: int,
    updates_count: int,
    deletes_count: int,
    unchanged_count: int
) -> dict:
    """
    Calculate CDC statistics summary.

    Args:
        inserts_count: Number of inserted rows
        updates_count: Number of updated rows
        deletes_count: Number of deleted rows
        unchanged_count: Number of unchanged rows

    Returns:
        Dict with statistics including total changes and percentages

    Example:
        >>> stats = get_cdc_statistics(
        ...     inserts_count=100,
        ...     updates_count=50,
        ...     deletes_count=10,
        ...     unchanged_count=840
        ... )
        >>> print(f"Total changes: {stats['total_changes']}")
        >>> print(f"Change rate: {stats['change_rate_pct']:.1f}%")
    """
    total_rows = inserts_count + updates_count + deletes_count + unchanged_count
    total_changes = inserts_count + updates_count + deletes_count

    change_rate_pct = (total_changes / total_rows * 100) if total_rows > 0 else 0

    return {
        "total_rows": total_rows,
        "total_changes": total_changes,
        "inserts": inserts_count,
        "updates": updates_count,
        "deletes": deletes_count,
        "unchanged": unchanged_count,
        "change_rate_pct": change_rate_pct,
    }
