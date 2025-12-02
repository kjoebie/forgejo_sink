"""
Hash Utilities for Bronze and Silver Processing

Provides functions for calculating row-level hashes for Change Data Capture (CDC)
and data quality validation. Uses SHA256 hashing with PySpark for distributed processing.

Author: Albert @ QBIDS
Created: 2025-11-25
"""

from typing import Optional, List, Set
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    sha2, concat_ws, col, coalesce, lit, when, array, concat,
    collect_list, explode, struct, sum as spark_sum
)


def add_row_hash(
    df: DataFrame,
    hash_column: str = "row_hash",
    include_cols: Optional[List[str]] = None,
    exclude_cols: Optional[List[str]] = None,
    null_token: str = "∅",
    separator: str = "|",
    hash_algorithm: str = "sha256"
) -> DataFrame:
    """
    Add a hash column to a DataFrame based on specified columns.
    
    The hash is calculated by:
    1. Converting all columns to strings
    2. Replacing NULLs with a null_token
    3. Concatenating with separator
    4. Applying SHA256 hash
    
    Args:
        df: Input DataFrame
        hash_column: Name of the hash column to add (default: "row_hash")
        include_cols: List of columns to include in hash. If None, uses all columns.
        exclude_cols: List of columns to exclude from hash (applied after include_cols)
        null_token: String to represent NULL values (default: "∅")
        separator: String to separate column values (default: "|")
        hash_algorithm: Hash algorithm - "sha256" or "md5" (default: "sha256")
    
    Returns:
        DataFrame with added hash column
    
    Examples:
        # Hash all columns
        df_hashed = add_row_hash(df)
        
        # Hash specific columns
        df_hashed = add_row_hash(df, include_cols=["id", "name", "email"])
        
        # Hash all except metadata columns
        df_hashed = add_row_hash(df, exclude_cols=["_load_ts", "_filename"])
    """
    
    # Validate hash column doesn't exist
    if hash_column in df.columns:
        raise ValueError(f"Column '{hash_column}' already exists in DataFrame")
    
    # Validate hash algorithm
    if hash_algorithm not in ("sha256", "md5"):
        raise ValueError(f"Unsupported hash_algorithm: {hash_algorithm}. Use 'sha256' or 'md5'.")
    
    # Determine columns to hash
    cols_to_hash = _resolve_hash_columns(
        all_cols=set(df.columns),
        include_cols=include_cols,
        exclude_cols=exclude_cols
    )
    
    if not cols_to_hash:
        raise ValueError("No columns available to hash after include/exclude filters")
    
    # Build hash expression
    # Convert each column to string, handle NULLs, then concatenate
    string_cols = [
        coalesce(col(c).cast("string"), lit(null_token)) 
        for c in sorted(cols_to_hash)
    ]
    
    concatenated = concat_ws(separator, *string_cols)
    
    # Apply hash
    if hash_algorithm == "sha256":
        hash_expr = sha2(concatenated, 256)
    else:  # md5
        hash_expr = sha2(concatenated, 128)  # MD5 = 128-bit
    
    # Add hash column
    return df.withColumn(hash_column, hash_expr)


def add_row_hash_partitioned(
    df: DataFrame,
    business_keys: List[str],
    hash_column: str = "row_hash",
    include_cols: Optional[List[str]] = None,
    exclude_cols: Optional[List[str]] = None,
    null_token: str = "∅",
    separator: str = "|"
) -> DataFrame:
    """
    Add row hash with optimization for large datasets using partitioning.
    
    This function is optimized for scenarios where you're hashing large tables
    and want to leverage Spark's partitioning for better performance.
    
    Args:
        df: Input DataFrame
        business_keys: List of business key columns to partition by
        hash_column: Name of the hash column to add
        include_cols: Columns to include in hash
        exclude_cols: Columns to exclude from hash
        null_token: String to represent NULL values
        separator: String to separate column values
    
    Returns:
        DataFrame with added hash column
    """
    
    # Repartition by business keys for better locality
    df_partitioned = df.repartition(*[col(k) for k in business_keys])
    
    # Add hash
    return add_row_hash(
        df_partitioned,
        hash_column=hash_column,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        null_token=null_token,
        separator=separator
    )


def validate_hash_columns(
    df: DataFrame,
    hash_column: str = "row_hash",
    expected_columns: Optional[List[str]] = None,
    max_rows: Optional[int] = None,
    cache_df: bool = False
) -> bool:
    """
    Validate that hash column exists and has expected properties.

    Args:
        df: DataFrame to validate
        hash_column: Name of hash column to check
        expected_columns: List of columns that should be included in hash
        max_rows: Maximum number of rows to validate. If provided, validation
            runs on a limited sample instead of the full DataFrame, so full
            validation is optional.
        cache_df: Whether to cache the DataFrame within the function before
            running validation. Set to False if caller manages caching.

    Returns:
        True if validation passes
    
    Raises:
        ValueError: If validation fails
    """

    if hash_column not in df.columns:
        raise ValueError(f"Hash column '{hash_column}' not found in DataFrame")

    working_df = df.cache() if cache_df else df
    if max_rows is not None:
        working_df = working_df.limit(max_rows)

    # Check hash column is string type
    hash_type = dict(df.dtypes)[hash_column]
    if hash_type != "string":
        raise ValueError(f"Hash column '{hash_column}' has type '{hash_type}', expected 'string'")

    # Check for NULL hashes (shouldn't happen if hash is calculated correctly)
    null_count = (
        working_df
        .agg(spark_sum(col(hash_column).isNull().cast("int")).alias("null_count"))
        .collect()[0]["null_count"]
        or 0
    )
    if null_count > 0:
        raise ValueError(f"Found {null_count} NULL values in hash column '{hash_column}'")

    # Check hash length (SHA256 = 64 chars, MD5 = 32 chars)
    first_row = working_df.select(hash_column).head(1)
    if not first_row:
        # Kies zelf of je dit ok vindt of juist een error wilt
        raise ValueError("Cannot validate hash length: DataFrame is empty")

    sample_hash = first_row[0][0]
    #sample_hash = df.select(hash_column).first()[0]
    
    if sample_hash:
        hash_length = len(sample_hash)
        if hash_length not in (32, 64):
            raise ValueError(f"Hash length {hash_length} is unexpected (should be 32 or 64)")
    
    return True


def compare_hash_differences(
    df1: DataFrame,
    df2: DataFrame,
    business_keys: List[str],
    hash_column: str = "row_hash"
) -> dict:
    """
    Compare two DataFrames based on business keys and hash values.
    
    Returns statistics about differences (inserts, updates, deletes).
    
    Args:
        df1: First DataFrame (e.g., Bronze/source)
        df2: Second DataFrame (e.g., Silver/target)
        business_keys: List of business key columns
        hash_column: Name of hash column to compare
    
    Returns:
        Dictionary with:
            - inserts: Count of rows in df1 not in df2
            - updates: Count of rows with different hashes
            - deletes: Count of rows in df2 not in df1
            - unchanged: Count of rows with same hash
    """
    
    # Validate hash column exists
    if hash_column not in df1.columns or hash_column not in df2.columns:
        raise ValueError(f"Hash column '{hash_column}' must exist in both DataFrames")
    
    # Select only keys + hash
    df1_subset = df1.select(*business_keys, hash_column).alias("df1")
    df2_subset = df2.select(*business_keys, hash_column).alias("df2")
    
    # Join on business keys
    joined = df1_subset.join(
        df2_subset,
        business_keys,
        "full_outer"
    )
    
    # Classify rows
    classified = joined.withColumn(
        "change_type",
        when(col(f"df2.{hash_column}").isNull(), lit("INSERT"))
        .when(col(f"df1.{hash_column}").isNull(), lit("DELETE"))
        .when(col(f"df1.{hash_column}") != col(f"df2.{hash_column}"), lit("UPDATE"))
        .otherwise(lit("UNCHANGED"))
    )
    
    # Count by type
    counts = classified.groupBy("change_type").count().collect()
    
    result = {
        "inserts": 0,
        "updates": 0,
        "deletes": 0,
        "unchanged": 0
    }
    
    for row in counts:
        change_type = row["change_type"].lower() + "s"
        result[change_type] = row["count"]
    
    return result


def _resolve_hash_columns(
    all_cols: Set[str],
    include_cols: Optional[List[str]],
    exclude_cols: Optional[List[str]]
) -> List[str]:
    """
    Internal function to resolve which columns to include in hash.
    
    Logic:
    1. If include_cols specified: start with those
    2. If include_cols is None: start with all columns
    3. Remove exclude_cols
    4. Sort alphabetically for consistency
    
    Args:
        all_cols: Set of all column names in DataFrame
        include_cols: Optional list of columns to include
        exclude_cols: Optional list of columns to exclude
    
    Returns:
        Sorted list of column names to hash
    
    Raises:
        ValueError: If specified columns don't exist
    """
    
    # Start with include_cols or all columns
    if include_cols is None:
        chosen = set(all_cols)
    else:
        include_set = set(include_cols)
        missing = include_set - all_cols
        if missing:
            raise ValueError(f"Include columns not found in DataFrame: {sorted(missing)}")
        chosen = include_set
    
    # Remove exclude_cols
    if exclude_cols:
        exclude_set = set(exclude_cols)
        missing_ex = exclude_set - all_cols
        if missing_ex:
            raise ValueError(f"Exclude columns not found in DataFrame: {sorted(missing_ex)}")
        chosen -= exclude_set
    
    # Return sorted list for deterministic ordering
    return sorted(chosen)


# Convenience functions for common patterns

def add_business_hash(
    df: DataFrame,
    business_keys: List[str],
    hash_column: str = "row_hash"
) -> DataFrame:
    """
    Add hash excluding common metadata columns.
    
    Automatically excludes columns starting with:
    - _bronze_
    - _silver_
    - _load_
    - _metadata_
    
    Args:
        df: Input DataFrame
        business_keys: Business key columns (always included in hash)
        hash_column: Name of hash column to add
    
    Returns:
        DataFrame with hash column
    """
    
    # Find metadata columns to exclude
    metadata_prefixes = ("_bronze_", "_silver_", "_load_", "_metadata_")
    exclude = [
        c for c in df.columns 
        if any(c.startswith(prefix) for prefix in metadata_prefixes)
    ]
    
    # Add hash, excluding metadata
    return add_row_hash(
        df,
        hash_column=hash_column,
        exclude_cols=exclude
    )


def add_incremental_hash(
    df: DataFrame,
    exclude_timestamp_cols: bool = True,
    hash_column: str = "row_hash"
) -> DataFrame:
    """
    Add hash for incremental tables, optionally excluding timestamp columns.
    
    For incremental tables, you might want to exclude audit timestamp columns
    (like last_modified_date) from the hash to avoid false updates.
    
    Args:
        df: Input DataFrame
        exclude_timestamp_cols: If True, excludes timestamp/date columns
        hash_column: Name of hash column to add
    
    Returns:
        DataFrame with hash column
    """
    
    exclude = []
    
    if exclude_timestamp_cols:
        # Find timestamp/date columns
        timestamp_types = ("timestamp", "date")
        exclude = [
            c for c, dtype in df.dtypes 
            if dtype in timestamp_types
        ]
    
    return add_row_hash(
        df,
        hash_column=hash_column,
        exclude_cols=exclude if exclude else None
    )