"""
Metadata Generation Utilities

Provides functions for generating DAG configurations from SQL Server metadata.
"""

import re
from typing import Any
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
import logging

logger = logging.getLogger(__name__)


def make_safe_identifier(name: str) -> str:
    """
    Normalize column names for Delta Lake compatibility.

    Removes special characters, converts spaces to underscores,
    ensures valid identifier format.

    Args:
        name: Original column name

    Returns:
        Sanitized column name safe for Delta Lake

    Example:
        >>> make_safe_identifier("Column Name (Special)")
        'Column_Name_Special'
        >>> make_safe_identifier("123Column")
        '_123Column'
    """
    if name is None:
        return ""

    # Remove special characters (keep only alphanumeric, underscore, space)
    cleaned = re.sub(r"[^0-9A-Za-z_ ]+", "", name)

    # Replace spaces with underscores
    cleaned = re.sub(r"\s+", "_", cleaned.strip())

    # Ensure doesn't start with digit
    if cleaned and cleaned[0].isdigit():
        cleaned = f"_{cleaned}"

    return cleaned or name


def column_expression(col: T.Row) -> str:
    """
    Build SQL column expression with proper type casting for SQL Server data types.

    Generates CAST expressions to handle:
    - Decimal precision/scale
    - Money types
    - DateTime variants
    - Text/binary types
    - Special SQL Server types (uniqueidentifier, xml, etc.)

    Args:
        col: Row with column metadata (data_type, numeric_precision, numeric_scale, column_name)

    Returns:
        SQL expression string (e.g., "CAST([column] AS decimal(19,4)) AS [safe_name]")

    Example:
        >>> # Assuming col is a Row with metadata
        >>> expr = column_expression(col)
        >>> # Returns: "CAST([Price] AS decimal(19,4)) AS [Price]"
    """
    dt = (col.data_type or "").lower()
    col_ref = f"[{col.column_name}]"

    # Decimal/Numeric types
    if dt in ("decimal", "numeric"):
        precision = col.numeric_precision or 38
        scale = col.numeric_scale or 18
        expr = f"CAST({col_ref} AS decimal({precision},{scale}))"

    # Money types
    elif dt == "money":
        expr = f"CAST({col_ref} AS decimal(19,4))"
    elif dt == "smallmoney":
        expr = f"CAST({col_ref} AS decimal(10,4))"

    # Integer types
    elif dt == "tinyint":
        expr = f"CAST({col_ref} AS smallint)"
    elif dt in {"smallint", "int", "bigint", "bit", "float", "real"}:
        expr = f"CAST({col_ref} AS {dt})"

    # Date/Time types
    elif dt == "date":
        expr = f"CAST({col_ref} AS date)"
    elif dt == "datetime":
        expr = f"CAST({col_ref} AS datetime2(3))"
    elif dt == "smalldatetime":
        expr = f"CAST({col_ref} AS datetime2(0))"
    elif dt == "datetime2":
        expr = f"CAST({col_ref} AS datetime2(6))"
    elif dt == "time":
        expr = f"CONVERT(varchar(8), {col_ref}, 108)"
    elif dt == "datetimeoffset":
        expr = f"CAST(SWITCHOFFSET({col_ref}, '+00:00') AS datetime2(6))"

    # String types
    elif dt in {"char", "varchar", "nchar", "nvarchar"}:
        expr = col_ref
    elif dt == "text":
        expr = f"CONVERT(varchar(max), {col_ref})"
    elif dt == "ntext":
        expr = f"CONVERT(nvarchar(max), {col_ref})"

    # Special types
    elif dt == "uniqueidentifier":
        expr = f"CONVERT(varchar(36), {col_ref})"
    elif dt == "xml":
        expr = f"CONVERT(nvarchar(max), {col_ref})"

    # Unknown types - pass through
    else:
        expr = col_ref

    # Add alias with safe name
    alias = make_safe_identifier(col.column_name)
    return f"{expr} AS [{alias}]"


def build_base_query(schema_name: str, table_name: str, columns: list) -> str:
    """
    Build SELECT query with proper type casting for all columns.

    Args:
        schema_name: SQL Server schema name
        table_name: SQL Server table name
        columns: List of column metadata Rows (with ordinal_position)

    Returns:
        Complete SELECT query string

    Example:
        >>> query = build_base_query("dbo", "Customers", columns)
        >>> # Returns: "SELECT CAST([Id] AS int) AS [Id], ... FROM [dbo].[Customers]"
    """
    # Sort by ordinal position to maintain column order
    ordered_cols = sorted(columns, key=lambda r: r.ordinal_position or 0)

    # Generate expressions for each column
    select_parts = [column_expression(col) for col in ordered_cols]

    # Join with commas
    select_clause = ",".join(select_parts)

    return f"SELECT {select_clause} FROM [{schema_name}].[{table_name}]"


def load_metadata(spark: SparkSession, path: str) -> DataFrame:
    """
    Load SQL Server metadata from parquet file.

    Args:
        spark: Active SparkSession
        path: Path to metadata parquet file

    Returns:
        DataFrame with metadata, filtered to exclude null table names

    Example:
        >>> metadata = load_metadata(spark, "Files/metadata/connection_vizier_prod_metadata.parquet")
        >>> metadata.show(5)
    """
    return spark.read.parquet(path).filter(F.col("obj_name").isNotNull())


def validate_metadata(df: DataFrame) -> DataFrame:
    """
    Validate metadata DataFrame has required columns and no nulls.

    Checks for:
    - Required columns exist (server_name, db_name, obj_name, column_name)
    - No null values in required columns

    Args:
        df: Metadata DataFrame to validate

    Returns:
        Validated DataFrame (same as input if valid)

    Raises:
        ValueError: If validation fails

    Example:
        >>> validated_df = validate_metadata(metadata_df)
    """
    required_cols = ["server_name", "db_name", "obj_name", "column_name"]

    # Check missing columns
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Required columns are missing: {', '.join(missing)}")

    # Check for nulls in ONE query instead of multiple count() calls
    null_checks = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls")
        for c in required_cols
    ]

    null_counts = df.select(null_checks).first()

    emptycols = [c for c in required_cols if null_counts[f"{c}_nulls"] > 0]
    if emptycols:
        raise ValueError(f"Required columns contain empty values: {', '.join(emptycols)}")

    return df
