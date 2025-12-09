"""
Error Detection Utilities

Provides heuristic error detection for common failure scenarios across Bronze, Silver, and Gold layers.
"""

import logging

logger = logging.getLogger(__name__)


def is_missing_path_error(exc: Exception) -> bool:
    """
    Heuristic to detect 'path not found' or 'no files found' errors.

    This helper identifies situations where:
    - Parquet files don't exist
    - Directory path is missing
    - File not found errors

    Args:
        exc: Exception to check

    Returns:
        True if exception indicates missing path/files

    Example:
        >>> try:
        ...     df = spark.read.parquet("/path/not/exist/*.parquet")
        ... except Exception as e:
        ...     if is_missing_path_error(e):
        ...         print("No files found - table not exported")
    """
    msg = str(exc).lower()
    return (
        "path does not exist" in msg
        or "no such file or directory" in msg
        or "file not found" in msg
        or "cannot find path" in msg
        or "path not found" in msg
    )


def is_probably_corrupt_delta(exc: Exception) -> bool:
    """
    Heuristic to detect a broken Delta table that may need to be recreated.

    This helper identifies situations where:
    - Delta table metadata is corrupt
    - Protocol version mismatch
    - Schema merge failures
    - Delta log errors

    Args:
        exc: Exception to check

    Returns:
        True if exception indicates Delta table corruption

    Example:
        >>> try:
        ...     df.write.format("delta").save("/path/to/table")
        ... except Exception as e:
        ...     if is_probably_corrupt_delta(e):
        ...         spark.sql("DROP TABLE IF EXISTS bronze.table")
        ...         # Recreate table
    """
    msg = str(exc).lower()
    return (
        "is not a delta table" in msg
        or "failed to merge fields" in msg
        or "incompatible format" in msg
        or ("protocol" in msg and "unsupported" in msg)
        or ("delta log" in msg and "error" in msg)
        or "cannot find delta log" in msg
    )


def is_schema_mismatch_error(exc: Exception) -> bool:
    """
    Heuristic to detect schema incompatibility errors.

    This helper identifies situations where:
    - Column types don't match
    - Columns are missing
    - Schema evolution failed

    Args:
        exc: Exception to check

    Returns:
        True if exception indicates schema mismatch

    Example:
        >>> try:
        ...     df.write.format("delta").mode("append").save(table)
        ... except Exception as e:
        ...     if is_schema_mismatch_error(e):
        ...         # Enable schema evolution
        ...         df.write.option("mergeSchema", "true").save(table)
    """
    msg = str(exc).lower()
    return (
        "schema mismatch" in msg
        or "cannot resolve" in msg
        or "column not found" in msg
        or "mismatched input" in msg
        or "incompatible schema" in msg
    )


def is_timeout_error(exc: Exception) -> bool:
    """
    Heuristic to detect timeout or resource exhaustion errors.

    This helper identifies situations where:
    - Query timeout
    - Connection timeout
    - OOM errors
    - Resource allocation failures

    Args:
        exc: Exception to check

    Returns:
        True if exception indicates timeout/resource issue

    Example:
        >>> try:
        ...     large_df.count()
        ... except Exception as e:
        ...     if is_timeout_error(e):
        ...         # Retry with more resources or partitioning
    """
    msg = str(exc).lower()
    return (
        "timeout" in msg
        or "timed out" in msg
        or "out of memory" in msg
        or "oom" in msg
        or "resource exhausted" in msg
        or "deadline exceeded" in msg
    )


def is_connection_error(exc: Exception) -> bool:
    """
    Heuristic to detect connection/network errors.

    This helper identifies situations where:
    - Database connection failed
    - Network error
    - Authentication failed
    - Connection refused

    Args:
        exc: Exception to check

    Returns:
        True if exception indicates connection issue

    Example:
        >>> try:
        ...     df = spark.read.jdbc(url, table, props)
        ... except Exception as e:
        ...     if is_connection_error(e):
        ...         # Retry connection
    """
    msg = str(exc).lower()
    return (
        "connection refused" in msg
        or "connection failed" in msg
        or "network error" in msg
        or "cannot connect" in msg
        or "authentication failed" in msg
        or "login failed" in msg
        or "connection timeout" in msg
    )


def classify_error(exc: Exception) -> str:
    """
    Classify an exception into a known error category.

    Args:
        exc: Exception to classify

    Returns:
        Error category string:
        - "MISSING_PATH": Path or files not found
        - "CORRUPT_DELTA": Delta table corruption
        - "SCHEMA_MISMATCH": Schema incompatibility
        - "TIMEOUT": Timeout or resource exhaustion
        - "CONNECTION": Connection or network failure
        - "UNKNOWN": Unknown error type

    Example:
        >>> try:
        ...     process_table(table)
        ... except Exception as e:
        ...     error_type = classify_error(e)
        ...     logger.error(f"Error type: {error_type}, message: {str(e)}")
    """
    if is_missing_path_error(exc):
        return "MISSING_PATH"
    elif is_probably_corrupt_delta(exc):
        return "CORRUPT_DELTA"
    elif is_schema_mismatch_error(exc):
        return "SCHEMA_MISMATCH"
    elif is_timeout_error(exc):
        return "TIMEOUT"
    elif is_connection_error(exc):
        return "CONNECTION"
    else:
        return "UNKNOWN"


def truncate_error_message(error_msg: str, max_length: int = 1000) -> str:
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
        return ""

    if len(error_msg) <= max_length:
        return error_msg

    return error_msg[:max_length] + "... [TRUNCATED]"
