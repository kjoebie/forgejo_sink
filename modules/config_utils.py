"""
Configuration Utilities for Bronze and Silver Processing

Provides configuration management for the data pipeline:
- DAG files loading and validation
- Watermarks management (READ-ONLY)
- Runplan scheduling
- Table filtering and query helpers
"""

import json
import os
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


def load_dag(dag_path: str, base_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load and validate a DAG configuration file.

    Args:
        dag_path: Path to DAG JSON file (relative or absolute)
        base_path: Base path for Files directory (optional)

    Returns:
        Dict with DAG configuration

    Raises:
        FileNotFoundError: If DAG file doesn't exist
        ValueError: If DAG validation fails
    """
    # Handle both absolute and relative paths
    if base_path and not os.path.isabs(dag_path):
        if not dag_path.startswith(base_path):
            dag_path = f"{base_path}/{dag_path}"

    if not os.path.exists(dag_path):
        raise FileNotFoundError(f"DAG file not found: {dag_path}")

    with open(dag_path, 'r') as f:
        dag = json.load(f)

    # Validate required fields
    validate_dag(dag)

    return dag


def validate_dag(dag: Dict[str, Any]) -> None:
    """
    Validate DAG structure and required fields.

    Raises:
        ValueError: If validation fails
    """
    # Required top-level fields
    required_fields = ["source", "tables"]

    for field in required_fields:
        if field not in dag:
            raise ValueError(f"DAG missing required field: {field}")

    # Validate source name
    if not dag["source"] or not isinstance(dag["source"], str):
        raise ValueError(f"Invalid source name: {dag.get('source')}")

    # Validate tables
    if not isinstance(dag["tables"], list):
        raise ValueError("DAG 'tables' must be a list")

    if len(dag["tables"]) == 0:
        raise ValueError("DAG has no tables defined")

    # Validate each table
    valid_load_modes = {"snapshot", "incremental", "window"}

    for idx, table in enumerate(dag["tables"]):
        if "name" not in table:
            raise ValueError(f"Table at index {idx} missing 'name' field")

        # Validate load_mode if present
        if "load_mode" in table:
            load_mode = table["load_mode"].lower()
            if load_mode not in valid_load_modes:
                logger.warning(f"Table '{table['name']}' has unsupported load_mode: {load_mode}")


def get_enabled_tables(dag: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Get all enabled tables from DAG.

    A table is enabled if:
    - 'enabled' field is True, 1, or missing (default=enabled)

    Returns:
        List of table definitions
    """
    enabled = []

    for table in dag["tables"]:
        # Default to enabled if field missing
        enabled_flag = table.get("enabled", True)

        # Handle various true values (True, 1, "1", "true")
        if enabled_flag in (True, 1, "1", "true", "True"):
            enabled.append(table)

    return enabled


def filter_retry_tables(
    tables: List[Dict[str, Any]],
    retry_tables: Optional[List[str]]
) -> List[Dict[str, Any]]:
    """
    Filter tables to only those in retry_tables list.

    Args:
        tables: List of table definitions
        retry_tables: List of table names to retry, or None for all

    Returns:
        Filtered list of tables
    """
    if retry_tables is None or len(retry_tables) == 0:
        return tables

    retry_set = set(retry_tables)
    filtered = [t for t in tables if t["name"] in retry_set]

    # Warn about missing tables
    found_names = {t["name"] for t in filtered}
    missing = retry_set - found_names
    if missing:
        logger.warning(f"Retry tables not found in DAG: {sorted(missing)}")

    return filtered


def get_tables_by_load_mode(
    tables: List[Dict[str, Any]],
    load_mode: str
) -> List[Dict[str, Any]]:
    """
    Filter tables by load_mode.

    Args:
        tables: List of table definitions
        load_mode: Load mode to filter (e.g., "incremental", "snapshot")

    Returns:
        Filtered list of tables
    """
    load_mode_lower = load_mode.lower()
    return [
        t for t in tables
        if t.get("load_mode", "snapshot").lower() == load_mode_lower
    ]


def get_tables_to_process(
    dag: Dict[str, Any],
    retry_tables: Optional[List[str]] = None,
    only_enabled: bool = True
) -> List[Dict[str, Any]]:
    """
    Get final list of tables to process based on filters.

    This is the main entry point for determining which tables to load.

    Args:
        dag: DAG configuration
        retry_tables: Optional list of specific tables to retry
        only_enabled: If True, only return enabled tables

    Returns:
        List of table definitions to process
    """
    tables = dag["tables"]

    # Filter by enabled status
    if only_enabled:
        tables = get_enabled_tables(dag)

    # Filter by retry list if provided
    if retry_tables:
        tables = filter_retry_tables(tables, retry_tables)

    return tables


def load_watermarks(watermarks_path: str) -> Dict[str, Any]:
    """
    Load watermarks configuration.

    NOTE: This is READ-ONLY. Watermarks are managed by the data extraction pipeline.

    Args:
        watermarks_path: Path to watermarks.json file

    Returns:
        Dict with watermarks configuration
    """
    if not os.path.exists(watermarks_path):
        raise FileNotFoundError(f"Watermarks file not found: {watermarks_path}")

    with open(watermarks_path, 'r') as f:
        watermarks = json.load(f)

    return watermarks


def get_source_watermarks(
    watermarks_path: str,
    source: str
) -> Optional[Dict[str, Any]]:
    """
    Get watermarks for a specific source.

    Args:
        watermarks_path: Path to watermarks.json file
        source: Source system name (e.g., "vizier", "anva_concern")

    Returns:
        Dict with table watermarks, or None if source not found
    """
    watermarks = load_watermarks(watermarks_path)

    # Watermarks structure: {"source": [{"name": "vizier", "tables": {...}}]}
    sources = watermarks.get("source", [])

    for src in sources:
        if src.get("name") == source:
            return src.get("tables", {})

    return None


def get_table_watermark(
    watermarks_path: str,
    source: str,
    table_name: str
) -> Optional[Any]:
    """
    Get watermark value for a specific table.

    Args:
        watermarks_path: Path to watermarks.json file
        source: Source system name
        table_name: Table name

    Returns:
        Watermark value (string, int, or None)
    """
    source_wm = get_source_watermarks(watermarks_path, source)

    if source_wm is None:
        return None

    return source_wm.get(table_name)


def build_bronze_table_name(
    table_def: Dict[str, Any],
    default_schema: str = "bronze"
) -> str:
    """
    Build full Bronze table name from table definition.

    Args:
        table_def: Table definition from DAG
        default_schema: Default schema if not specified in table_def

    Returns:
        Full table name (schema.table)
    """
    table_name = table_def.get("name")
    delta_table = table_def.get("delta_table", table_name)

    # Check if delta_table already has schema
    if "." in delta_table:
        return delta_table

    # Get schema from table_def or use default
    schema = table_def.get("delta_schema", default_schema)

    return f"{schema}.{delta_table}"


def build_silver_table_name(
    table_def: Dict[str, Any],
    default_schema: str = "silver"
) -> str:
    """
    Build full Silver table name from table definition.

    Args:
        table_def: Table definition from DAG
        default_schema: Default schema if not specified

    Returns:
        Full table name (schema.table)
    """
    # Check if delta_table specifies Silver schema
    delta_table = table_def.get("delta_table")
    if delta_table and delta_table.startswith("silver."):
        return delta_table

    # Otherwise use table name with default Silver schema
    table_name = table_def.get("name")
    return f"{default_schema}.{table_name}"


def get_dag_metadata(dag: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract metadata from DAG.

    Returns:
        Dict with metadata fields
    """
    return {
        "source": dag.get("source"),
        "base_files": dag.get("base_files", "greenhouse_sources"),
        "watermarks_path": dag.get("watermarks_path", "config/watermarks.json"),
        "connection_name": dag.get("connection_name"),
        "defaults": dag.get("defaults", {}),
    }


def get_business_keys(table_def: Dict[str, Any]) -> Optional[List[str]]:
    """
    Get business keys for a table (used for CDC merge).

    Returns:
        List of business key column names, or None if not defined
    """
    return table_def.get("business_keys")


def get_incremental_column(table_def: Dict[str, Any]) -> Optional[str]:
    """
    Get incremental column for a table.

    Returns:
        Column name used for incremental loading, or None
    """
    incremental = table_def.get("incremental", {})
    return incremental.get("column")


def get_window_config(table_def: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Get window configuration for a table.

    Returns:
        Dict with window config (column, granularity, lookback, etc.)
    """
    return table_def.get("window")


def get_partitioning_config(table_def: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Get partitioning configuration for a table.

    Returns:
        Dict with partitioning config (type, year_col, month_col)
    """
    return table_def.get("partitioning")


def summarize_dag(dag: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate summary statistics for a DAG.

    Returns:
        Dict with counts by load_mode, enabled status, etc.
    """
    tables = dag["tables"]
    enabled = get_enabled_tables(dag)

    # Count by load_mode
    load_mode_counts = {}
    for table in enabled:
        mode = table.get("load_mode", "snapshot")
        load_mode_counts[mode] = load_mode_counts.get(mode, 0) + 1

    return {
        "source": dag.get("source"),
        "total_tables": len(tables),
        "enabled_tables": len(enabled),
        "disabled_tables": len(tables) - len(enabled),
        "load_mode_counts": load_mode_counts,
    }


def load_runplan(runplan_path: str) -> List[Dict[str, Any]]:
    """
    Load runplan configuration.

    Args:
        runplan_path: Path to runplan.json file

    Returns:
        List of scheduled runs
    """
    if not os.path.exists(runplan_path):
        logger.warning(f"Runplan file not found: {runplan_path}")
        return []

    with open(runplan_path, 'r') as f:
        runplan = json.load(f)

    return runplan


def get_source_schedule(runplan_path: str, source: str) -> List[Dict[str, Any]]:
    """
    Get schedule entries for a specific source.

    Args:
        runplan_path: Path to runplan.json file
        source: Source system name

    Returns:
        List of schedule entries (may be multiple for weekday/weekend)
    """
    runplan = load_runplan(runplan_path)
    return [entry for entry in runplan if entry.get("source") == source]
