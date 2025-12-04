from __future__ import annotations
"""
Path Resolution Utilities for Bronze Pipeline

Provides portable path handling for Fabric (OneLake) and Local (Spark cluster) environments.

Key Features:
- Runtime environment detection
- Uniform Files-basepath detection (Fabric, cluster glob, local)
- Parquet directory construction (Files/source/year/month/day/run_ts/table)
- Files-path resolution to the correct physical root

Author: Albert @ QBIDS
Date: 2025-11-25
"""

import glob
import logging
import os
from typing import Optional
from pyspark.sql import SparkSession

from modules.constants import CLUSTER_FILES_ROOT

# No module-level logging configuration - let consumers configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# ENVIRONMENT DETECTION
# ============================================================================

def _is_fabric_from_spark(spark: Optional[SparkSession]) -> bool:
    """
    Detect Fabric via Spark configuration if available.

    Args:
        spark: Optional SparkSession

    Returns:
        bool: True when the Fabric workspace config key is present
    """
    if spark is None:
        return False

    try:
        return bool(spark.conf.get("spark.microsoft.fabric.workspaceId"))
    except Exception:
        return False


def detect_environment(spark: Optional[SparkSession] = None) -> str:
    """
    Detect runtime environment (Fabric or Local).

    Checks Spark configuration first (when provided) and falls back to
    filesystem heuristics for notebook/local execution without Spark.

    Returns:
        str: 'fabric' if running in Microsoft Fabric, 'local' otherwise
    
    Examples:
        >>> env = detect_environment()
        >>> logger.info(f"Running in: {env}")
        Running in: local
    """
    if _is_fabric_from_spark(spark) or os.path.exists('/lakehouse/default'):
        return 'fabric'
    return 'local'


def get_base_path_filesystem(spark: Optional[SparkSession] = None) -> str:
    """
    Get the absolute filesystem path to the Files directory.

    This is used for filesystem operations (Python open(), os.path, etc.),
    NOT for Spark operations. Always returns absolute paths.

    Args:
        spark: Optional SparkSession for environment detection

    Returns:
        str: Absolute path to Files directory
    """
    if detect_environment(spark) == 'fabric' or os.path.exists("/lakehouse/default/Files"):
        return "/lakehouse/default/Files"

    cluster_candidates = []
    if os.path.exists(CLUSTER_FILES_ROOT):
        cluster_candidates.append(CLUSTER_FILES_ROOT)

    if os.path.exists('/data/lakehouse'):
        matches = sorted(glob.glob('/data/lakehouse/**/Files', recursive=True))
        cluster_candidates.extend(matches)

    for candidate in cluster_candidates:
        if os.path.exists(candidate):
            return candidate

    # Fallback to relative (for local dev)
    return 'Files'


def get_base_path(spark: Optional[SparkSession] = None) -> str:
    """
    Bepaal het Files-basispad voor Spark operaties.

    Detectievolgorde:
    1) Fabric: Spark-config of het bestaan van `/lakehouse/default/Files`
       -> Retourneer 'Files' (relatief pad voor Spark API)
    2) Cluster: eerst de vaste `CLUSTER_FILES_ROOT`, daarna glob op `/data/lakehouse/**/Files`
    3) Fallback: relatieve `Files` map (bijv. in de repo)

    IMPORTANT: This returns paths suitable for Spark operations (spark.read, etc).
    For filesystem operations (open(), os.path), use get_base_path_filesystem() instead.

    Args:
        spark: Optionele SparkSession voor Fabric-detectie.

    Returns:
        str: Pad naar de Files-root, afgestemd op de omgeving.
    """
    if detect_environment(spark) == 'fabric' or os.path.exists("/lakehouse/default/Files"):
        # In Fabric, Spark expects relative paths starting with 'Files/'
        # NOT absolute paths like /lakehouse/default/Files
        base_path = "Files"
        logger.info("Detected Fabric environment - using relative Spark path: %s", base_path)
        return base_path

    cluster_candidates = []
    if os.path.exists(CLUSTER_FILES_ROOT):
        cluster_candidates.append(CLUSTER_FILES_ROOT)

    if os.path.exists('/data/lakehouse'):
        matches = sorted(glob.glob('/data/lakehouse/**/Files', recursive=True))
        cluster_candidates.extend(matches)
        logger.debug("Detected cluster Files directories: %s", matches)

    for candidate in cluster_candidates:
        if os.path.exists(candidate):
            #logger.info("Detected cluster Files path: %s", candidate)
            return candidate

    logger.info("Falling back to relative Files directory")
    return 'Files'


# ============================================================================
# PARQUET PATH BUILDERS
# ============================================================================

def build_parquet_dir(base_files: str,
                      source_name: str,
                      run_ts: str,
                      table_name: str,
                      spark: Optional[SparkSession] = None) -> str:
    """
    Build the directory path for parquet files of a single table and run_ts.
    
    Path structure: {base_files}/{source}/year/month/day/{run_ts}/{table}
    
    Args:
        base_files: Base folder name (e.g., 'greenhouse_sources')
        source_name: Source system name (e.g., 'anva_concern')
        run_ts: Run timestamp in format yyyymmddThhmmss (e.g., '20251125T060000')
        table_name: Table name (e.g., 'Dim_Relatie')
        spark: Optional SparkSession used for Fabric detection
    
    Returns:
        str: Full path to parquet directory
    
    Raises:
        ValueError: If run_ts format is invalid (< 8 characters)
    
    Examples:
        >>> # In Fabric (relative path):
        >>> path = build_parquet_dir('greenhouse_sources', 'anva_concern',
        ...                          '20251125T060000', 'Dim_Relatie')
        >>> logger.info(path)
        Files/greenhouse_sources/anva_concern/2025/11/25/20251125T060000/Dim_Relatie

        >>> # In Cluster (absolute path):
        >>> # /data/lakehouse/gh_b_avd/lh_gh_bronze/Files/greenhouse_sources/anva_concern/2025/11/25/20251125T060000/Dim_Relatie
    """
    if not run_ts or len(run_ts) < 8:
        raise ValueError(f"run_ts '{run_ts}' is not in expected yyyymmddThhmmss format")
    
    # Extract date components from run_ts
    year = run_ts[0:4]
    month = run_ts[4:6]
    day = run_ts[6:8]
    
    # Get environment-specific base path
    #base_path = get_base_path()
    #return f"{base_path}/{base_files}/{source_name}/{year}/{month}/{day}/{run_ts}/{table_name}"
    
    # Logical path, like in Fabric
    relative_dir = f"Files/{base_files}/{source_name}/{year}/{month}/{day}/{run_ts}/{table_name}"
    return resolve_files_path(relative_dir, spark)


# ============================================================================
# FILES PATH RESOLUTION
# ============================================================================

def resolve_files_path(relative: str, spark: Optional[SparkSession] = None) -> str:
    """
    Converteer een Files-pad naar het juiste fysieke pad per omgeving.

    - Fabric: behoud relatief pad 'Files/...' (Spark API verwacht dit)
    - Cluster: map naar de gevonden Files-root (glob of configuratie)
    - Local: gebruik relatieve 'Files' map

    Args:
        relative: Logical Files path (e.g., 'Files/greenhouse_sources/...')
        spark: Optional SparkSession for environment detection

    Returns:
        str: Environment-specific path for Spark to use

    Examples:
        >>> # In Fabric (relative path for Spark):
        >>> resolve_files_path('Files/greenhouse_sources/anva/2025/11/25/...')
        'Files/greenhouse_sources/anva/2025/11/25/...'

        >>> # In Cluster (absolute path):
        >>> resolve_files_path('Files/greenhouse_sources/anva/2025/11/25/...')
        '/data/lakehouse/gh_b_avd/lh_gh_bronze/Files/greenhouse_sources/anva/2025/11/25/...'
    """

    original = relative

    # leading slash weghalen alleen voor Files-paths ("/Files/..." -> "Files/..." )
    if relative.startswith("/"):
        relative = relative[1:]

    # We verwachten hier altijd iets met "Files"
    if not relative.startswith("Files"):
        # Onverwacht gebruik: geef het dan gewoon door
        return original

    # Get the absolute base path for all environments (including Fabric)
    base_path = get_base_path(spark)

    environment = detect_environment(spark)
    logger.debug(
        "Resolving Files path '%s' using base path '%s' in env '%s'",
        relative,
        base_path,
        environment,
    )

    if relative == "Files":
        return base_path

    # Extract suffix after 'Files'
    suffix = relative[len("Files"):]
    if suffix.startswith('/'):
        suffix = suffix[1:]

    # Build absolute path
    if base_path.endswith('/'):
        return f"{base_path}{suffix}"
    return f"{base_path}/{suffix}"