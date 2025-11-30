from __future__ import annotations
"""
Path Resolution Utilities for Bronze Pipeline

Provides portable path handling for Fabric (OneLake) and Local (Spark cluster) environments.

Key Features:
- Runtime environment detection
- Parquet path construction (Files/source/year/month/day/run_ts/table/*.parquet)
- Delta table name formatting
- Path validation and existence checks

Author: Albert @ QBIDS
Date: 2025-11-25
"""

import logging
import os
from typing import Optional, List
from pathlib import Path
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

CLUSTER_FILES_ROOT  = "/data/lakehouse/gh_b_avd/lh_gh_bronze/Files"

# ============================================================================
# ENVIRONMENT DETECTION
# ============================================================================

def detect_environment() -> str:
    """
    Detect runtime environment (Fabric or Local).
    
    Returns:
        str: 'fabric' if running in Microsoft Fabric, 'local' otherwise
    
    Examples:
        >>> env = detect_environment()
        >>> logger.info(f"Running in: {env}")
        Running in: local
    """
    if os.path.exists('/lakehouse/default'):
        return 'fabric'
    return 'local'


def get_base_path() -> str:
    """
    Get the base path for Files storage based on environment.
    
    Returns:
        str: '/lakehouse/default/Files' for Fabric, 'Files' for Local
    
    Examples:
        >>> base = get_base_path()
        >>> logger.info(f"Base path: {base}")
        Base path: Files
    """
    return '/lakehouse/default/Files' if detect_environment() == 'fabric' else 'Files'


# ============================================================================
# PARQUET PATH BUILDERS
# ============================================================================

def build_parquet_dir(base_files: str,
                      source_name: str,
                      run_ts: str,
                      table_name: str,
                      spark: SparkSession) -> str:
    """
    Build the directory path for parquet files of a single table and run_ts.
    
    Path structure: {base_files}/{source}/year/month/day/{run_ts}/{table}
    
    Args:
        base_files: Base folder name (e.g., 'greenhouse_sources')
        source_name: Source system name (e.g., 'anva_concern')
        run_ts: Run timestamp in format yyyymmddThhmmss (e.g., '20251125T060000')
        table_name: Table name (e.g., 'Dim_Relatie')
    
    Returns:
        str: Full path to parquet directory
    
    Raises:
        ValueError: If run_ts format is invalid (< 8 characters)
    
    Examples:
        >>> path = build_parquet_dir('greenhouse_sources', 'anva_concern', 
        ...                          '20251125T060000', 'Dim_Relatie')
        >>> logger.info(path)
        Files/greenhouse_sources/anva_concern/2025/11/25/20251125T060000/Dim_Relatie
        
        >>> # In Fabric environment:
        >>> # /lakehouse/default/Files/greenhouse_sources/anva_concern/2025/11/25/20251125T060000/Dim_Relatie
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


def build_parquet_glob(base_files: str,
                       source_name: str,
                       run_ts: str,
                       table_name: str) -> str:
    """
    Build glob pattern for reading all parquet files in a table directory.
    
    Args:
        base_files: Base folder name (e.g., 'greenhouse_sources')
        source_name: Source system name (e.g., 'anva_concern')
        run_ts: Run timestamp (e.g., '20251125T060000')
        table_name: Table name (e.g., 'Dim_Relatie')
    
    Returns:
        str: Glob pattern path (appends '/*.parquet')
    
    Examples:
        >>> glob = build_parquet_glob('greenhouse_sources', 'anva_concern',
        ...                           '20251125T060000', 'Dim_Relatie')
        >>> logger.info(glob)
        Files/greenhouse_sources/anva_concern/2025/11/25/20251125T060000/Dim_Relatie/*.parquet
    """
    parquet_dir = build_parquet_dir(base_files, source_name, run_ts, table_name)
    return f"{parquet_dir}/*.parquet"


# ============================================================================
# DELTA TABLE NAME BUILDERS
# ============================================================================

def build_delta_table_name(delta_schema: str, 
                          table_name: str,
                          table_def: Optional[dict] = None) -> str:
    """
    Build fully qualified Delta table name.
    
    Args:
        delta_schema: Schema/database name (e.g., 'bronze', 'silver')
        table_name: Default table name
        table_def: Optional table definition dict from DAG (may contain 'delta_table' override)
    
    Returns:
        str: Fully qualified table name (schema.table)
    
    Examples:
        >>> # Without override
        >>> name = build_delta_table_name('bronze', 'Dim_Relatie')
        >>> logger.info(name)
        bronze.Dim_Relatie
        
        >>> # With override in table_def
        >>> table_def = {'delta_table': 'custom_name'}
        >>> name = build_delta_table_name('bronze', 'Dim_Relatie', table_def)
        >>> logger.info(name)
        bronze.custom_name
        
        >>> # With schema override
        >>> table_def = {'delta_schema': 'silver', 'delta_table': 'Dim_Relatie'}
        >>> name = build_delta_table_name('bronze', 'Dim_Relatie', table_def)
        >>> logger.info(name)
        silver.Dim_Relatie
    """
    # Check for overrides in table_def
    if table_def:
        target_table = table_def.get('delta_table', table_name)
        target_schema = table_def.get('delta_schema', delta_schema)
    else:
        target_table = table_name
        target_schema = delta_schema
    
    return f"{target_schema}.{target_table}"


# ============================================================================
# WATERMARK PATH BUILDERS
# ============================================================================

def build_watermark_folder(source_name: str, run_id: str) -> str:
    """
    Build watermark runtime folder path for incremental processing.
    
    Path structure: {BASE_PATH}/runtime/{source}/{run_id}/
    
    Args:
        source_name: Source system name (e.g., 'vizier')
        run_id: Run identifier (typically same as run_ts)
    
    Returns:
        str: Full path to watermark folder
    
    Examples:
        >>> path = build_watermark_folder('vizier', '20251125T060000')
        >>> logger.info(path)
        Files/runtime/vizier/20251125T060000/
    """
    base_path = get_base_path()
    return f"{base_path}/runtime/{source_name}/{run_id}/"


def build_config_path(config_relative_path: str) -> str:
    """
    Build full path to config file (DAG, watermarks, etc).
    
    Args:
        config_relative_path: Relative path from Files root (e.g., 'config/dag_vizier_week.json')
    
    Returns:
        str: Full path to config file
    
    Examples:
        >>> path = build_config_path('config/dag_vizier_week.json')
        >>> logger.info(path)
        Files/config/dag_vizier_week.json
        
        >>> # In Fabric:
        >>> # /lakehouse/default/Files/config/dag_vizier_week.json
    """
    base_path = get_base_path()
    return f"{base_path}/{config_relative_path}"


# ============================================================================
# PATH VALIDATION
# ============================================================================

def path_exists(path: str) -> bool:
    """
    Check if a path exists (file or directory).
    
    Args:
        path: Path to check
    
    Returns:
        bool: True if path exists, False otherwise
    
    Examples:
        >>> exists = path_exists('Files/config/dag_vizier_week.json')
        >>> logger.info(f"Config exists: {exists}")
        Config exists: True
    """
    return os.path.exists(path)


def list_run_ts_folders(base_files: str, source_name: str) -> List[str]:
    """
    List all run_ts folders for a given source (for recovery/replay scenarios).
    
    Scans: {BASE_PATH}/{base_files}/{source}/*/*/*/*/
    
    Args:
        base_files: Base folder name (e.g., 'greenhouse_sources')
        source_name: Source system name (e.g., 'anva_concern')
    
    Returns:
        List[str]: Sorted list of run_ts values found
    
    Examples:
        >>> run_ts_list = list_run_ts_folders('greenhouse_sources', 'anva_concern')
        >>> logger.info(run_ts_list[:3])
        ['20251101T060000', '20251108T060000', '20251115T060000']
    """
    base_path = get_base_path()
    source_path = f"{base_path}/{base_files}/{source_name}"
    
    if not os.path.exists(source_path):
        return []
    
    run_ts_set = set()
    
    # Walk through year/month/day structure
    for year in os.listdir(source_path):
        year_path = os.path.join(source_path, year)
        if not os.path.isdir(year_path):
            continue
            
        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue
                
            for day in os.listdir(month_path):
                day_path = os.path.join(month_path, day)
                if not os.path.isdir(day_path):
                    continue
                    
                # List run_ts folders
                for run_ts in os.listdir(day_path):
                    run_ts_path = os.path.join(day_path, run_ts)
                    if os.path.isdir(run_ts_path) and run_ts.startswith('202'):  # Basic validation
                        run_ts_set.add(run_ts)
    
    return sorted(list(run_ts_set))


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def extract_date_from_run_ts(run_ts: str) -> tuple:
    """
    Extract year, month, day from run_ts string.
    
    Args:
        run_ts: Run timestamp (e.g., '20251125T060000')
    
    Returns:
        tuple: (year, month, day) as strings
    
    Raises:
        ValueError: If run_ts format is invalid
    
    Examples:
        >>> year, month, day = extract_date_from_run_ts('20251125T060000')
        >>> logger.info(f"{year}-{month}-{day}")
        2025-11-25
    """
    if not run_ts or len(run_ts) < 8:
        raise ValueError(f"run_ts '{run_ts}' is not in expected yyyymmddThhmmss format")
    
    return run_ts[0:4], run_ts[4:6], run_ts[6:8]


def validate_run_ts_format(run_ts: str) -> bool:
    """
    Validate run_ts format (basic check).
    
    Args:
        run_ts: Run timestamp to validate
    
    Returns:
        bool: True if format looks valid
    
    Examples:
        >>> validate_run_ts_format('20251125T060000')
        True
        >>> validate_run_ts_format('invalid')
        False
    """
    if not run_ts or len(run_ts) < 8:
        return False
    
    try:
        year, month, day = extract_date_from_run_ts(run_ts)
        # Basic range checks
        return (1900 <= int(year) <= 2100 and 
                1 <= int(month) <= 12 and 
                1 <= int(day) <= 31)
    except:
        return False

# ============================================================================
# FILES PATH RESOLUTION
# ============================================================================

def _is_fabric(spark: SparkSession) -> bool:
    """
    Probeer te bepalen of we in Fabric draaien.

    In Fabric hoort de config key 'spark.microsoft.fabric.workspaceId'
    aanwezig te zijn. Bestaat die key niet, dan gooien we een exceptie
    en nemen we aan dat we NIET in Fabric zitten.
    """
    try:
        _ = spark.conf.get("spark.microsoft.fabric.workspaceId")
        return True
    except Exception:
        return False

def resolve_files_path(relative: str, spark: SparkSession) -> str:
    """
    Neem een pad binnen de lakehouse (zoals 'Files/...' of 'Files')
    en geef het juiste fysieke pad terug voor de huidige omgeving.

    - In Fabric:  'Files/...'
    - Op je cluster: '/data/lakehouse/gh_b_avd/lh_gh_bronze/Files/...'
    """

    # leading slash weghalen ("/Files/..." -> "Files/...")
    if relative.startswith("/"):
        relative = relative[1:]

    # We verwachten hier altijd iets met "Files"
    if not relative.startswith("Files"):
        # Onverwacht gebruik: geef het dan gewoon door
        return relative

    if _is_fabric(spark):
        # Spark in Fabric verwacht direct 'Files/...'
        return relative

    # Cluster: map 'Files' naar jouw NFS-root
    if relative == "Files":
        return CLUSTER_FILES_ROOT

    # 'Files/...' -> '/data/lakehouse/.../Files/...'
    suffix = relative[len("Files/"):]
    return f"{CLUSTER_FILES_ROOT}/{suffix}"

# ============================================================================
# MODULE INFO
# ============================================================================

def get_module_info() -> dict:
    """
    Get module information (for debugging/logging).
    
    Returns:
        dict: Module metadata
    """
    return {
        'module': 'path_utils',
        'version': '1.0.0',
        'environment': detect_environment(),
        'base_path': get_base_path(),
        'description': 'Path resolution utilities for Fabric/Local portability'
    }