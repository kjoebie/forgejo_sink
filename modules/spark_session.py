"""

Spark Session Management Module

Provides centralized Spark session initialization for both cloud platforms
(Microsoft Fabric, Databricks) and local/cluster environments.

Example usage:

    from modules.spark_session import get_or_create_spark_session

    spark = get_or_create_spark_session(

        app_name="MyNotebook",
        enable_hive=True
    )
"""

import logging, os, sys, shutil, tempfile
from typing import Optional, Dict
from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)

def get_or_create_spark_session(

    app_name: str = "DWH_Processing",
    enable_hive: bool = True,
    additional_configs: Optional[Dict[str, str]] = None

) -> SparkSession:

    """

    Get existing Spark session or create a new one with standardized configuration.


    This function detects whether a Spark session already exists (as in Fabric/Databricks)
    or needs to be created (local/cluster environments). It applies standardized
    configurations including Parquet datetime rebase modes.

    Args:

        app_name: Name for the Spark application (used when creating new session)
        enable_hive: Whether to enable Hive support (default: True)
        additional_configs: Optional dictionary of additional Spark configurations

    Returns:

        SparkSession: Configured Spark session

    Example:

        >>> spark = get_or_create_spark_session(
        ...     app_name="DWH_Bronze_Silver_Processing",
        ...     enable_hive=True,
        ...     additional_configs={"spark.sql.shuffle.partitions": "200"}
        ... )

        >>> logger.info(f"Spark version: {spark.version}")

    """

    # Try to get existing Spark session
    try:

        spark = SparkSession.getActiveSession()

        if spark is not None:

            logger.info("✓ Using existing Spark session")
            _apply_standard_configs(spark, additional_configs)
            _log_spark_info(spark)
            return spark

    except Exception as e:

        logger.debug(f"No active session found: {e}")


    # Create new Spark session for local/cluster environments

    logger.info("Creating new Spark session for local/cluster environment...")

 
    builder = SparkSession.builder.appName(app_name)

    if enable_hive:

        builder = builder.enableHiveSupport()
        logger.info("  - Hive support enabled")

    ###
    ## Additional builder configurations can be added here if needed
    ###
    try:
        import mssparkutils  # type: ignore
        
    except ImportError:
        # === LOCAL / CLUSTER ENVIRONMENT ===
        logger.info("  - Detection: Local/Cluster environment detected")

        # A. Python Interpreter (Jouw specifieke VENV)
        # Dit pad moet exact kloppen op zowel de Driver (notebook) als de Worker nodes.
        # Omdat je 'local cluster' (192.168.x.x) waarschijnlijk op dezelfde machine/mount draait, werkt dit.
        forced_python = "/home/sparkadmin/source/repos/dwh_spark_processing/.venv/bin/python"
        
        if os.path.exists(forced_python):
            logger.info(f"  - Python VENV gevonden: {forced_python}")
            os.environ["PYSPARK_PYTHON"] = forced_python
            os.environ["PYSPARK_DRIVER_PYTHON"] = forced_python
        else:
            logger.warning(f"  - ! VENV pad niet gevonden: {forced_python}. Fallback naar sys.executable.")
            os.environ["PYSPARK_PYTHON"] = sys.executable
            os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        # B. Code Distributie (De Oplossing voor ModuleNotFoundError)
        # In plaats van PYTHONPATH te hacken, zippen we de modules en geven die mee aan Spark.
        project_root = _find_project_root()
        modules_path = os.path.join(project_root, "modules")
        
        if os.path.exists(modules_path):
            # Maak een tijdelijke zip van de modules map
            # We maken de zip zodanig dat 'modules' de root folder in de zip is.
            zip_filename = _create_modules_zip(project_root)
            logger.info(f"  - Modules gezipt voor distributie: {zip_filename}")
            
            # Voeg toe aan pyFiles. Spark distribueert dit naar alle workers en voegt toe aan hun pad.
            builder.config("spark.submit.pyFiles", zip_filename)
        else:
            logger.warning(f"  - ! Kan 'modules' map niet vinden in {project_root}")

    spark = builder.getOrCreate()
    _apply_standard_configs(spark, additional_configs)
    _log_spark_info(spark)

    return spark


def _find_project_root() -> str:
    """Probeert slim de root van de repo te vinden."""
    cwd = os.getcwd()
    # Check of we in 'notebooks' zitten, zo ja, ga eentje omhoog
    if os.path.basename(cwd) in ['notebooks', 'config', 'modules']:
        return os.path.dirname(cwd)
    # Check of modules map hier staat
    if os.path.exists(os.path.join(cwd, "modules")):
        return cwd
    # Fallback naar jouw hardcoded pad
    return "/home/sparkadmin/source/repos/dwh_spark_processing"

def _create_modules_zip(project_root: str) -> str:
    """Zipt de modules folder naar een temp file."""
    # We gebruiken temp dir zodat we geen rommel achterlaten in je repo
    temp_dir = tempfile.gettempdir()
    zip_base_name = os.path.join(temp_dir, "dwh_modules_package")
    
    # shutil.make_archive maakt automatisch .zip extensie eraan vast
    # root_dir=project_root, base_dir='modules' zorgt dat de zip intern start met 'modules/...'
    # Dit is cruciaal voor 'from modules import ...'
    zip_path = shutil.make_archive(
        base_name=zip_base_name, 
        format='zip', 
        root_dir=project_root, 
        base_dir='modules'
    )
    return zip_path

def _apply_standard_configs(

    spark: SparkSession,
    additional_configs: Optional[Dict[str, str]] = None

) -> None:

    """

    Apply standardized Spark configurations.

    
    Sets Parquet datetime rebase modes to CORRECTED to handle legacy date/time
    formats correctly. Also applies any additional configurations provided.

    Args:

        spark: SparkSession to configure
        additional_configs: Optional dictionary of additional configurations
    """

    # Standard Parquet datetime configurations
    # These handle legacy Parquet files with old date/time representations

    standard_configs = {

        "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
        "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    }

    # Apply standard configs
    for key, value in standard_configs.items():
        spark.conf.set(key, value)
    logger.debug("  - Standard Parquet configurations applied")


    # Apply additional configs if provided
    if additional_configs:
        for key, value in additional_configs.items():
            spark.conf.set(key, value)
            logger.debug(f"  - Custom config: {key} = {value}")
 

def _log_spark_info(spark: SparkSession) -> None:

    """

    Log information about the Spark session.


    Args:

        spark: SparkSession to log information about

    """

    try:

        logger.info(f"  Spark version: {spark.version}")
        logger.info(f"  Application ID: {spark.sparkContext.applicationId}")
        logger.info(f"  Application name: {spark.sparkContext.appName}")

    except Exception as e:
        logger.warning(f"  Could not retrieve some Spark info: {e}")


def stop_spark_session() -> None:

    """

    Stop the active Spark session.

 
    Use this for cleanup in test environments or when explicitly needed.
    In production notebooks, the session is typically managed by the platform.


    Warning:

        Only use this in local/test environments. Do not stop sessions in
        Fabric/Databricks as they are managed by the platform.

    """

    try:

        spark = SparkSession.getActiveSession()

        if spark is not None:
            spark.stop()
            logger.info("✓ Spark session stopped")

        else:
            logger.info("No active Spark session to stop")

    except Exception as e:
        logger.error(f"Error stopping Spark session: {e}")