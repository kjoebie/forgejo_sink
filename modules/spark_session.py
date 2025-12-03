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

import logging
from typing import Optional, Dict, Any
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

    spark = builder.getOrCreate()

    logger.info("✓ Spark session created")

 
    # Apply standard configurations
    _apply_standard_configs(spark, additional_configs)

    # Log session info
    _log_spark_info(spark)

    return spark

 
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