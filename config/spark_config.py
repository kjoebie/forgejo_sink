"""
Spark Session Configuration Module

Compatible with Fabric Runtime 1.3 (Spark 3.5, Python 3.11)
"""
from typing import Optional
from pyspark.sql import SparkSession
import os


def create_spark_session(
    app_name: str = "DWH_Spark_Processing",
    master: Optional[str] = None,
    executor_memory: str = "4g",
    executor_cores: int = 2,
    max_cores: int = 6
) -> SparkSession:
    """
    Create Spark session configured for vanilla Spark cluster
    
    Args:
        app_name: Application name
        master: Spark master URL (default: spark://master-ip:7077 from env or local[*])
        executor_memory: Memory per executor
        executor_cores: Cores per executor
        max_cores: Maximum cores to use across cluster
        
    Returns:
        SparkSession object
    """
    # Get master URL from environment or parameter
    if master is None:
        master = os.getenv("SPARK_MASTER_URL", "local[*]")
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.executor.cores", str(executor_cores)) \
        .config("spark.cores.max", str(max_cores)) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    return builder.getOrCreate()


def get_or_create_spark_session(app_name: str = "DWH_Spark_Processing") -> SparkSession:
    """
    Get existing Spark session or create new one
    Singleton pattern for notebook environments
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def stop_spark_session(spark: Optional[SparkSession] = None):
    """Stop the Spark session"""
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    if spark is not None:
        spark.stop()