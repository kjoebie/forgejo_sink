"""
Silver Layer CDC Processing

Provides core Silver CDC merge logic for incremental table updates from Bronze to Silver.

Architecture: CDC with Soft Deletes
- INSERT: New keys from Bronze
- UPDATE: Changed rows (detected via hash)
- DELETE: Keys missing from Bronze (soft delete with is_deleted flag)
- UNCHANGED: Rows with same hash
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from datetime import datetime
from typing import Dict, Any, List, Optional
from uuid import uuid4
import logging

from modules.cdc_utils import reconstruct_bronze_current_state, get_business_columns
from modules.hash_utils import add_row_hash

logger = logging.getLogger(__name__)


def process_silver_cdc_merge(
    spark: SparkSession,
    table_def: Dict[str, Any],
    source_name: str,
    run_id: str,
    run_ts: str,
    debug: bool = False
) -> Dict[str, Any]:
    """
    Perform CDC merge from Bronze to Silver for a single table.

    Args:
        spark: Active SparkSession
        table_def: Table definition from DAG
        source_name: Source system name
        run_id: Unique run identifier
        run_ts: Run timestamp
        debug: Enable debug output

    Returns:
        Dict with CDC merge results:
        - log_id, run_id, run_ts, source, table_name, load_mode
        - status (SUCCESS, FAILED, SKIPPED)
        - rows_inserted, rows_updated, rows_deleted, rows_unchanged
        - total_silver_rows, bronze_rows
        - bronze_table, silver_table
        - start_time, end_time, duration_seconds
        - error_message
    """

    # Validate
    table_name = table_def.get("name")
    if not table_name:
        raise ValueError("table_def is missing 'name'")

    # Get business keys (required for CDC)
    business_keys = table_def.get("business_keys")
    if not business_keys:
        # Cannot do CDC without business keys
        log_id = f"{source_name}:{table_name}:{run_ts}:silver:{uuid4().hex[:8]}"
        start_time = datetime.utcnow()
        end_time = datetime.utcnow()

        if debug:
            logger.info(f"[{table_name}] SKIPPED: No business_keys defined (required for CDC)")

        return {
            "log_id": log_id,
            "run_id": run_id,
            "run_ts": run_ts,
            "source": source_name,
            "table_name": table_name,
            "load_mode": table_def.get("load_mode"),
            "status": "SKIPPED",
            "rows_inserted": None,
            "rows_updated": None,
            "rows_deleted": None,
            "rows_unchanged": None,
            "total_silver_rows": None,
            "bronze_rows": None,
            "bronze_table": None,
            "silver_table": None,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": 0,
            "error_message": "Business keys not defined in table config",
        }

    # Initialize metrics
    log_id = f"{source_name}:{table_name}:{run_ts}:silver:{uuid4().hex[:8]}"
    start_time = datetime.utcnow()
    load_mode = table_def.get("load_mode", "snapshot").lower()

    # Build table names
    bronze_schema = table_def.get("delta_schema", "bronze")
    bronze_target = table_def.get("delta_table", table_name)
    if "." not in bronze_target:
        bronze_table_full = f"{bronze_schema}.{bronze_target}"
    else:
        bronze_table_full = bronze_target

    # Silver table name
    if bronze_target.startswith("silver."):
        silver_table_full = bronze_target
    else:
        silver_table_full = f"silver.{table_name}"

    if debug:
        logger.info(f"\n[{table_name}] Starting Silver CDC merge ({load_mode})")
        logger.info(f"  Bronze: {bronze_table_full}")
        logger.info(f"  Silver: {silver_table_full}")
        logger.info(f"  Business keys: {business_keys}")

    try:
        # ====================================================================
        # STEP 1: Reconstruct Bronze Current State
        # ====================================================================

        if load_mode == "incremental":
            # Reconstruct current state from Bronze history
            bronze_current = reconstruct_bronze_current_state(
                spark,
                bronze_table_full,
                business_keys,
                run_ts
            )

            if debug:
                bronze_count = bronze_current.count()
                logger.info(f"  Reconstructed Bronze state: {bronze_count:,} rows")
        else:
            # Snapshot/window: Bronze contains current state
            bronze_current = spark.table(bronze_table_full)

            if debug:
                bronze_count = bronze_current.count()
                logger.info(f"  Bronze snapshot: {bronze_count:,} rows")

        # Get business columns only (exclude metadata)
        business_cols = get_business_columns(bronze_current)

        # ====================================================================
        # STEP 2: Add Row Hash
        # ====================================================================

        bronze_with_hash = add_row_hash(
            bronze_current,
            hash_column="row_hash",
            include_cols=business_cols,
            exclude_cols=None
        )

        if debug:
            logger.info(f"  Added row_hash to Bronze")

        # ====================================================================
        # STEP 3: Ensure Silver Table Exists
        # ====================================================================

        if not spark.catalog.tableExists(silver_table_full):
            # Create Silver table with initial data
            if debug:
                logger.info(f"  Creating new Silver table: {silver_table_full}")

            # Add Silver metadata columns
            silver_initial = bronze_with_hash \
                .withColumn("_silver_inserted_ts", lit(run_ts)) \
                .withColumn("_silver_updated_ts", lit(run_ts)) \
                .withColumn("_silver_deleted_ts", lit(None).cast("string")) \
                .withColumn("is_deleted", lit(False))

            # Write initial data
            silver_initial.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(silver_table_full)

            rows_inserted = bronze_with_hash.count()

            end_time = datetime.utcnow()
            duration = int((end_time - start_time).total_seconds())

            if debug:
                logger.info(f"  Created Silver table with {rows_inserted:,} rows")

            return {
                "log_id": log_id,
                "run_id": run_id,
                "run_ts": run_ts,
                "source": source_name,
                "table_name": table_name,
                "load_mode": load_mode,
                "status": "SUCCESS",
                "rows_inserted": rows_inserted,
                "rows_updated": 0,
                "rows_deleted": 0,
                "rows_unchanged": 0,
                "total_silver_rows": rows_inserted,
                "bronze_rows": rows_inserted,
                "bronze_table": bronze_table_full,
                "silver_table": silver_table_full,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "error_message": None,
            }

        # ====================================================================
        # STEP 4: MERGE (INSERT + UPDATE)
        # ====================================================================

        silver_delta = DeltaTable.forName(spark, silver_table_full)

        # Build merge condition
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in business_keys
        ])

        # Add Silver metadata to source
        bronze_for_merge = bronze_with_hash \
            .withColumn("_silver_updated_ts", lit(run_ts)) \
            .withColumn("_silver_inserted_ts", lit(run_ts)) \
            .withColumn("_silver_deleted_ts", lit(None).cast("string")) \
            .withColumn("is_deleted", lit(False))

        # Prepare update/insert column mappings
        all_cols = bronze_for_merge.columns

        update_set = {col: f"source.{col}" for col in all_cols}
        update_set["_silver_updated_ts"] = f"source._silver_updated_ts"
        # Keep original _silver_inserted_ts for updates
        update_set["_silver_inserted_ts"] = "target._silver_inserted_ts"

        insert_values = {col: f"source.{col}" for col in all_cols}

        # Execute MERGE
        if debug:
            logger.info(f"  Executing MERGE...")

        merge_builder = silver_delta.alias("target").merge(
            bronze_for_merge.alias("source"),
            merge_condition
        )

        # UPDATE: when hashes differ
        merge_builder = merge_builder.whenMatchedUpdate(
            condition="target.row_hash != source.row_hash",
            set=update_set
        )

        # INSERT: when not matched
        merge_builder = merge_builder.whenNotMatchedInsert(
            values=insert_values
        )

        # Execute
        merge_result = merge_builder.execute()

        if debug:
            logger.info(f"  MERGE completed")

        # ====================================================================
        # STEP 5: DELETE Detection (Incremental only)
        # ====================================================================

        rows_deleted = 0

        if load_mode == "incremental":
            if debug:
                logger.info(f"  Detecting deletes...")

            # Get active keys from Silver
            silver_current = spark.table(silver_table_full).where("is_deleted = false")

            # Find keys in Silver but not in Bronze (LEFT ANTI join)
            deleted_keys = silver_current.select(*business_keys).join(
                bronze_with_hash.select(*business_keys),
                business_keys,
                "left_anti"
            )

            deleted_count = deleted_keys.count()

            if deleted_count > 0:
                if debug:
                    logger.info(f"  Found {deleted_count:,} deleted keys")

                # Mark as deleted (soft delete)
                delete_merge_condition = " AND ".join([
                    f"target.{key} = source.{key}" for key in business_keys
                ])

                silver_delta.alias("target").merge(
                    deleted_keys.alias("source"),
                    delete_merge_condition
                ).whenMatchedUpdate(
                    set={
                        "is_deleted": "true",
                        "_silver_deleted_ts": f"'{run_ts}'"
                    }
                ).execute()

                rows_deleted = deleted_count
            else:
                if debug:
                    logger.info(f"  No deletes detected")

        # ====================================================================
        # STEP 6: Calculate Statistics
        # ====================================================================

        # Total Silver rows (including deleted)
        total_silver_rows = spark.table(silver_table_full).count()

        # Bronze rows processed
        bronze_rows = bronze_with_hash.count()

        # Simplified metrics (exact counts would require tracking during MERGE)
        rows_inserted = None
        rows_updated = None
        rows_unchanged = None

        # Status
        status = "SUCCESS"
        error_message = None

    except Exception as e:
        status = "FAILED"
        error_message = str(e)[:500]

        rows_inserted = None
        rows_updated = None
        rows_deleted = 0
        rows_unchanged = None
        total_silver_rows = None
        bronze_rows = None

        if debug:
            logger.info(f"[{table_name}] FAILED: {str(e)[:200]}")

    # ========================================================================
    # STEP 7: Return Results
    # ========================================================================

    end_time = datetime.utcnow()
    duration = int((end_time - start_time).total_seconds())

    if debug and status == "SUCCESS":
        logger.info(f"[{table_name}] SUCCESS in {duration}s")
        logger.info(f"  Silver rows: {total_silver_rows:,}")
        logger.info(f"  Deleted: {rows_deleted}")

    return {
        "log_id": log_id,
        "run_id": run_id,
        "run_ts": run_ts,
        "source": source_name,
        "table_name": table_name,
        "load_mode": load_mode,
        "status": status,
        "rows_inserted": rows_inserted,
        "rows_updated": rows_updated,
        "rows_deleted": rows_deleted,
        "rows_unchanged": rows_unchanged,
        "total_silver_rows": total_silver_rows,
        "bronze_rows": bronze_rows,
        "bronze_table": bronze_table_full,
        "silver_table": silver_table_full,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration,
        "error_message": error_message,
    }
