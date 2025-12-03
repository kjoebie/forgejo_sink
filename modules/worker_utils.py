"""
Worker Optimization Utilities

Provides intelligent worker count selection based on historical execution metrics
from logs.bronze_run_summary table.
"""
import logging
from collections import defaultdict
from typing import Optional
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

def choose_worker_profile_from_history(
    spark: SparkSession,
    source_name: str,
    summary_table: str = "logs.bronze_run_summary",
    default_workers: int = 10,
    min_workers: int = 2,
    max_workers_cap: int = 12,
    lookback_runs: int = 5,
    optimize_for: str = "throughput",  # "throughput" or "efficiency"
    debug: bool = False,
) -> int:
    """
    Pick optimal worker count for the next run based on historical analysis.

    This function analyzes the last N runs for a source and determines the optimal
    worker count by:
    1. Computing throughput (rows/second) OR efficiency for each historical run
    2. Finding the worker count with best average metric
    3. Applying volume-based caps (small datasets don't need many workers)
    4. Gradually adjusting from last run (max ±2 workers per iteration)

    Args:
        spark: Active SparkSession
        source_name: Name of the source to optimize (e.g., "anva_concern")
        summary_table: Fully qualified table name with history (default: "logs.bronze_run_summary")
        default_workers: Default worker count when no history exists (default: 8)
        min_workers: Minimum allowed workers (default: 2)
        max_workers_cap: Maximum allowed workers (default: 12)
        lookback_runs: Number of historical runs to analyze, 1-5 (default: 3)
        optimize_for: Optimization strategy (default: "throughput")
            - "throughput": Maximize rows/second (best for Fabric/serverless)
            - "efficiency": Maximize efficiency percentage (best for on-prem clusters)
        debug: Enable debug logging (default: False)

    Returns:
        int: Recommended worker count for next run

    Example:
        >>> # For Fabric/serverless (optimize for speed)
        >>> MAX_WORKERS = choose_worker_profile_from_history(
        ...     spark=spark,
        ...     source_name="anva_concern",
        ...     default_workers=8,
        ...     min_workers=2,
        ...     max_workers_cap=12,
        ...     lookback_runs=3,
        ...     optimize_for="throughput",  # Default
        ...     debug=True
        ... )
        >>>
        >>> # For on-prem cluster (optimize for resource efficiency)
        >>> MAX_WORKERS = choose_worker_profile_from_history(
        ...     spark=spark,
        ...     source_name="anva_concern",
        ...     optimize_for="efficiency",
        ...     debug=True
        ... )
        >>>
        >>> with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        ...     # Process tables in parallel
    """

    # Clamp lookback between 1 and 5
    lookback_runs = max(1, min(lookback_runs, 5))

    # If no summary table yet → just use default
    if not spark.catalog.tableExists(summary_table):
        if debug:
            logger.info(f"[WORKER_OPTIMIZER] No summary table ({summary_table}), using default {default_workers} workers")
        return int(default_workers)

    base_df = spark.table(summary_table).filter(
        F.col("source") == source_name
    )

    # If there is no data for this source → default
    if base_df.head(1) == []:
        if debug:
            logger.info(f"[WORKER_OPTIMIZER] No history for source={source_name}, using default {default_workers} workers")
        return int(default_workers)

    # Order by run_start if available, otherwise run_ts
    cols = base_df.columns
    if "run_start" in cols:
        base_df = base_df.orderBy(F.col("run_start").desc())
    else:
        base_df = base_df.orderBy(F.col("run_ts").desc())

    hist_df = base_df.limit(lookback_runs).select(
        "workers",
        "efficiency_pct",
        "total_rows",
        "duration_seconds",
    )

    rows = hist_df.collect()
    if not rows:
        return int(default_workers)

    # Last run is first row in the ordering
    last = rows[0]
    last_workers = int(last["workers"]) if last["workers"] is not None else default_workers

    # Build list of valid runs with usable metrics
    history = []
    for r in rows:
        w = r["workers"]
        eff = r["efficiency_pct"]
        tot = r["total_rows"]
        dur = r["duration_seconds"]

        if w is None or dur is None or dur <= 0 or tot is None or tot <= 0:
            continue

        # Skip extremely bad efficiency (<20%) for analysis
        if eff is not None and eff < 20.0:
            continue

        throughput = float(tot) / float(dur)

        # Determine metric based on optimization strategy
        if optimize_for == "efficiency":
            metric = float(eff) if eff is not None else 0.0
        else:  # throughput (default)
            metric = throughput

        history.append(
            {
                "workers": int(w),
                "eff": float(eff) if eff is not None else None,
                "rows": int(tot),
                "duration": float(dur),
                "throughput": throughput,
                "metric": metric,  # The value we optimize for
            }
        )

    # If no usable data remains → keep last_workers or default
    if not history:
        if debug:
            logger.info(f"[WORKER_OPTIMIZER] No usable history for source={source_name}, using last_workers={last_workers}")
        return int(last_workers or default_workers)

    # 1) Determine volume profile (median rows over these runs)
    sorted_by_rows = sorted(history, key=lambda x: x["rows"])
    mid = len(sorted_by_rows) // 2
    if len(sorted_by_rows) % 2 == 1:
        median_rows = sorted_by_rows[mid]["rows"]
    else:
        median_rows = int(
            (sorted_by_rows[mid - 1]["rows"] + sorted_by_rows[mid]["rows"]) / 2
        )

    # 2) Calculate average metric per worker count
    metric_by_workers = defaultdict(list)
    for h in history:
        metric_by_workers[h["workers"]].append(h["metric"])

    avg_metric_by_workers = {
        w: sum(vals) / len(vals) for w, vals in metric_by_workers.items()
    }

    # 3) Choose target_workers based on best metric
    best_metric = max(avg_metric_by_workers.values())
    # Workers within 5% of best metric
    candidate_workers = [
        w for w, metric in avg_metric_by_workers.items() if metric >= 0.95 * best_metric
    ]
    # Choose smallest to save resources
    target_workers = min(candidate_workers)

    # 4) Volume-based caps:
    #    Small runs don't benefit from many workers
    #    Adjust these thresholds based on your data landscape
    #
    #    Note: For Fabric/serverless environments, efficiency may be low even
    #    with good throughput due to cold starts and scheduling overhead.
    #    Focus on throughput (rows/sec) rather than efficiency percentage.
    #
    #    Recommended caps based on data volume:
    if median_rows < 100_000:
        # Very small datasets: max 2 workers (overhead not worth it)
        target_workers = min(target_workers, 2)
    elif median_rows < 1_000_000:
        # Small datasets: max 4 workers
        target_workers = min(target_workers, 4)
    elif median_rows < 10_000_000:
        # Medium datasets: max 8 workers
        target_workers = min(target_workers, 8)
    # For larger volumes (10M+ rows), max_workers_cap is the upper bound

    # 5) Clamp target within global min/max
    target_workers = max(min_workers, min(max_workers_cap, target_workers))

    # 6) Move maximum ±2 workers relative to last run (gradual steps)
    if target_workers > last_workers:
        new_workers = min(last_workers + 2, target_workers, max_workers_cap)
    elif target_workers < last_workers:
        new_workers = max(last_workers - 2, target_workers, min_workers)
    else:
        new_workers = last_workers

    if debug:
        metric_name = "efficiency %" if optimize_for == "efficiency" else "throughput (rows/s)"
        logger.info(
            f"[WORKER_OPTIMIZER] source={source_name}, median_rows={median_rows:,}, "
            f"last_workers={last_workers}, target={target_workers}, new_workers={new_workers}, "
            f"best_{optimize_for}={best_metric:.0f} {metric_name}"
        )

    return int(new_workers)
