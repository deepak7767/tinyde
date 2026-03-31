from tinyde.common_functions_libs.common_functions import *

def get_memory_status(is_sufficient: bool, has_adequate_margin: bool, memory_utilization_pct: float) -> str:
    """Determine memory status including underutilization detection."""
    if not is_sufficient:
        return "CRITICAL"
    elif not has_adequate_margin:
        return "WARNING"
    elif memory_utilization_pct < 25:
        return "UNDERUTILIZED"
    else:
        return "OK"
    
def analyze_memory_sufficiency(write_analyzer_dict):
    """
    Analyze if executor memory is sufficient considering cores (true Spark behavior).
    """

    # Memory per core (actual usable per task)
    memory_per_core_mb = write_analyzer_dict['execution_memory_mb'] / write_analyzer_dict['executor_cores']

    # Partition sizes
    partition_max_size_mb = write_analyzer_dict['max_size'] * write_analyzer_dict['average_row_size']
    partition_avg_size_mb = write_analyzer_dict['mean_size'] * write_analyzer_dict['average_row_size']

    # Required memory (Spark overhead: shuffle, serialization, etc.)
    required_memory_per_partition_mb = partition_max_size_mb * 2.5

    # Utilization (core-level, not executor-level)
    memory_utilization_pct = (required_memory_per_partition_mb / memory_per_core_mb) * 100

    # Safety margin
    memory_safety_margin_pct = 100 - memory_utilization_pct

    # Checks
    is_sufficient = required_memory_per_partition_mb < memory_per_core_mb
    has_adequate_margin = memory_safety_margin_pct >= 20
    is_underutilized = memory_utilization_pct < 25

    # Recommended safe partition size
    safe_partition_size_mb = memory_per_core_mb / 2.5 if memory_per_core_mb > 0 else 0

    return {
        "execution_memory_mb": round(write_analyzer_dict['execution_memory_mb'], 2),
        "memory_per_core_mb": round(memory_per_core_mb, 2),

        "partition_max_size_mb": round(partition_max_size_mb, 2),
        "partition_avg_size_mb": round(partition_avg_size_mb, 2),

        "required_memory_per_partition_mb": round(required_memory_per_partition_mb, 2),

        "memory_utilization_pct": round(memory_utilization_pct, 2),
        "memory_safety_margin_pct": round(memory_safety_margin_pct, 2),

        "safe_partition_size_mb": round(safe_partition_size_mb, 2),

        "is_sufficient": is_sufficient,
        "has_adequate_margin": has_adequate_margin,
        "is_underutilized": is_underutilized,
        "status": get_memory_status(is_sufficient, has_adequate_margin, memory_utilization_pct),

    }

def estimate_write_time(write_analyzer_dict, write_mode):
    """Estimate write operation time."""
    
    total_data_size_mb = write_analyzer_dict["average_row_size"]
    num_partitions = write_analyzer_dict["num_partitions"]
    try:
        total_cores = write_analyzer_dict["executor_cores"] * write_analyzer_dict["max_workers"]
    except KeyError:
        print("Worker Nodes number not found. Cannot estimate wait time")      
        return {}

    # Compression ratio for Delta/Parquet
    compression_ratio = 0.4

    # Write complexity factors
    complexity_factors = {
        "append": 1.0,
        "overwrite": 1.2,
        "merge": 2.5}
    
    if "merge" in write_mode:
        write_complexity = complexity_factors['merge']
    elif write_mode in complexity_factors:
        write_complexity = complexity_factors[write_mode]
    else:
        print(f"Unknown write_mode '{write_mode}', defaulting to append factor")
        write_complexity = complexity_factors['append']

    # Adjusted data size
    adjusted_data_size_mb = total_data_size_mb * compression_ratio * write_complexity

    # Use effective parallelism to calculate throughput
    effective_parallelism = min(num_partitions, total_cores)
    theoretical_throughput_mb_per_sec = effective_parallelism * 10  # 10 MB/sec per core

    estimated_time_sec = adjusted_data_size_mb / theoretical_throughput_mb_per_sec

    # If max partition is much larger than mean, the slowest task dominates the stage
    max_partition_size = write_analyzer_dict['max_size']
    mean_partition_size = write_analyzer_dict['mean_size']
    if mean_partition_size > 0 and max_partition_size > 0:
        skew_ratio = max_partition_size / mean_partition_size
        # Apply penalty when skew_ratio > 2 (straggler effect)
        skew_penalty = max(1.0, 1.0 + (skew_ratio - 2) * 0.15) if skew_ratio > 2 else 1.0
    else:
        skew_penalty = 1.0

    # Wide tables with many columns incur higher serialization/deserialization cost
    num_columns = write_analyzer_dict['num_columns']
    if num_columns > 100:
        schema_complexity_factor = 1.0 + (num_columns - 100) * 0.002  # +0.2% per column beyond 100
    else:
        schema_complexity_factor = 1.0

    # --- Improvement 4: Delta transaction log commit overhead ---
    # Delta writes have a fixed cost for commit protocol (conflict resolution, log write)
    delta_commit_overhead_sec = 2.0  # base commit cost in seconds
    if write_mode == "overwrite":
        delta_commit_overhead_sec += 1.0  # extra for metadata cleanup
    elif "merge" in write_mode:
        delta_commit_overhead_sec += 3.0  # extra for scan + conflict detection

    file_creation_overhead = num_partitions * 0.5
    total_estimated_time_sec = (
        (estimated_time_sec * skew_penalty * schema_complexity_factor)
        + file_creation_overhead
        + delta_commit_overhead_sec
    ) * 1.2  # general overhead buffer

    # Calculate range (±20%)
    min_time_sec = total_estimated_time_sec * 0.8
    max_time_sec = total_estimated_time_sec * 1.2

    return {
        "estimated_range_min_minutes": round(min_time_sec / 60, 2),
        "estimated_range_max_minutes": round(max_time_sec / 60, 2),
        "baseline_throughput_mb_per_sec": theoretical_throughput_mb_per_sec,
        "write_mode": write_mode,
        "effective_parallelism": effective_parallelism
    }
