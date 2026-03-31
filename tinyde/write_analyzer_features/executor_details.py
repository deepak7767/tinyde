from tinyde.common_functions_libs.common_functions import *
# Common instance types used in Databricks (AWS + Azure) -> vCPU count
INSTANCE_CORES = {# General Purpose - M4
    "m4.large": 2, "m4.xlarge": 4, "m4.2xlarge": 8, "m4.4xlarge": 16,
    "m4.10xlarge": 40, "m4.16xlarge": 64,
    # General Purpose - M5
    "m5.large": 2, "m5.xlarge": 4, "m5.2xlarge": 8, "m5.4xlarge": 16,
    "m5.8xlarge": 32, "m5.12xlarge": 48, "m5.16xlarge": 64, "m5.24xlarge": 96,
    # General Purpose - M5d
    "m5d.large": 2, "m5d.xlarge": 4, "m5d.2xlarge": 8, "m5d.4xlarge": 16,
    "m5d.8xlarge": 32, "m5d.12xlarge": 48, "m5d.16xlarge": 64, "m5d.24xlarge": 96,
    # General Purpose - M5a
    "m5a.large": 2, "m5a.xlarge": 4, "m5a.2xlarge": 8, "m5a.4xlarge": 16,
    "m5a.8xlarge": 32, "m5a.12xlarge": 48, "m5a.16xlarge": 64, "m5a.24xlarge": 96,
    # General Purpose - M6i
    "m6i.large": 2, "m6i.xlarge": 4, "m6i.2xlarge": 8, "m6i.4xlarge": 16,
    "m6i.8xlarge": 32, "m6i.12xlarge": 48, "m6i.16xlarge": 64, "m6i.24xlarge": 96,
    # General Purpose - M6g (Graviton)
    "m6g.large": 2, "m6g.xlarge": 4, "m6g.2xlarge": 8, "m6g.4xlarge": 16,
    "m6g.8xlarge": 32, "m6g.12xlarge": 48, "m6g.16xlarge": 64,
    # Memory Optimized - R4
    "r4.large": 2, "r4.xlarge": 4, "r4.2xlarge": 8, "r4.4xlarge": 16,
    "r4.8xlarge": 32, "r4.16xlarge": 64,
    # Memory Optimized - R5
    "r5.large": 2, "r5.xlarge": 4, "r5.2xlarge": 8, "r5.4xlarge": 16,
    "r5.8xlarge": 32, "r5.12xlarge": 48, "r5.16xlarge": 64, "r5.24xlarge": 96,
    # Memory Optimized - R5d
    "r5d.large": 2, "r5d.xlarge": 4, "r5d.2xlarge": 8, "r5d.4xlarge": 16,
    "r5d.8xlarge": 32, "r5d.12xlarge": 48, "r5d.16xlarge": 64, "r5d.24xlarge": 96,
    # Memory Optimized - R5a
    "r5a.large": 2, "r5a.xlarge": 4, "r5a.2xlarge": 8, "r5a.4xlarge": 16,
    "r5a.8xlarge": 32, "r5a.12xlarge": 48, "r5a.16xlarge": 64, "r5a.24xlarge": 96,
    # Memory Optimized - R6i
    "r6i.large": 2, "r6i.xlarge": 4, "r6i.2xlarge": 8, "r6i.4xlarge": 16,
    "r6i.8xlarge": 32, "r6i.12xlarge": 48, "r6i.16xlarge": 64, "r6i.24xlarge": 96,
    # Storage Optimized - I3
    "i3.large": 2, "i3.xlarge": 4, "i3.2xlarge": 8, "i3.4xlarge": 16,
    "i3.8xlarge": 32, "i3.16xlarge": 64,
    # Storage Optimized - I3en
    "i3en.large": 2, "i3en.xlarge": 4, "i3en.2xlarge": 8, "i3en.3xlarge": 12,
    "i3en.6xlarge": 24, "i3en.12xlarge": 48, "i3en.24xlarge": 96,
    # Storage Optimized - I4i
    "i4i.large": 2, "i4i.xlarge": 4, "i4i.2xlarge": 8, "i4i.4xlarge": 16,
    "i4i.8xlarge": 32, "i4i.16xlarge": 64, "i4i.32xlarge": 128,
    # Compute Optimized - C5
    "c5.large": 2, "c5.xlarge": 4, "c5.2xlarge": 8, "c5.4xlarge": 16,
    "c5.9xlarge": 36, "c5.12xlarge": 48, "c5.18xlarge": 72, "c5.24xlarge": 96,
    # Compute Optimized - C5d
    "c5d.large": 2, "c5d.xlarge": 4, "c5d.2xlarge": 8, "c5d.4xlarge": 16,
    "c5d.9xlarge": 36, "c5d.12xlarge": 48, "c5d.18xlarge": 72, "c5d.24xlarge": 96,
    # Compute Optimized - C6i
    "c6i.large": 2, "c6i.xlarge": 4, "c6i.2xlarge": 8, "c6i.4xlarge": 16,
    "c6i.8xlarge": 32, "c6i.12xlarge": 48, "c6i.16xlarge": 64, "c6i.24xlarge": 96,
    # GPU - P3
    "p3.2xlarge": 8, "p3.8xlarge": 32, "p3.16xlarge": 64,
    # GPU - G4dn
    "g4dn.xlarge": 4, "g4dn.2xlarge": 8, "g4dn.4xlarge": 16, "g4dn.8xlarge": 32,
    "g4dn.12xlarge": 48, "g4dn.16xlarge": 64,
    # GPU - G5
    "g5.xlarge": 4, "g5.2xlarge": 8, "g5.4xlarge": 16, "g5.8xlarge": 32,
    "g5.12xlarge": 48, "g5.16xlarge": 64, "g5.24xlarge": 96, "g5.48xlarge": 192,

    # ===== Azure VM Instance Types =====
    # General Purpose - Ds_v3
    "Standard_D2s_v3": 2, "Standard_D4s_v3": 4, "Standard_D8s_v3": 8,
    "Standard_D16s_v3": 16, "Standard_D32s_v3": 32, "Standard_D48s_v3": 48,
    "Standard_D64s_v3": 64,
    # General Purpose - Ds_v4
    "Standard_D2s_v4": 2, "Standard_D4s_v4": 4, "Standard_D8s_v4": 8,
    "Standard_D16s_v4": 16, "Standard_D32s_v4": 32, "Standard_D48s_v4": 48,
    "Standard_D64s_v4": 64,
    # General Purpose - Ds_v5
    "Standard_D2s_v5": 2, "Standard_D4s_v5": 4, "Standard_D8s_v5": 8,
    "Standard_D16s_v5": 16, "Standard_D32s_v5": 32, "Standard_D48s_v5": 48,
    "Standard_D64s_v5": 64, "Standard_D96s_v5": 96,
    # General Purpose - Dads_v5 (AMD)
    "Standard_D2ads_v5": 2, "Standard_D4ads_v5": 4, "Standard_D8ads_v5": 8,
    "Standard_D16ads_v5": 16, "Standard_D32ads_v5": 32, "Standard_D48ads_v5": 48,
    "Standard_D64ads_v5": 64, "Standard_D96ads_v5": 96,
    # General Purpose - Das_v4 (AMD)
    "Standard_D2as_v4": 2, "Standard_D4as_v4": 4, "Standard_D8as_v4": 8,
    "Standard_D16as_v4": 16, "Standard_D32as_v4": 32, "Standard_D48as_v4": 48,
    "Standard_D64as_v4": 64, "Standard_D96as_v4": 96,
    # General Purpose - Dds_v4
    "Standard_D2ds_v4": 2, "Standard_D4ds_v4": 4, "Standard_D8ds_v4": 8,
    "Standard_D16ds_v4": 16, "Standard_D32ds_v4": 32, "Standard_D48ds_v4": 48,
    "Standard_D64ds_v4": 64,
    # General Purpose - Dds_v5
    "Standard_D2ds_v5": 2, "Standard_D4ds_v5": 4, "Standard_D8ds_v5": 8,
    "Standard_D16ds_v5": 16, "Standard_D32ds_v5": 32, "Standard_D48ds_v5": 48,
    "Standard_D64ds_v5": 64, "Standard_D96ds_v5": 96,
    # Memory Optimized - Es_v3
    "Standard_E2s_v3": 2, "Standard_E4s_v3": 4, "Standard_E8s_v3": 8,
    "Standard_E16s_v3": 16, "Standard_E32s_v3": 32, "Standard_E64s_v3": 64,
    # Memory Optimized - Es_v4
    "Standard_E2s_v4": 2, "Standard_E4s_v4": 4, "Standard_E8s_v4": 8,
    "Standard_E16s_v4": 16, "Standard_E20s_v4": 20, "Standard_E32s_v4": 32,
    "Standard_E48s_v4": 48, "Standard_E64s_v4": 64,
    # Memory Optimized - Es_v5
    "Standard_E2s_v5": 2, "Standard_E4s_v5": 4, "Standard_E8s_v5": 8,
    "Standard_E16s_v5": 16, "Standard_E20s_v5": 20, "Standard_E32s_v5": 32,
    "Standard_E48s_v5": 48, "Standard_E64s_v5": 64, "Standard_E96s_v5": 96,
    # Memory Optimized - Eads_v5 (AMD)
    "Standard_E2ads_v5": 2, "Standard_E4ads_v5": 4, "Standard_E8ads_v5": 8,
    "Standard_E16ads_v5": 16, "Standard_E32ads_v5": 32, "Standard_E48ads_v5": 48,
    "Standard_E64ads_v5": 64, "Standard_E96ads_v5": 96,
    # Memory Optimized - Eas_v4 (AMD)
    "Standard_E2as_v4": 2, "Standard_E4as_v4": 4, "Standard_E8as_v4": 8,
    "Standard_E16as_v4": 16, "Standard_E20as_v4": 20, "Standard_E32as_v4": 32,
    "Standard_E48as_v4": 48, "Standard_E64as_v4": 64, "Standard_E96as_v4": 96,
    # Memory Optimized - Eds_v4
    "Standard_E2ds_v4": 2, "Standard_E4ds_v4": 4, "Standard_E8ds_v4": 8,
    "Standard_E16ds_v4": 16, "Standard_E20ds_v4": 20, "Standard_E32ds_v4": 32,
    "Standard_E48ds_v4": 48, "Standard_E64ds_v4": 64,
    # Memory Optimized - Eds_v5
    "Standard_E2ds_v5": 2, "Standard_E4ds_v5": 4, "Standard_E8ds_v5": 8,
    "Standard_E16ds_v5": 16, "Standard_E20ds_v5": 20, "Standard_E32ds_v5": 32,
    "Standard_E48ds_v5": 48, "Standard_E64ds_v5": 64, "Standard_E96ds_v5": 96,
    # Storage Optimized - Ls_v2
    "Standard_L8s_v2": 8, "Standard_L16s_v2": 16, "Standard_L32s_v2": 32,
    "Standard_L48s_v2": 48, "Standard_L64s_v2": 64, "Standard_L80s_v2": 80,
    # Storage Optimized - Ls_v3
    "Standard_L8s_v3": 8, "Standard_L16s_v3": 16, "Standard_L32s_v3": 32,
    "Standard_L48s_v3": 48, "Standard_L64s_v3": 64, "Standard_L80s_v3": 80,
    # Compute Optimized - Fs_v2
    "Standard_F2s_v2": 2, "Standard_F4s_v2": 4, "Standard_F8s_v2": 8,
    "Standard_F16s_v2": 16, "Standard_F32s_v2": 32, "Standard_F48s_v2": 48,
    "Standard_F64s_v2": 64, "Standard_F72s_v2": 72,
    # GPU - NCv3 (V100)
    "Standard_NC6s_v3": 6, "Standard_NC12s_v3": 12, "Standard_NC24s_v3": 24,
    # GPU - NCas_T4_v3
    "Standard_NC4as_T4_v3": 4, "Standard_NC8as_T4_v3": 8,
    "Standard_NC16as_T4_v3": 16, "Standard_NC64as_T4_v3": 64,
    # GPU - NCads_A100_v4
    "Standard_NC24ads_A100_v4": 24, "Standard_NC48ads_A100_v4": 48,
    "Standard_NC96ads_A100_v4": 96,
    # GPU - NDasrA100_v4 (A100 80GB)
    "Standard_ND96asr_v4": 96}


def get_spark_conf(key, default=None):
    """Retrieve a Spark config value using SET."""
    spark = get_or_create_spark()
    try:
        row = spark.sql(f"SET {key}").first()
        value = row["value"]
        if value is None or value.endswith("is not set"):
            return default
        return value
    except Exception:
        return default

def parse_memory(mem_str):
    """Parse memory string like '4g' or '1024m' to MB."""
    if not mem_str:
        return 0
    mem = mem_str.strip().lower()
    if mem.endswith("g"):
        return int(float(mem[:-1]) * 1024)
    elif mem.endswith("m"):
        return int(float(mem[:-1]))
    return int(mem)

def get_max_workers():
    """Get max workers from classic cluster autoscaling or fixed-size config."""
    max_w = get_spark_conf("spark.databricks.clusterUsageTags.clusterMaxWorkers")
    if max_w:
        return int(max_w)
    # Fixed-size cluster: clusterWorkers is the fixed count
    fixed = get_spark_conf("spark.databricks.clusterUsageTags.clusterWorkers")
    if fixed:
        return int(fixed)
    return None

def get_executor_cores():
    """Resolve executor cores from Spark config, instance type lookup, or CPU count."""
    # 1. Try looking up the worker node instance type from the dictionary
    for conf_key in [
        "spark.databricks.clusterUsageTags.clusterWorkerInstanceType",
        "spark.databricks.clusterUsageTags.clusterNodeType",
        "spark.databricks.workerNodeTypeId",
    ]:
        node_type = get_spark_conf(conf_key)
        if node_type and node_type in INSTANCE_CORES:
            return INSTANCE_CORES[node_type]

    # 2. Fallback to multiprocessing.cpu_count()
    return multiprocessing.cpu_count()

def compute_memory_splits(executor_heap_mb):
    """Compute unified and storage memory from executor JVM heap."""
    memory_fraction = (get_spark_conf("spark.memory.fraction") or get_spark_conf("spark.shuffle.memoryFraction"))
    memory_fraction = float(memory_fraction) if memory_fraction else 0.6
    storage_fraction = (get_spark_conf("spark.memory.storageFraction") or get_spark_conf("spark.storage.memoryFraction"))
    print("***", storage_fraction)
    storage_fraction = float(storage_fraction) if storage_fraction else 0.5

    reserved_mb = 300  # Spark reserves 300 MB for internal metadata
    usable_memory_mb = executor_heap_mb - reserved_mb
    unified_memory_mb = int(usable_memory_mb * memory_fraction)
    storage_memory_mb = int(unified_memory_mb * storage_fraction)
    # execution_memory_mb = unified_memory_mb - storage_memory_mb

    return {
        "memory_fraction": memory_fraction,
        "storage_fraction": storage_fraction,
        "usable_heap_mb": usable_memory_mb,
        "unified_memory_mb": unified_memory_mb,
        "storage_memory_mb": storage_memory_mb,
        # "execution_memory_mb": execution_memory_mb,
    }
    
def get_executor_details():
    """
    Returns executor cores, memory (MB), max workers, and memory fraction splits.
    Works on both classic and serverless clusters.

    NOTE on serverless:
      - SPARK_WORKER_MEMORY is the executor JVM heap.
      - spark.executor.memory / cores are CONFIG_NOT_AVAILABLE via Spark Connect.
      - Memory overhead is managed by Databricks and not exposed.
    """
    is_serverless = False
    try:
        executor_memory_str = get_spark_conf("spark.executor.memory")
        executor_cores = get_executor_cores()

        if executor_memory_str and executor_cores:
            worker_memory_mb = parse_memory(executor_memory_str)

            overhead_str = get_spark_conf("spark.executor.memoryOverhead", str(int(worker_memory_mb * 0.10)))
            overhead_mb = int(float(overhead_str.rstrip("m")))
            max_workers = get_max_workers()

            result = {
                "cluster_type": "classic",
                "max_workers": max_workers if max_workers else "NA",
                "executor_cores": executor_cores,
                "executor_memory_mb": worker_memory_mb,
                "memory_overhead_mb": overhead_mb}
            
            result.update(compute_memory_splits(worker_memory_mb))
            return result
        else:
            is_serverless = True
    except Exception:
        is_serverless = True

    if is_serverless:
        executor_heap_mb = int(
            os.environ.get("SPARK_WORKER_MEMORY", "0").rstrip("m")
        )
        executor_cores = get_executor_cores()

        result = {
            "cluster_type": "serverless",
            "max_workers": "NA",
            "executor_cores": executor_cores,
            "executor_memory_mb": executor_heap_mb,
            "memory_overhead_mb": "NA",
        }
        result.update(compute_memory_splits(executor_heap_mb))
        return result
