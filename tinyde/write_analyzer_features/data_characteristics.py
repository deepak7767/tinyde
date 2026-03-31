from tinyde.common_functions_libs.common_functions import *

def get_df_size_mb(df, total_count):
    """Estimate DataFrame size in MB using sample-based estimation."""
    
    sample_size = 600 if total_count>600 else total_count
    avg_row_bytes = df.limit(sample_size).toPandas().memory_usage(deep=True).sum() / sample_size
    return builtins.round((avg_row_bytes * total_count) / (1024 * 1024), 2)



def get_partition_distribution(df, partition_cols):
    """Partition distribution without full collect."""

    if len(partition_cols) > 0:
        partition_sizes_df = df.groupby(*partition_cols).count()
    else:
        df_with_pid = df.withColumn("_partitionid", F.spark_partition_id())

        # Count rows per partition
        partition_sizes_df = df_with_pid.groupBy("_partitionid").count()

    partition_sizes = [row["count"] for row in partition_sizes_df.collect()]

    sizes_sorted = sorted(partition_sizes, reverse=True)

    return {
        "partition_sizes": sizes_sorted,
        "num_partitions": len(partition_sizes),
        "max_size": builtins.max(partition_sizes),
        "min_size": builtins.min(partition_sizes),
        "mean_size": statistics.mean(partition_sizes),
        "std_dev": statistics.stdev(partition_sizes) if len(partition_sizes) > 1 else 0,
        "empty_partitions": builtins.sum(1 for s in partition_sizes if s == 0),
        "num_columns": len(df.columns)
    }

def detect_skewness(data_analyze_dict):
    """Detect data skewness across partitions."""
    max_size = data_analyze_dict['max_size']
    avg_size = data_analyze_dict['mean_size']
    std_dev = data_analyze_dict['std_dev']
    sizes = data_analyze_dict['partition_sizes']

    # Calculate skewness metrics
    skew_factor = max_size / avg_size
    coefficient_of_variation = std_dev / avg_size

    # Count imbalanced partitions using 2-sigma threshold
    upper_bound = avg_size + 2 * std_dev
    lower_bound = avg_size - 2 * std_dev
    imbalanced_count = sum(1 for s in sizes if s > upper_bound or s < lower_bound)
    imbalanced_pct = (imbalanced_count / len(sizes) * 100) if sizes else 0

    # Determine severity
    severity = calculate_skew_severity(
        skew_factor, coefficient_of_variation
    )

    return {
        "skew_factor": round(skew_factor, 2),
        "coefficient_of_variation": round(coefficient_of_variation, 2),
        "imbalanced_partitions_count": imbalanced_count,
        "imbalanced_partitions_pct": round(imbalanced_pct, 2),
        "empty_partitions": data_analyze_dict.get("empty_partitions", 0),
        "severity": severity,
        "status": get_skew_status(severity)
    }



def calculate_skew_severity(skew_factor, cv):
    """Calculate skewness severity level."""
    if skew_factor >= 5.0 or cv >= 1.0:
        return "SEVERE"
    elif skew_factor >= 3.0 or cv >= 0.7:
        return "HIGH"
    elif skew_factor >= 2.0 or cv >= 0.5:
        return "MEDIUM"
    elif skew_factor >= 1.5 or cv >= 0.3:
        return "LOW"
    else:
        return "NONE"


def get_skew_status(severity):
    """Map severity to status."""
    if severity in ["SEVERE", "HIGH"]:
        return "CRITICAL"
    elif severity == "MEDIUM":
        return "WARNING"
    else:
        return "OK"
    
def get_data_characteristics(df, partition_cols = []):
    total_count = df.count()
    size_mb = get_df_size_mb(df, total_count)

    data_analyze_dict = {
        "total_count": total_count,
        "average_row_size": size_mb}

    data_analyze_dict.update(get_partition_distribution(df, partition_cols))
    data_analyze_dict.update(detect_skewness(data_analyze_dict))

    return data_analyze_dict
