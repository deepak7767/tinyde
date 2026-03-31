from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from tinyde.common_functions_libs.common_functions import get_or_create_spark


def _build_pk_match_condition(pk_hash):
    """Build the primary key match condition for merge operations."""
    return (
        f"concat(hash({pk_hash['pk_target_hash']}), '_', hash({pk_hash['pk_target_hash_reverse']})) = "
        f"concat(hash({pk_hash['pk_source_hash']}), '_', hash({pk_hash['pk_source_hash_reverse']}))"
    )


def _build_content_change_condition(pk_hash, content_hash):
    """Build the content change detection condition for merge operations."""
    return (
        f"concat(hash({pk_hash['pk_target_hash']}), '_', hash({content_hash['content_target_hash']})) != "
        f"concat(hash({pk_hash['pk_source_hash']}), '_', hash({content_hash['content_source_hash']}))"
    )


def scd_1_merge(source_df, target_table_name, pk_hash, content_hash, identity_columns,
                update_col_exclusion=None, update_col_overrides=None):
    """
    SCD Type 1 merge: updates changed records, inserts new records, deletes missing records.

    Args:
        source_df: Source Spark DataFrame.
        target_table_name: Fully qualified target table name.
        pk_hash: Primary key hash dictionary from create_hash.
        content_hash: Content hash dictionary from create_hash.
        identity_columns: List of IDENTITY columns to exclude from updates.
        update_col_exclusion: Columns to exclude from update SET clause.
        update_col_overrides: Column values to override during update.
    """
    spark = get_or_create_spark()

    if update_col_exclusion is None:
        update_col_exclusion = ['record_effective_date']
    if update_col_overrides is None:
        update_col_overrides = {
            'record_type': lit("U"),
            'record_expiry_date': lit(current_timestamp())
        }

    excluded_from_update = update_col_exclusion + list(identity_columns)
    update_values = {col: f"source.{col}" for col in source_df.columns if col not in excluded_from_update}
    update_values.update(update_col_overrides)
    insert_values = {col: f"source.{col}" for col in source_df.columns}

    pk_match_condition = _build_pk_match_condition(pk_hash)
    content_change_condition = _build_content_change_condition(pk_hash, content_hash)

    target_table = DeltaTable.forName(spark, target_table_name)
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=pk_match_condition
    ).whenMatchedUpdate(
        condition=content_change_condition,
        set=update_values
    ).whenNotMatchedInsert(
        values=insert_values
    ).whenNotMatchedBySourceDelete().execute()

    print(f"SCD1 merge completed for {target_table_name}")


def scd_1_overwrite(source_df, target_table_name, partition_columns=None):
    """
    SCD Type 1 overwrite: replaces all data in the target table.

    Args:
        source_df: Source Spark DataFrame.
        target_table_name: Fully qualified target table name.
        partition_columns: Optional list of columns to partition by.
    """
    writer = source_df.write.format("delta").mode("overwrite")

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.option("overwriteSchema", "true").saveAsTable(target_table_name)
    print(f"SCD1 overwrite completed for {target_table_name}")


def append(source_df, target_table_name, partition_columns=None):
    """
    SCD Type 1 append: appends new data to the target table.

    Args:
        source_df: Source Spark DataFrame.
        target_table_name: Fully qualified target table name.
        partition_columns: Optional list of columns to partition by.
    """
    writer = source_df.write.format("delta").mode("append")

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.saveAsTable(target_table_name)
    print(f"Append completed for {target_table_name}")
