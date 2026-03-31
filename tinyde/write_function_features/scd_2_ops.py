from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from tinyde.write_function_features.scd_1_overwrite_merge_append import (
    _build_pk_match_condition, _build_content_change_condition
)
from tinyde.common_functions_libs.common_functions import get_or_create_spark


def scd_2_expiry_only(source_df, target_table_name, pk_hash, content_hash, identity_columns,
                      expiry_col_overrides=None, record_status_column="record_deletion_status",
                      update_col_overrides=None):
    """
    SCD Type 2: expires old records on change, inserts new versions, deletes missing PKs.

    Args:
        source_df: Source Spark DataFrame.
        target_table_name: Fully qualified target table name.
        pk_hash: Primary key hash dictionary from create_hash.
        content_hash: Content hash dictionary from create_hash.
        identity_columns: List of IDENTITY columns (unused but kept for interface consistency).
        expiry_col_overrides: Column values to set when expiring a record.
        record_status_column: Column indicating active/deleted status.
        update_col_overrides: Column values to override when inserting updated records.
    """
    spark = get_or_create_spark()

    if expiry_col_overrides is None:
        expiry_col_overrides = {
            'record_expiry_date': lit(current_timestamp()),
            'record_type': lit("D"),
            'record_deletion_status': lit("Yes")
        }
    if update_col_overrides is None:
        update_col_overrides = {'record_type': lit("U")}

    insert_values = {col: f"source.{col}" for col in source_df.columns}
    target_table = DeltaTable.forName(spark, target_table_name)

    pk_match_condition = _build_pk_match_condition(pk_hash)
    content_change_condition = _build_content_change_condition(pk_hash, content_hash)
    active_record_filter = f"target.{record_status_column} = 'No'"

    # Step 1: Expire old records, insert new PKs, delete missing
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"{pk_match_condition} AND {active_record_filter}"
    ).whenMatchedUpdate(
        condition=content_change_condition,
        set=expiry_col_overrides
    ).whenNotMatchedInsert(
        values=insert_values
    ).whenNotMatchedBySourceDelete(
        condition=active_record_filter
    ).execute()

    # Step 2: Insert new live versions of expired records
    insert_values.update(update_col_overrides)
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"{pk_match_condition} AND {active_record_filter}"
    ).whenNotMatchedInsert(
        values=insert_values
    ).execute()

    print(f"SCD2 expiry-only completed for {target_table_name}")


def scd_2_expiry_and_deleted(source_df, target_table_name, pk_hash, content_hash, identity_columns,
                              expiry_col_overrides=None, delete_col_overrides=None,
                              record_status_column="record_deletion_status",
                              update_col_overrides=None):
    """
    SCD Type 2: expires old records on change, inserts new versions, expires deleted records.

    Args:
        source_df: Source Spark DataFrame.
        target_table_name: Fully qualified target table name.
        pk_hash: Primary key hash dictionary from create_hash.
        content_hash: Content hash dictionary from create_hash.
        identity_columns: List of IDENTITY columns (unused but kept for interface consistency).
        expiry_col_overrides: Column values to set when expiring a record.
        delete_col_overrides: Column values to set when marking a record as deleted.
        record_status_column: Column indicating active/deleted status.
        update_col_overrides: Column values to override when inserting updated records.
    """
    spark = get_or_create_spark()

    if expiry_col_overrides is None:
        expiry_col_overrides = {
            'record_expiry_date': lit(current_timestamp()),
            'record_type': lit("D"),
            'record_deletion_status': lit("Yes")
        }
    if delete_col_overrides is None:
        delete_col_overrides = {
            'record_expiry_date': lit(current_timestamp()),
            'record_type': lit("D"),
            'record_deletion_status': lit("Yes")
        }
    if update_col_overrides is None:
        update_col_overrides = {'record_type': lit("U")}

    insert_values = {col: f"source.{col}" for col in source_df.columns}
    target_table = DeltaTable.forName(spark, target_table_name)

    pk_match_condition = _build_pk_match_condition(pk_hash)
    content_change_condition = _build_content_change_condition(pk_hash, content_hash)
    active_record_filter = f"target.{record_status_column} = 'No'"

    # Step 1: Expire changed records, insert new PKs
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=pk_match_condition
    ).whenNotMatchedInsert(
        values=insert_values
    ).whenMatchedUpdate(
        condition=content_change_condition,
        set=expiry_col_overrides
    ).execute()

    # Step 2: Insert new live versions, expire deleted records
    insert_values.update(update_col_overrides)
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"{pk_match_condition} AND {active_record_filter}"
    ).whenNotMatchedInsert(
        values=insert_values
    ).whenNotMatchedBySourceUpdate(
        condition=active_record_filter,
        set=delete_col_overrides
    ).execute()

    print(f"SCD2 expiry-and-deleted completed for {target_table_name}")
