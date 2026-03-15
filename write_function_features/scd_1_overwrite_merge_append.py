def scd_1_merge(source_df, target_table_name, pk_hash_dict, content_hash_dict, identity_columns, update_col_exclusion = ['AuditRecordCreatedTimestamp', 'AuditSourceSystemId'], update_col_dict = {'AuditRecordType': "U", 'AuditRecordUpdatedTimestamp': lit(current_timestamp())}):
    """
    Performs SCD Type 1 merge: updates changed records, inserts new, deletes missing.
    """
    update_col_exclusion.extend(identity_columns)
    update_cond = {col: f"source.{col}" for col in source_df.columns if col not in update_col_exclusion}
    update_cond.update(update_col_dict)
    insert_cond = {col: f"source.{col}" for col in source_df.columns}
    target_table = DeltaTable.forName(spark, target_table_name)
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"concat(hash({pk_hash_dict['pk_target_hash']}), '_', hash({pk_hash_dict['pk_target_hash_reverse']}))=" +
                  f"concat(hash({pk_hash_dict['pk_source_hash']}), '_', hash({pk_hash_dict['pk_source_hash_reverse']}))"
    ).whenMatchedUpdate(
        condition=f"concat(hash({pk_hash_dict['pk_target_hash']}), '_', hash({content_hash_dict['content_target_hash']})) !=" +
                  f"concat(hash({pk_hash_dict['pk_source_hash']}), '_', hash({content_hash_dict['content_source_hash']}))",
        set=update_cond
    ).whenNotMatchedInsert(values = insert_cond).whenNotMatchedBySourceDelete().execute()
