def scd_2_expiry_only(source_df, target_table_name, pk_hash_dict, content_hash_dict, identity_columns,
                      expiry_col_dict={'AuditRecordUpdatedTimestamp': lit(current_timestamp()), 'AuditRecordType': "U",
                                       'AuditRecordDeletedFlg': "Yes"},
                      record_status_column="AuditRecordDeletedFlg", update_col_dict={'AuditRecordType': "U"}):
    """
    SCD Type 2: Expires old records, inserts new, deletes missing PKs.
    """
    expiry_cond = expiry_col_dict
    insert_cond = {col: f"source.{col}" for col in source_df.columns}
    target_table = DeltaTable.forName(spark, target_table_name)

    # Expiring old records of same PK, Adding new PK records
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"concat(hash({pk_hash_dict['pk_target_hash']}), '_', hash({pk_hash_dict['pk_target_hash_reverse']}))=" +
                  f"concat(hash({pk_hash_dict['pk_source_hash']}), '_', hash({pk_hash_dict['pk_source_hash_reverse']})) AND target.{record_status_column} = 'No'"
    ).whenMatchedUpdate(
        condition=f"concat(hash({content_hash_dict['content_target_hash']})) != concat(hash({content_hash_dict['content_source_hash']}))",
        set=expiry_cond
    ).whenNotMatchedInsert(values=insert_cond).whenNotMatchedBySourceDelete(
        condition=f"target.{record_status_column} = 'No'").execute()

    # Adding new live version of old records
    insert_cond.update(update_col_dict)
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"concat(hash({pk_hash_dict['pk_target_hash']}), '_', hash({pk_hash_dict['pk_target_hash_reverse']}))=" +
                  f"concat(hash({pk_hash_dict['pk_source_hash']}), '_', hash({pk_hash_dict['pk_source_hash_reverse']})) AND target.{record_status_column} = 'No'"
    ).whenNotMatchedInsert(values=insert_cond).execute()

def scd_2_expiry_and_deleted(source_df, target_table_name, pk_hash_dict, content_hash_dict, identity_columns,
                             expiry_col_dict=
                             {'AuditRecordUpdatedTimestamp': lit(current_timestamp()), 'AuditRecordType': "U",
                              'AuditRecordDeletedFlg': "Yes"}, delete_col_dict=
                             {'AuditRecordUpdatedTimestamp': lit(current_timestamp()), 'AuditRecordType': "U",
                              'AuditRecordDeletedFlg': "Yes"},
                             record_status_column="AuditRecordDeletedFlg", update_col_dict={'AuditRecordType': "U"}):
    """
    SCD Type 2: Expires old records, inserts new, expires deleted records.
    """

    expiry_cond = expiry_col_dict
    delete_cond = delete_col_dict
    insert_cond = {col: f"source.{col}" for col in source_df.columns}
    target_table = DeltaTable.forName(spark, target_table_name)

    # Expiring old records of same PK, Adding new PK records and Expiring Deleted Records
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"concat(hash({pk_hash_dict['pk_target_hash']}), '_', hash({pk_hash_dict['pk_target_hash_reverse']}))=concat(hash({pk_hash_dict['pk_source_hash']}), '_', hash({pk_hash_dict['pk_source_hash_reverse']}))"
    ).whenNotMatchedInsert(values=insert_cond).whenMatchedUpdate(
        condition=f"concat(hash({pk_hash_dict['pk_target_hash']}), '_', hash({content_hash_dict['content_target_hash']})) != concat(hash({pk_hash_dict['pk_source_hash']}), '_', hash({content_hash_dict['content_source_hash']}))",
        set=expiry_cond
    ).execute()

    insert_cond.update(update_col_dict)

    # Adding new live version of old records
    target_table.alias("target").merge(
        source=source_df.alias("source"),
        condition=f"concat(hash({pk_hash_dict['pk_target_hash']}), '_', hash({pk_hash_dict['pk_target_hash_reverse']}))=concat(hash({pk_hash_dict['pk_source_hash']}), '_', hash({pk_hash_dict['pk_source_hash_reverse']})) AND target.{record_status_column} = 'No'"
    ).whenNotMatchedInsert(values=insert_cond).whenNotMatchedBySourceUpdate(
        condition=f"target.{record_status_column} = 'No'",
        set=delete_cond
    ).execute()