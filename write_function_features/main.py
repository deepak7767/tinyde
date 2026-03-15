from common_functions_libs.common_functions import *
from metadata_management_features.create_and_read_ddl import read_ddl_file

def write_table(catalog_name, schema_name, table_name, ddl_path):

    ddl_dict = read_ddl_file(ddl_path)
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    target = spark.table(full_table_name)
    excluded_cols_for_hash = ['record_effective_date', 'record_expriry_date', 'record_type', 'record_deletion_status']

    # Validate excluded columns exist in target
    missing_excluded = set(excluded_cols_for_hash) - set(target.columns)
    if missing_excluded:
        raise Exception(f"Excluded columns not present in {full_table_name}: {sorted(missing_excluded)}. Run not completed.")
    if not primary_key:
        raise Exception(f"No Primary Key Input attched, Run not completed for {full_table_name}")

    # Remove IDENTITY columns in hash generation
    for _col in ddl_dict[full_table_name]:
        if 'generated_identity' in list(ddl_dict[full_table_name][_col].keys()):
            if _col in primary_key:
                raise Exception(f"Identity type column is included in primary key: {_col}. Run not completed for {full_table_name}")
            excluded_cols_for_hash.append(_col)
            incremental_dict['identity_columns'].append(_col)
            print(f"Identity Columns Detected for {full_table_name}. Not including the identity columns for hash calculation")

        incremental_dict['identity_columns'].append(_col)
        print(f"Identity Columns Detected for {full_table_name}. Not including the identity columns for hash calculation")

    # Reordering columns according to target table
    target_columns = [_col for _col in target.columns if _col not in incremental_dict['identity_columns']]
    dataframe = dataframe.select(*target_columns)

    # Hash Creation
    print(f"Not including following columns for {full_table_name}: {excluded_cols_for_hash}")
    get_cols = [col for col in target.columns if col not in set(excluded_cols_for_hash)]

    incremental_dict['pk_hash_dict'], incremental_dict['content_hash_dict'] = create_hash(dataframe, primary_key, hash_cols=get_cols)
    
    pk_hash_dict = incremental_dict['pk_hash_dict']
    content_hash_dict = incremental_dict['content_hash_dict']
    identity_columns = incremental_dict['identity_columns']

    if write_type == 'scd_1_merge':
        self.scd_1_merge(source_df, full_table_name, pk_hash_dict, content_hash_dict, identity_columns)
    if write_type == 'scd_2_expiry_only':
        self.scd_2_expiry_only(source_df, full_table_name, pk_hash_dict, content_hash_dict, identity_columns)
    if write_type == 'scd_2_expiry_and_deleted':
        self.scd_2_expiry_and_deleted(source_df, full_table_name, pk_hash_dict, content_hash_dict, identity_columns)






