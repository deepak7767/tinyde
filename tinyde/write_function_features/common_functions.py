from tinyde.metadata_management_features.create_and_read_ddl import read_ddl_file
from tinyde.common_functions_libs.common_functions import get_or_create_spark

EXCLUDED_HASH_COLUMNS = [
    'record_effective_date', 'record_expiry_date',
    'record_type', 'record_deletion_status'
]


def create_hash(primary_key, hash_cols):
    """Generate PK and content hash expression dictionaries for merge conditions."""
    if len(primary_key) == 1:
        pk_hash = {
            "pk_target_hash_reverse": f"coalesce(reverse(cast(target.{primary_key[0]} as string)), '')",
            "pk_source_hash_reverse": f"coalesce(reverse(cast(source.{primary_key[0]} as string)), '')"
        }
    else:
        pk_hash = {
            "pk_target_hash_reverse": ', '.join([f"coalesce(cast(target.{col} as string), '')" for col in primary_key[::-1]]),
            "pk_source_hash_reverse": ', '.join([f"coalesce(cast(source.{col} as string), '')" for col in primary_key[::-1]])
        }

    pk_hash['pk_target_hash'] = ', '.join([f"coalesce(cast(target.{col} as string), '')" for col in primary_key])
    pk_hash['pk_source_hash'] = ', '.join([f"coalesce(cast(source.{col} as string), '')" for col in primary_key])

    content_hash = {
        "content_source_hash": ', '.join([f"coalesce(cast(source.{col} as string), '')" for col in hash_cols]),
        "content_target_hash": ', '.join([f"coalesce(cast(target.{col} as string), '')" for col in hash_cols])
    }
    return pk_hash, content_hash


def pre_checks(catalog_name, schema_name, table_name, ddl_path, primary_key, write_type):
    """Validate table schema, detect identity columns, and generate hash dictionaries."""
    spark = get_or_create_spark()
    ddl_dict = read_ddl_file(ddl_path)
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    target_table = spark.table(full_table_name)
    excluded_columns = list(EXCLUDED_HASH_COLUMNS)
    identity_cols = []

    # Validate excluded columns exist in target
    missing_columns = set(excluded_columns) - set(target_table.columns)
    if missing_columns and write_type not in ('scd_1_overwrite', 'append'):
        raise Exception(f"Excluded columns not present in {full_table_name}: {sorted(missing_columns)}. Run not completed.")
    if not primary_key and write_type not in ('scd_1_overwrite', 'append'):
        raise Exception(f"No primary key provided. Run not completed for {full_table_name}")

    # Detect and exclude IDENTITY columns from hash generation
    for col_name in ddl_dict[full_table_name]:
        if 'generated_identity' in ddl_dict[full_table_name][col_name].keys():
            if col_name in primary_key:
                raise Exception(f"Identity column included in primary key: {col_name}. Run not completed for {full_table_name}")
            excluded_columns.append(col_name)
            identity_cols.append(col_name)
            print(f"Identity column detected for {full_table_name}: {col_name}. Excluding from hash calculation.")

    # Determine columns for hashing (exclude identity + metadata columns)
    reordered_columns = [col for col in target_table.columns if col not in identity_cols]
    hash_columns = [col for col in target_table.columns if col not in set(excluded_columns)]
    print(f"Excluded columns for {full_table_name}: {excluded_columns}")

    # Generate hash dictionaries
    pk_hash, content_hash = create_hash(primary_key, hash_cols=hash_columns)

    return pk_hash, content_hash, identity_cols, reordered_columns
