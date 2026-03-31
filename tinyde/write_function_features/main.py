from tinyde.write_function_features.common_functions import pre_checks
from tinyde.write_function_features.scd_1_overwrite_merge_append import scd_1_merge, scd_1_overwrite, append
from tinyde.write_function_features.scd_2_ops import scd_2_expiry_only, scd_2_expiry_and_deleted
from tinyde.metadata_management_features.create_and_read_ddl import read_ddl_file

SUPPORTED_WRITE_TYPES = [
    'scd_1_merge', 'scd_1_overwrite', 'append',
    'scd_2_expiry_only', 'scd_2_expiry_and_deleted'
]


def write_table(catalog_name, schema_name, table_name, source_dataframe, ddl_path):
    """
    Write data to a Delta table using the specified SCD strategy.

    Args:
        catalog_name: Unity Catalog name.
        schema_name: Schema/database name.
        table_name: Target table name.
        source_dataframe: Source Spark DataFrame to write.
        ddl_path: Path to the DDL metadata file.
    """
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

    ddl_dict = read_ddl_file(ddl_path)

    partition_columns = ddl_dict[full_table_name][full_table_name].get('partition_by', None)
    write_type = ddl_dict[full_table_name][full_table_name].get('write_type', None)
    primary_key = ddl_dict[full_table_name][full_table_name]['constraints'].get('primary_key', None)
    if write_type is None:
        raise ValueError(f"Write type not specified for table {full_table_name} in DDL file.")
    if write_type not in SUPPORTED_WRITE_TYPES:
        raise ValueError(f"Unsupported write type '{write_type}' for table {full_table_name}. Supported types: {SUPPORTED_WRITE_TYPES}")

    # Run pre-checks and generate hash dictionaries
    pk_hash, content_hash, identity_cols, reordered_columns = pre_checks(catalog_name, schema_name, table_name, ddl_path, primary_key, write_type)

    # Reorder source columns to match target (excluding identity columns)
    source_dataframe = source_dataframe.select(*reordered_columns)

    # Execute write operation
    if write_type == 'scd_1_merge':
        scd_1_merge(source_dataframe, full_table_name, pk_hash, content_hash, identity_cols)
    elif write_type == 'scd_1_overwrite':
        scd_1_overwrite(source_dataframe, full_table_name, partition_columns)
    elif write_type == 'append':
        append(source_dataframe, full_table_name, partition_columns)
    elif write_type == 'scd_2_expiry_only':
        scd_2_expiry_only(source_dataframe, full_table_name, pk_hash, content_hash, identity_cols)
    elif write_type == 'scd_2_expiry_and_deleted':
        scd_2_expiry_and_deleted(source_dataframe, full_table_name, pk_hash, content_hash, identity_cols)
