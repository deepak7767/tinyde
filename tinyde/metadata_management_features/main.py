from tinyde.metadata_management_features.update_existing_ddl import (
    get_existing_table_schema, parse_ddl_to_schema_dict, compute_schema_diff,
    execute_table_and_column_alterations, create_and_run_masking_functions
)
from tinyde.metadata_management_features.create_table import table_create
from tinyde.metadata_management_features.create_and_read_ddl import create_ddl_file, read_ddl_file
from tinyde.common_functions_libs.common_functions import *


def metadata_create_or_alter(catalog_name, schema_name, table_name, final_df, ddl_path):
    """
    Creates or alters a table in the specified catalog and schema based on the provided
    DataFrame and DDL path.

    If the DDL file does not exist, it generates one from the DataFrame. If the table
    does not exist, it creates it using the DDL. If the table exists, it compares the
    existing schema with the desired schema and applies necessary alterations including
    column changes, partition/cluster updates, and masking functions.

    Args:
        catalog_name (str): Name of the catalog.
        schema_name (str): Name of the schema.
        table_name (str): Name of the table.
        final_df (DataFrame): Spark DataFrame representing the desired table structure.
        ddl_path (str): Path to the DDL file (auto-appends .py if missing)

    Returns:
        dict: The parsed DDL dictionary for the table.
    """
    spark = get_or_create_spark()
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

    if not ddl_path.endswith('.py'):
        ddl_path += '.py'

    # Load or create the DDL file
    if not os.path.exists(ddl_path):
        print("No DDL file found. Creating DDL file at specified path.")
        ddl_dict = create_ddl_file(full_table_name, final_df, ddl_path)
    else:
        ddl_dict = read_ddl_file(ddl_path)

    # Check if table exists and create or alter accordingly
    table_exists = spark.catalog.tableExists(full_table_name)

    #Check if keys are right
    table_keys_set = {'comment', 'masking', 'partition_by', 'cluster_by', 'constraints', 'write_type'}
    col_keys_set = {'comment', 'datatype', 'constraints', 'masking', 'allow_nulls'}
    for col, details in ddl_dict[full_table_name].items():
        keys = set(details.keys())
        if col == full_table_name:
            invalid_keys = keys - table_keys_set
            if invalid_keys:
                raise Exception(f"Invalid table keys {invalid_keys} in {col}")
        else:
            invalid_keys = keys - col_keys_set
            if invalid_keys:
                raise Exception(f"Invalid column keys {invalid_keys} in {col}")


    if not table_exists:
        table_create(ddl_dict, full_table_name)
        ddl_dict_cleaned, ddl_col_order, masking_dict = parse_ddl_to_schema_dict(ddl_dict, full_table_name)
        create_and_run_masking_functions(full_table_name, masking_dict)
        print(f"Table {full_table_name} has been created")
    else:
        ddl_dict_cleaned, ddl_col_order, masking_dict = parse_ddl_to_schema_dict(ddl_dict, full_table_name)
        existing_table_dict = get_existing_table_schema(full_table_name)
        
        final_alter_dict = compute_schema_diff(existing_table_dict, ddl_dict_cleaned)
        execute_table_and_column_alterations(full_table_name, final_alter_dict, ddl_col_order)
        create_and_run_masking_functions(full_table_name, masking_dict)

    return None