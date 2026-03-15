from metadata_management_features.update_existing_ddl import existing_table_to_dict, ddl_to_dict, alter_dict_diff, alter_table_column_commands
from metadata_management_features.create_table import table_create
from metadata_management_features.create_and_read_ddl import create_ddl_file, read_ddl_file
from common_functions_libs.common_functions import *


def metadata_create_or_alter(catalog_name, schema_name, table_name, final_df, ddl_path, primary_key_list):
    """
    Creates or alters a table in the specified catalog and schema based on the provided DataFrame and DDL path.

    This function checks if a table exists in the given catalog and schema. If the table does not exist,
    it creates the table using the DDL generated from the provided DataFrame. If the table exists, it compares
    the existing schema with the desired schema and applies necessary alterations to match the desired schema.

    Args:
        catalog_name (str): Name of the catalog.
        schema_name (str): Name of the schema.
        table_name (str): Name of the table.
        final_df (DataFrame): Spark DataFrame representing the desired table structure. 
        ddl_path (str): Path to the DDL notebook or file.

    Returns:
        None
    """

    if not ddl_path.endswith('.py'):
        ddl_path = ddl_path + '.py'
    # Try to load the DDL notebook; if not found, create a new DDL file

    if not os.path.exists(ddl_path):
        print("No DDL notebook found. Creating DDL notebook in specified path")
        first_run_flag = True
        ddl_dict = create_ddl_file(catalog_name, schema_name, table_name, final_df, ddl_path, primary_key_list)

    ddl_dict = read_ddl_file(ddl_path)

    first_run_flag = False

    # Check if table exists in the specified catalog and schema
    table_exist = spark.sql(f"show tables in {catalog_name}.{schema_name}").filter(F.col('tableName') == table_name).count()
    
    if table_exist == 0:
        # Create table if it does not exist
        table_create(ddl_dict, catalog_name, schema_name, table_name)
        print(f"Table {full_table_name} has been created")
    else:
        # Alter table if it exists
        existing_table_dict = existing_table_to_dict(catalog_name, schema_name, table_name)
        ddl_dict_cleaned, ddl_col_order = ddl_to_dict(ddl_dict, catalog_name, schema_name, table_name)
        final_alter_dict = alter_dict_diff(existing_table_dict, ddl_dict_cleaned)
        alter_table_column_commands(catalog_name, schema_name, table_name, final_alter_dict, ddl_col_order)

    return ddl_dict 