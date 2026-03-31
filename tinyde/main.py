from tinyde.metadata_management_features.main import metadata_create_or_alter
from tinyde.integration_test_features.main import run_integration_tests
from tinyde.write_analyzer_features.main import write_analyzer
from tinyde.write_function_features.main import write_table

def tinyde_write(catalog_name, schema_name, table_name, final_df, ddl_path):
    # Create or Alter Metadata for the table
    metadata_create_or_alter(catalog_name, schema_name, table_name, final_df, ddl_path)
    # Run Integration Tests
    run_integration_tests(catalog_name, schema_name, table_name, final_df, ddl_path)
    # # Write Analyzer
    # write_analyzer(final_df, [])
    # # Write Function
    write_table(catalog_name, schema_name, table_name, final_df, ddl_path)
    return None
