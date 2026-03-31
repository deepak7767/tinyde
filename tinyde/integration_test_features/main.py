from tinyde.integration_test_features.basic_checks import (
    check_min_row_count, check_null_threshold, check_range,
    check_primary_key, check_skewness, validate_schema, save_all_test_results, evaluate_check_results
)
from tinyde.metadata_management_features.create_and_read_ddl import read_ddl_file

def run_integration_tests(catalog_name, scehma_name, table_name, final_df, ddl_path):
    """
    Execute integration tests on a DataFrame against configured validation rules.

    Performs the following validations:
    - Schema validation
    - Minimum row count check
    - Null percentage threshold checks per column
    - Range checks per column
    - Primary key uniqueness and null checks
    - Skewness (category dominance) checks per column

    All results (pass and fail) are saved to integration_test_table in a single write.

    Args:
        catalog_name: Target catalog name
        scehma_name: Target database/schema name
        table_name: Target table name
        final_df: DataFrame to validate
        ddl_path: Path to DDL configuration file

    Raises:
        ValueError: If any validation checks fail, with detailed error messages
    """
    full_table_name = f'{catalog_name}.{scehma_name}.{table_name}'
    
    ddl_dict = read_ddl_file(ddl_path)
    integration_test_table = f"{catalog_name}.{ddl_dict[full_table_name][full_table_name]['constraints'].get('integration_test_table_name', None)}"
    if not ddl_dict[full_table_name][full_table_name]['constraints'].get('integration_test_table_name', None):
        integration_test_table = f'{catalog_name}.{scehma_name}.integration_test_table'
    test_results = []

    # --- Schema Validation ---
    schema_status, schema_details, schema_extra = validate_schema(full_table_name, final_df, ddl_dict)
    test_results.append((full_table_name, "schema_validation", schema_status, schema_details, schema_extra))

    if schema_status == "FAILED":
        save_all_test_results(test_results, integration_test_table)
        raise ValueError(f"Schema validation failed for '{full_table_name}':\n\n{schema_details}")

    # --- Build and Execute Check Expressions ---
    select_expr_list = []
    check_min_row_count(ddl_dict, full_table_name, select_expr_list)
    check_null_threshold(ddl_dict, full_table_name, select_expr_list)
    check_range(ddl_dict, full_table_name, select_expr_list)
    check_primary_key(ddl_dict, full_table_name, select_expr_list)
    check_skewness(ddl_dict, full_table_name, select_expr_list)

    if not select_expr_list:
        save_all_test_results(test_results, integration_test_table)
        print(f"No data checks configured for '{full_table_name}'. Schema validation PASSED.")
        return

    result_row = final_df.selectExpr(*select_expr_list).collect()[0].asDict()

    # --- Evaluate All Check Results ---
    failures = evaluate_check_results(result_row, ddl_dict, full_table_name, test_results)

    # --- Save All Results at Once ---
    save_all_test_results(test_results, integration_test_table)

    # --- Raise on Failures ---
    if failures:
        raise ValueError(
            f"Integration tests FAILED for table '{full_table_name}':\n\n" +
            '\n\n'.join(failures)
        )
    else:
        print(f"All integration tests PASSED for '{full_table_name}'")
