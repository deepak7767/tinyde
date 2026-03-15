from integration_test_features.basic_checks import min_row_count, null_threshold, min_max_threshold, pk_check, validate_schema
from metadata_management_features.create_and_read_ddl import read_ddl_file

def run_integration_tests(catalog_write, db_write, table_write, final_df, ddl_path):
    """
    Execute integration tests on a DataFrame against configured validation rules.

    Performs the following validations:
    - Minimum row count check
    - Null percentage threshold checks per column
    - Min/max value threshold checks per column
    - Primary key uniqueness check

    Args:
        catalog_write: Target catalog name
        db_write: Target database/schema name
        table_write: Target table name
        final_df: DataFrame to validate
        ddl_dict: Dictionary containing table metadata and validation rules

    Raises:
        ValueError: If any validation checks fail, with detailed error messages
    """
    full_table_name = f'{catalog_write}.{db_write}.{table_write}'
    ddl_dict = read_ddl_file(ddl_path)
    validate_schema(full_table_name, final_df, ddl_dict)

    select_expr_list = []
    min_row_count(ddl_dict, full_table_name, select_expr_list)
    null_threshold(ddl_dict, full_table_name, select_expr_list)
    min_max_threshold(ddl_dict, full_table_name, select_expr_list)
    pk_check(ddl_dict, full_table_name, select_expr_list)
    integration_test_results = final_df.selectExpr(*select_expr_list)
    result_row = integration_test_results.collect()[0].asDict()

    print(result_row)
    # Check for failed flags
    failures = []

    # Check minimum row count
    if not result_row.get('minimum_row_count_flag', True):
        actual_count = result_row.get('minimum_row_count', 0)
        minimum_required = ddl_dict[full_table_name][full_table_name]['minimum_row_count']
        failures.append(
            f"Minimum Row Count Check FAILED:\n"
            f"  Expected: >= {minimum_required} rows\n"
            f"  Actual: {actual_count} rows"
        )

    # Check null thresholds
    for col_name in result_row.keys():
        if col_name.startswith('null_percentage_flag_') and not result_row[col_name]:
            col = col_name.replace('null_percentage_flag_', '')
            actual_percentage = result_row.get(f'null_percentage_{col}', 0)
            threshold = ddl_dict[full_table_name][col]['null_threshold']
            failures.append(
                f"Null Threshold Check FAILED for column '{col}':\n"
                f"  Expected: <= {threshold}% null values\n"
                f"  Actual: {actual_percentage:.2f}% null values"
            )

    # Check min/max thresholds
    for col_name in result_row.keys():
        if col_name.startswith('min_max_threshold_flag_') and not result_row[col_name]:
            col = col_name.replace('min_max_threshold_flag_', '')
            violation_count = result_row.get(f'min_max_threshold_{col}', 0)
            thresholds = ddl_dict[full_table_name][col]['min_max_threshold']
            min_val = thresholds.get('minimum_value', 'N/A')
            max_val = thresholds.get('maximum_value', 'N/A')
            failures.append(
                f"Min/Max Threshold Check FAILED for column '{col}':\n"
                f"  Expected: values between [{min_val}, {max_val}]\n"
                f"  Actual: {violation_count} rows violate the threshold"
            )

    # Check primary key
    if not result_row.get('primary_key_flag', True):
        duplicate_count = result_row.get('primary_key', 0)
        pk_cols = ddl_dict[full_table_name][full_table_name]['primary_key']
        failures.append(
            f"Primary Key Check FAILED:\n"
            f"  Columns: {', '.join(pk_cols)}\n"
            f"  Duplicate count: {duplicate_count} rows"
        )

    # Check primary key nulls
    if not result_row.get('primary_key_null_flag', True):
        duplicate_count = result_row.get('primary_key_null', 0)
        pk_cols = ddl_dict[full_table_name][full_table_name]['primary_key']
        print(
            f"Primary Key have NULLS:\n"
            f"  Columns: {', '.join(pk_cols)}\n"
            f"  Duplicate count: {duplicate_count} individual values having nulls"
        )

    # Raise exception if any failures
    if failures:
        error_message = (
            f"Integration tests FAILED for table '{full_table_name}':\n\n" +
            '\n\n'.join(failures)
        )
        raise ValueError(error_message)

    # Update code to store these values


