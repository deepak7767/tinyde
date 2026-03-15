from common_functions_libs.common_functions import *

def min_row_count(ddl_dict, full_table_name, select_expr_list):
    """
    Add minimum row count validation expressions to the select list.

    Args:
        ddl_dict: Dictionary containing table metadata and validation rules
        full_table_name: Full table name (catalog.db.table)
        select_expr_list: List to append validation SQL expressions to
    """
    minimum_row_count = ddl_dict[full_table_name][full_table_name].get('minimum_row_count', False)
    if not minimum_row_count:
        return None

    select_expr_list.extend([
        "count(*) as minimum_row_count",
        f"count(*) >= {minimum_row_count} as minimum_row_count_flag"
    ])

def null_threshold(ddl_dict, full_table_name, select_expr_list):
    """
    Add null threshold validation expressions to the select list for columns with null_threshold config.

    Args:
        ddl_dict: Dictionary containing table metadata and validation rules
        full_table_name: Full table name (catalog.db.table)
        select_expr_list: List to append validation SQL expressions to
    """
    table_config = ddl_dict[full_table_name]
    for col, config in table_config.items():
        if 'null_threshold' in config:
            threshold = config['null_threshold']
            null_calc = f"(SUM(CASE WHEN {col} IS NULL OR TRIM(CAST({col} AS STRING)) = '' THEN 1 ELSE 0 END) / COUNT(*)) * 100"
            select_expr_list.extend([
                f"{null_calc} <= {threshold} AS null_percentage_flag_{col}",
                f"{null_calc} AS null_percentage_{col}"
            ])

def min_max_threshold(ddl_dict, full_table_name, select_expr_list):
    """
    Add min/max threshold validation expressions to the select list for columns with min_max_threshold config.

    Args:
        ddl_dict: Dictionary containing table metadata and validation rules
        full_table_name: Full table name (catalog.db.table)
        select_expr_list: List to append validation SQL expressions to
    """
    table_config = ddl_dict[full_table_name]
    for col, config in table_config.items():
        if 'min_max_threshold' not in config:
            continue

        thresholds = config['min_max_threshold']
        min_val = thresholds.get('minimum_value')
        max_val = thresholds.get('maximum_value')

        if min_val and max_val:
            min_check = f"SUM(CASE WHEN {col} < '{min_val}' THEN 1 ELSE 0 END)"
            max_check = f"SUM(CASE WHEN {col} > '{max_val}' THEN 1 ELSE 0 END)"
            combined = f"{min_check} + {max_check}"
            select_expr_list.extend([
                f"{combined} = 0 AS min_max_threshold_flag_{col}",
                f"{combined} AS min_max_threshold_{col}"
            ])
        elif min_val:
            min_check = f"SUM(CASE WHEN {col} < '{min_val}' THEN 1 ELSE 0 END)"
            select_expr_list.extend([
                f"{min_check} = 0 AS min_max_threshold_flag_{col}",
                f"{min_check} AS min_max_threshold_{col}"
            ])
        elif max_val:
            max_check = f"SUM(CASE WHEN {col} > '{max_val}' THEN 1 ELSE 0 END)"
            select_expr_list.extend([
                f"{max_check} = 0 AS min_max_threshold_flag_{col}",
                f"{max_check} AS min_max_threshold_{col}"
            ])

def pk_check(ddl_dict, full_table_name, select_expr_list):
    """
    Add primary key uniqueness validation expressions to the select list.

    Args:
        ddl_dict: Dictionary containing table metadata and validation rules
        full_table_name: Full table name (catalog.db.table)
        select_expr_list: List to append validation SQL expressions to
    """
    try:
        pk_list = ddl_dict[full_table_name][full_table_name]['primary_key']
    except KeyError:
        print("Primary Key does not exist")
    pk_cols = ', '.join(pk_list)
    pk_calc = f"COUNT(*) - COUNT(DISTINCT {pk_cols})"
    null_calc = " + ".join([f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)" for c in pk_list])
    select_expr_list.extend([
        f"{pk_calc} = 0 AS primary_key_flag",
        f"{pk_calc} AS primary_key",
        f"{null_calc} = 0 AS primary_key_null_flag",
        f"{null_calc} AS primary_key_null",
    ])

def validate_schema(full_table_name, final_df, ddl_dict):
    """
    Validate schema between final_df and target table, checking both column existence and data types.

    Args:
        full_table_name: Fully qualified table name (catalog.db.table)
        final_df: DataFrame to validate
        ddl_dict: Optional DDL dictionary for additional column validation

    Raises:
        ValueError: If type mismatches or column differences are found
    """
    output_df = spark.table(full_table_name)

    # Schema comparison for data types
    schema1 = {field.name: field.dataType.simpleString() for field in final_df.schema.fields}
    schema2 = {field.name: field.dataType.simpleString() for field in output_df.schema.fields}
    common_cols = schema1.keys() & schema2.keys()

    type_mismatches = {
        col: (schema1[col], schema2[col])
        for col in common_cols
        if schema1[col] != schema2[col]
    }

    # Column existence check
    ddl_cols = {col.lower() for col, meta in ddl_dict[full_table_name].items()
                if 'generated_identity' not in meta and col != full_table_name}

    final_cols = {col.lower() for col in final_df.columns}
    column_differences = final_cols ^ ddl_cols

    # Build error message if issues found
    errors = []

    if type_mismatches:
        mismatch_details = '\n'.join([
            f" - Column '{col}': DataFrame has '{df_type}', Target has '{target_type}'"
            for col, (df_type, target_type) in type_mismatches.items()
        ])
        errors.append(f"Type mismatches found:\n{mismatch_details}")

    if column_differences:
        # Determine which columns are missing where
        reference_cols = {col.lower() for col, meta in ddl_dict[full_table_name].items()
                          if 'generated_identity' not in meta and col != full_table_name}

        final_cols_set = {col.lower() for col in final_df.columns}
        missing_in_df = reference_cols - final_cols_set
        extra_in_df = final_cols_set - reference_cols

        diff_details = []

        if missing_in_df:
            diff_details.append(f" Missing in DataFrame: {sorted(missing_in_df)}")
        if extra_in_df:
            diff_details.append(f" Extra in DataFrame: {sorted(extra_in_df)}")

        errors.append(f"Column differences found:\n" + '\n'.join(diff_details))

    if errors:
        error_message = f"Schema validation failed for table '{full_table_name}':\n\n" + '\n\n'.join(errors)
        raise ValueError(error_message)