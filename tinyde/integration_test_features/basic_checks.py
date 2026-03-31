from tinyde.common_functions_libs.common_functions import *


def _get_column_configs(ddl_dict, full_table_name):
    """Yield (column_name, constraints) pairs for columns with constraints defined."""
    for col, config in ddl_dict[full_table_name].items():
        if col != full_table_name and isinstance(config, dict):
            constraints = config.get('constraints', {})
            if constraints:
                yield col, constraints


def check_min_row_count(ddl_dict, full_table_name, select_expr_list):
    """Add minimum row count validation expressions to the select list."""
    min_row_count = ddl_dict[full_table_name].get(full_table_name, {}).get('constraints', {}).get('minimum_row_count')
    if not min_row_count:
        return
    select_expr_list.extend([
        "COUNT(*) AS min_row_count",
        f"COUNT(*) >= {min_row_count} AS min_row_count_flag"
    ])


def check_null_threshold(ddl_dict, full_table_name, select_expr_list):
    """Add null threshold validation expressions for columns with null_threshold constraint."""
    for col, constraints in _get_column_configs(ddl_dict, full_table_name):
        threshold = constraints.get('null_threshold')
        if threshold is None:
            continue
        null_pct = (
            f"(SUM(CASE WHEN {col} IS NULL OR TRIM(CAST({col} AS STRING)) = '' "
            f"THEN 1 ELSE 0 END) * 100.0 / COUNT(*))"
        )
        select_expr_list.extend([
            f"{null_pct} AS null_pct_{col}",
            f"{null_pct} <= {threshold} AS null_pct_flag_{col}"
        ])


def check_range(ddl_dict, full_table_name, select_expr_list):
    """
    Add range validation expressions for columns with range_check constraint.
    Expected structure in constraints: {minimum_value: ..., maximum_value: ...}
    """
    for col, constraints in _get_column_configs(ddl_dict, full_table_name):
        range_config = constraints.get('range_check')
        if not range_config:
            continue
        min_val = range_config.get('minimum_value')
        max_val = range_config.get('maximum_value')
        checks = []
        if min_val is not None:
            checks.append(f"SUM(CASE WHEN {col} < '{min_val}' THEN 1 ELSE 0 END)")
        if max_val is not None:
            checks.append(f"SUM(CASE WHEN {col} > '{max_val}' THEN 1 ELSE 0 END)")
        if not checks:
            continue
        violation_expr = " + ".join(checks)
        select_expr_list.extend([
            f"{violation_expr} AS range_violations_{col}",
            f"{violation_expr} = 0 AS range_flag_{col}"
        ])


def check_primary_key(ddl_dict, full_table_name, select_expr_list):
    """Add primary key uniqueness and null validation expressions to the select list."""
    pk_list = ddl_dict[full_table_name].get(full_table_name, {}).get('constraints', {}).get('primary_key')
    if not pk_list:
        return
    pk_cols = ", ".join(pk_list)
    dup_expr = f"COUNT(*) - COUNT(DISTINCT {pk_cols})"
    null_expr = " + ".join(f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)" for c in pk_list)
    select_expr_list.extend([
        f"{dup_expr} AS pk_dup_count",
        f"{dup_expr} = 0 AS pk_flag",
        f"{null_expr} AS pk_null_count",
        f"{null_expr} = 0 AS pk_null_flag",
    ])


def check_skewness(ddl_dict, full_table_name, select_expr_list):
    """
    Add skewness validation expressions for columns with skewness_threshold constraint.
    Computes each category's percentage of total rows via a scalar subquery.
    Fails if any single category holds more than the given threshold percentage.
    """
    for col, constraints in _get_column_configs(ddl_dict, full_table_name):
        threshold = constraints.get('skewness_threshold')
        if threshold is None:
            continue
        skew_pct = (
            f"(SELECT MAX(cnt) * 100.0 / SUM(cnt) "
            f"FROM (SELECT {col}, COUNT(*) AS cnt FROM {full_table_name} GROUP BY {col}))"
        )
        select_expr_list.extend([
            f"{skew_pct} AS skewness_pct_{col}",
            f"{skew_pct} <= {threshold} AS skewness_flag_{col}"
        ])


def save_all_test_results(test_results, integration_test_table):
    """
    Persist all integration test results (pass or fail) to the specified output table in a single write.

    Args:
        test_results: List of tuples (table_name, check_name, status, details, extra_info)
        integration_test_table: Fully qualified table name to write results to
    """
    if not test_results:
        return
    spark = get_or_create_spark()
    from datetime import datetime
    run_ts = datetime.now().isoformat()
    rows = [
        (tbl, chk, st, det, ext, run_ts)
        for tbl, chk, st, det, ext in test_results
    ]
    columns = ["table_name", "check_name", "status", "details", "extra_info", "run_timestamp"]
    result_df = spark.createDataFrame(rows, columns)
    result_df.write.mode("append").saveAsTable(integration_test_table)


def validate_schema(full_table_name, final_df, ddl_dict):
    """
    Validate schema between final_df and target table, checking column existence and data types.
    Returns (status, details, extra_info) tuple for the caller to handle saving and raising.
    """
    spark = get_or_create_spark()
    output_df = spark.table(full_table_name)
    src_schema = {f.name: f.dataType.simpleString() for f in final_df.schema.fields}
    tgt_schema = {f.name: f.dataType.simpleString() for f in output_df.schema.fields}

    # Type mismatch check
    type_mismatches = {
        col: (src_schema[col], tgt_schema[col])
        for col in src_schema.keys() & tgt_schema.keys()
        if src_schema[col] != tgt_schema[col]
    }

    # Column difference check
    ddl_cols = {
        col.lower() for col, meta in ddl_dict[full_table_name].items()
        if col != full_table_name and 'generated_identity' not in meta
    }
    final_cols = {col.lower() for col in final_df.columns}
    missing_in_df = ddl_cols - final_cols
    extra_in_df = final_cols - ddl_cols

    errors = []
    if type_mismatches:
        details = "\n".join(
            f"  - {col}: DataFrame='{dt}', Target='{tt}'"
            for col, (dt, tt) in type_mismatches.items()
        )
        errors.append(f"Type mismatches:\n{details}")
    if missing_in_df:
        errors.append(f"Missing in DataFrame: {sorted(missing_in_df)}")
    if extra_in_df:
        errors.append(f"Extra in DataFrame: {sorted(extra_in_df)}")

    status = "FAILED" if errors else "PASSED"
    result_details = "\n\n".join(errors) if errors else "Schema validation passed."
    extra = {
        "final_df_schema": str(final_df.schema),
        "output_df_schema": str(output_df.schema),
        "ddl_dict": str(ddl_dict),
    }
    return status, result_details, str(extra)


def evaluate_check_results(result_row, ddl_dict, full_table_name, test_results):
    """
    Evaluate the collected check results and classify each as PASSED or FAILED.

    Inspects result_row (the output of selectExpr) for flag/value columns generated
    by the check functions, compares against ddl_dict thresholds, and appends
    detailed (table, check_name, status, message, extra) tuples to test_results.

    Supports test_type at constraint level:
    - 'hard' (default): Failures raise a ValueError via the caller.
    - 'soft': Failures are recorded in test_results but do NOT raise an exception.

    Args:
        result_row: Dict from the collected selectExpr row (flag and metric columns).
        ddl_dict: DDL configuration dictionary.
        full_table_name: Fully qualified table name (catalog.schema.table).
        test_results: List to append result tuples to (mutated in place).

    Returns:
        List of hard failure message strings (empty if all checks passed or are soft).
    """
    failures = []
    row_str = str(result_row)
    table_meta = ddl_dict[full_table_name].get(full_table_name, {})
    table_test_type = table_meta.get('test_type', 'hard')
    table_constraints = table_meta.get('constraints', {})

    # --- Minimum Row Count ---
    if 'min_row_count_flag' in result_row:
        actual = result_row.get('min_row_count') or 0
        if not result_row['min_row_count_flag']:
            minimum_required = table_constraints.get('minimum_row_count', 'N/A')
            msg = f"Minimum Row Count FAILED: Expected >= {minimum_required}, Actual: {actual}"
            test_results.append((full_table_name, "min_row_count", "FAILED", msg, row_str))
            if table_test_type == 'hard':
                failures.append(msg)
        else:
            test_results.append((full_table_name, "min_row_count", "PASSED",
                f"Row count: {actual}", row_str))

    # --- Primary Key ---
    if 'pk_flag' in result_row:
        dup_count = result_row.get('pk_dup_count') or 0
        pk_cols = table_constraints.get('primary_key', [])
        if not result_row['pk_flag']:
            msg = f"Primary Key FAILED: Columns: {', '.join(pk_cols)}, Duplicates: {dup_count}"
            test_results.append((full_table_name, "primary_key", "FAILED", msg, row_str))
            if table_test_type == 'hard':
                failures.append(msg)
        else:
            test_results.append((full_table_name, "primary_key", "PASSED",
                "No duplicates found", row_str))

    if 'pk_null_flag' in result_row:
        null_count = result_row.get('pk_null_count') or 0
        pk_cols = table_constraints.get('primary_key', [])
        if not result_row['pk_null_flag']:
            msg = f"Primary Key Null FAILED: Columns: {', '.join(pk_cols)}, Null count: {null_count}"
            test_results.append((full_table_name, "primary_key_null", "FAILED", msg, row_str))
            if table_test_type == 'hard':
                failures.append(msg)
        else:
            test_results.append((full_table_name, "primary_key_null", "PASSED",
                "No nulls in PK columns", row_str))

    # --- Column-Level Checks (null, range, skewness) ---
    for col, config in ddl_dict[full_table_name].items():
        if col == full_table_name or not isinstance(config, dict):
            continue
        constraints = config.get('constraints', {})
        if not constraints:
            continue
        test_type = constraints.get('test_type', 'hard')

        # Null threshold
        if constraints.get('null_threshold') is not None:
            threshold = constraints['null_threshold']
            flag_key = f"null_pct_flag_{col}"
            pct_key = f"null_pct_{col}"
            actual_pct = result_row.get(pct_key) or 0
            if not result_row.get(flag_key, True):
                msg = f"Null Threshold FAILED for '{col}': Expected <= {threshold}%, Actual: {actual_pct:.2f}%"
                test_results.append((full_table_name, f"null_threshold_{col}", "FAILED", msg, row_str))
                if test_type == 'hard':
                    failures.append(msg)
            elif flag_key in result_row:
                test_results.append((full_table_name, f"null_threshold_{col}", "PASSED",
                    f"Null pct: {actual_pct:.2f}%", row_str))

        # Range check
        range_config = constraints.get('range_check')
        if range_config:
            min_val = range_config.get('minimum_value')
            max_val = range_config.get('maximum_value')
            flag_key = f"range_flag_{col}"
            violations_key = f"range_violations_{col}"
            violation_count = result_row.get(violations_key) or 0
            if not result_row.get(flag_key, True):
                msg = (f"Range Check FAILED for '{col}': "
                       f"Expected values in [{min_val or 'N/A'}, {max_val or 'N/A'}], "
                       f"Violations: {violation_count}")
                test_results.append((full_table_name, f"range_{col}", "FAILED", msg, row_str))
                if test_type == 'hard':
                    failures.append(msg)
            elif flag_key in result_row:
                test_results.append((full_table_name, f"range_{col}", "PASSED",
                    "No violations", row_str))

        # Skewness
        if constraints.get('skewness_threshold') is not None:
            threshold = constraints['skewness_threshold']
            flag_key = f"skewness_flag_{col}"
            pct_key = f"skewness_pct_{col}"
            actual_pct = result_row.get(pct_key) or 0
            if not result_row.get(flag_key, True):
                msg = (f"Skewness FAILED for '{col}': "
                       f"Max category holds {actual_pct:.2f}%, threshold: {threshold}%")
                test_results.append((full_table_name, f"skewness_{col}", "FAILED", msg, row_str))
                if test_type == 'hard':
                    failures.append(msg)
            elif flag_key in result_row:
                test_results.append((full_table_name, f"skewness_{col}", "PASSED",
                    f"Max category pct: {actual_pct:.2f}%", row_str))

    return failures
