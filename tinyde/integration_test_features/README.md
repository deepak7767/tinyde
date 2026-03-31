# Integration Test Features

A platform-agnostic Python module for **declarative data quality validation** of Spark DataFrames using DDL-as-code. Works with any environment that supports PySpark, Delta Lake, and the `catalog.schema.table` namespace. Define your validation rules directly in the same DDL file used for schema management and let the framework handle schema validation, primary key checks, null thresholds, range boundaries, and skewness detection — automatically.

> **One DDL file, full coverage.** Validation constraints live alongside schema definitions — no separate test configs, no drift between what you define and what you validate. Engineers add a few keys to the DDL and get production-grade integration tests for free.

---

## The Problem It Solves

In modern data engineering, validating data quality before it reaches downstream consumers is critical but often fragmented:

| Challenge | How This Module Addresses It |
| --- | --- |
| **Scattered test definitions** — Data quality rules live in separate scripts, notebooks, or external tools, disconnected from the schema they validate | Reads constraints **directly from the DDL file** that also drives schema management — one source of truth for both structure and quality |
| **Manual test SQL** — Engineers write repetitive, error-prone SQL to check nulls, ranges, duplicates, and row counts for every table | Generates all validation SQL expressions automatically from declarative constraint definitions in the DDL |
| **No audit trail** — Test results are printed to logs and lost, making it hard to track quality over time | Persists every test result (pass and fail) to a configurable **integration test results table** with timestamps |
| **All-or-nothing failures** — A single failing check aborts the pipeline even when some checks are informational | Supports **hard** and **soft** test types — hard failures raise exceptions, soft failures are recorded but don't block the pipeline |
| **Schema drift goes unnoticed** — DataFrame schema silently diverges from the target table, causing runtime errors downstream | Validates column existence and datatype alignment between the DataFrame and target table **before** any data checks run |
| **No skewness detection** — Category dominance in key columns causes downstream join explosions and performance issues that go undetected | Computes per-column category dominance percentage and flags columns exceeding a configurable threshold |

---

## Architecture Overview

```
run_integration_tests()              <- single entry point
        |
        |-- basic_checks.py          <- check builders + evaluation + persistence
        |     |-- validate_schema()         <- column existence & datatype match
        |     |-- check_min_row_count()     <- minimum row count threshold
        |     |-- check_null_threshold()    <- per-column null percentage limits
        |     |-- check_range()             <- per-column value range boundaries
        |     |-- check_primary_key()       <- PK uniqueness & null checks
        |     |-- check_skewness()          <- category dominance detection
        |     |-- evaluate_check_results()  <- classify results as PASSED/FAILED
        |     +-- save_all_test_results()   <- persist results to Delta table
        |-- intermediate_checks.py   <- (reserved for intermediate-level validations)
        |-- metadata_checks.py       <- (reserved for metadata-level validations)
        +-- create_and_read_ddl.py   <- DDL file I/O (from metadata_management_features)
```

All checks are built as SQL `selectExpr` expressions, executed in a **single pass** over the DataFrame, and evaluated against DDL thresholds. Results are saved in one batch write.

---

## Quick Start

```python
from tinyde.integration_test_features.main import run_integration_tests

run_integration_tests(
    catalog_name="my_catalog",
    scehma_name="my_schema",
    table_name="customers",
    final_df=spark.table("raw.customers"),   # DataFrame to validate
    ddl_path="/path/to/ddl/customers"        # same DDL file used for schema management
)
```

**What happens:**
1. Reads the DDL file and resolves the integration test results table name
2. Validates the DataFrame schema against the target table (column existence + datatypes)
3. Builds SQL expressions for all configured checks (row count, nulls, ranges, PK, skewness)
4. Executes all checks in a single `selectExpr` pass
5. Evaluates results against thresholds, classifying each as PASSED or FAILED
6. Persists all results to the integration test results table
7. Raises `ValueError` if any **hard** checks failed

---

## DDL Constraint Configuration

Constraints are defined in the same DDL file used by `metadata_management_features`. They live at both the **table level** and **column level**:

```python
ddl_dict = {
  "my_catalog.my_schema.customers": {
    "customer_id": {
      "datatype": "bigint",
      "comment": "Unique customer identifier",
      "allow_nulls": False
    },
    "name": {
      "datatype": "string",
      "comment": "Customer full name",
      "allow_nulls": True,
      "constraints": {
        "null_threshold": 5,
        "test_type": "soft"
      }
    },
    "age": {
      "datatype": "integer",
      "comment": "Customer age",
      "allow_nulls": True,
      "constraints": {
        "range_check": {
          "minimum_value": 0,
          "maximum_value": 150
        }
      }
    },
    "region": {
      "datatype": "string",
      "comment": "Customer region",
      "allow_nulls": True,
      "constraints": {
        "skewness_threshold": 80
      }
    },
    "my_catalog.my_schema.customers": {
      "comment": "Main customer dimension table",
      "constraints": {
        "primary_key": ["customer_id"],
        "minimum_row_count": 1000,
        "integration_test_table_name": "my_schema.integration_test_results"
      }
    }
  }
}
```

### Supported Column-Level Constraint Keys

| Key | Type | Description |
| --- | --- | --- |
| `null_threshold` | `number` | Maximum allowed null/empty percentage for the column (e.g., `5` = max 5% nulls). Counts both `NULL` and empty/whitespace-only strings |
| `range_check` | `dict` | Value range boundaries with `minimum_value` and/or `maximum_value` keys. Counts violations where values fall outside the range |
| `skewness_threshold` | `number` | Maximum allowed percentage for any single category in the column (e.g., `80` = no single value may represent more than 80% of rows) |
| `test_type` | `str` | `"hard"` (default) raises an exception on failure; `"soft"` records the failure but does not block the pipeline |

### Supported Table-Level Constraint Keys

| Key | Type | Description |
| --- | --- | --- |
| `primary_key` | `list` | Column names forming the primary key. Checks for both duplicate rows and null values across PK columns |
| `minimum_row_count` | `number` | Minimum expected row count for the DataFrame |
| `integration_test_table_name` | `str` | Custom schema-qualified name for the results table (e.g., `"my_schema.test_results"`). Defaults to `{schema}.integration_test_table` if not specified |
| `test_type` | `str` | Default test type for table-level checks (`"hard"` or `"soft"`). Applies to row count and primary key checks |

---

## Validation Checks Reference

| Check | Level | DDL Key | What It Validates | Failure Message Example |
| --- | --- | --- | --- | --- |
| Schema validation | Table | (automatic) | Column existence and datatype match between DataFrame and target table | `Type mismatches: - age: DataFrame='string', Target='integer'` |
| Minimum row count | Table | `minimum_row_count` | DataFrame has at least N rows | `Minimum Row Count FAILED: Expected >= 1000, Actual: 42` |
| Primary key uniqueness | Table | `primary_key` | No duplicate rows across PK columns | `Primary Key FAILED: Columns: customer_id, Duplicates: 5` |
| Primary key nulls | Table | `primary_key` | No null values in PK columns | `Primary Key Null FAILED: Columns: customer_id, Null count: 3` |
| Null threshold | Column | `null_threshold` | Null/empty percentage does not exceed the specified limit | `Null Threshold FAILED for 'name': Expected <= 5%, Actual: 12.34%` |
| Range check | Column | `range_check` | All values fall within `minimum_value` and/or `maximum_value` bounds | `Range Check FAILED for 'age': Expected values in [0, 150], Violations: 7` |
| Skewness | Column | `skewness_threshold` | No single category exceeds the given percentage of total rows | `Skewness FAILED for 'region': Max category holds 92.50%, threshold: 80%` |

---

## Test Results Table

All results (pass and fail) are persisted to a Delta table with the following schema:

| Column | Type | Description |
| --- | --- | --- |
| `table_name` | `string` | Fully qualified table name that was tested |
| `check_name` | `string` | Name of the check (e.g., `schema_validation`, `min_row_count`, `null_threshold_name`, `range_age`) |
| `status` | `string` | `PASSED` or `FAILED` |
| `details` | `string` | Human-readable result message |
| `extra_info` | `string` | Full `selectExpr` result row or schema details (for debugging) |
| `run_timestamp` | `string` | ISO-format timestamp of the test run |

Results are written in `append` mode, preserving historical test runs for trend analysis and auditing.

---

## Module Reference

### `main.py`

#### `run_integration_tests(catalog_name, scehma_name, table_name, final_df, ddl_path)`

The **single entry point** for the entire module. Orchestrates the full validation lifecycle:

1. Reads the DDL file and resolves the integration test results table
2. Runs schema validation (fails fast if schema doesn't match)
3. Builds SQL check expressions from DDL constraints
4. Executes all checks in a single DataFrame pass
5. Evaluates and classifies results
6. Saves all results to the test results table
7. Raises `ValueError` if any hard checks failed

| Parameter | Type | Description |
| --- | --- | --- |
| `catalog_name` | `str` | Catalog name (follows `catalog.schema.table` convention) |
| `scehma_name` | `str` | Schema (database) name |
| `table_name` | `str` | Table name |
| `final_df` | `DataFrame` | Spark DataFrame to validate |
| `ddl_path` | `str` | Path to the DDL `.py` file (same file used by `metadata_management_features`) |

**Returns:** `None`

**Raises:** `ValueError` if schema validation fails or any hard checks fail.

---

### `basic_checks.py`

Contains all check builders, the evaluation engine, and the results persistence layer.

#### `validate_schema(full_table_name, final_df, ddl_dict)`

Validates schema alignment between the DataFrame and the target table. Checks:

* **Type mismatches** — columns present in both but with different datatypes
* **Missing in DataFrame** — columns defined in DDL but absent from the DataFrame (excludes identity columns)
* **Extra in DataFrame** — columns in the DataFrame but not in the DDL

**Returns:** `tuple(status, details, extra_info)` where status is `"PASSED"` or `"FAILED"`.

#### `check_min_row_count(ddl_dict, full_table_name, select_expr_list)`

Appends `COUNT(*)` and a boolean flag expression to the select list. The flag evaluates whether the row count meets or exceeds the `minimum_row_count` threshold from the DDL.

**Returns:** `None` (mutates `select_expr_list` in place).

#### `check_null_threshold(ddl_dict, full_table_name, select_expr_list)`

For each column with a `null_threshold` constraint, appends a null/empty percentage expression and a boolean flag. Counts both `NULL` values and empty/whitespace-only strings (via `TRIM(CAST(col AS STRING)) = ''`).

**Returns:** `None` (mutates `select_expr_list` in place).

#### `check_range(ddl_dict, full_table_name, select_expr_list)`

For each column with a `range_check` constraint, appends a violation count expression and a boolean flag. Supports `minimum_value`, `maximum_value`, or both. Values are compared as strings.

**Returns:** `None` (mutates `select_expr_list` in place).

#### `check_primary_key(ddl_dict, full_table_name, select_expr_list)`

Appends expressions for both duplicate detection (`COUNT(*) - COUNT(DISTINCT pk_cols)`) and null detection (`SUM(CASE WHEN pk_col IS NULL ...)`) across all primary key columns.

**Returns:** `None` (mutates `select_expr_list` in place).

#### `check_skewness(ddl_dict, full_table_name, select_expr_list)`

For each column with a `skewness_threshold` constraint, appends a scalar subquery that computes the maximum category percentage. Uses `GROUP BY` on the column and calculates `MAX(count) * 100.0 / SUM(count)` to find the dominant category's share.

**Returns:** `None` (mutates `select_expr_list` in place).

#### `evaluate_check_results(result_row, ddl_dict, full_table_name, test_results)`

Evaluates the collected `selectExpr` output row against DDL thresholds. For each check:

* Reads the flag and metric columns from `result_row`
* Classifies the result as `PASSED` or `FAILED`
* Appends a detailed tuple to `test_results`
* Respects `test_type` at both table level and column level — `"hard"` failures are collected for raising, `"soft"` failures are recorded only

**Returns:** `list` of hard failure message strings (empty if all checks passed or are soft).

#### `save_all_test_results(test_results, integration_test_table)`

Persists all accumulated test results to the specified Delta table in a single `append` write. Each result includes table name, check name, status, details, extra info, and a run timestamp.

**Returns:** `None`

#### `_get_column_configs(ddl_dict, full_table_name)`

Internal helper that yields `(column_name, constraints)` pairs for all columns that have a non-empty `constraints` dictionary in the DDL.

**Returns:** Generator of `tuple(str, dict)`.

---

### `intermediate_checks.py`

Reserved for intermediate-level validations. Currently empty — designed as an extension point for checks that require joins, lookups, or multi-table comparisons.

---

### `metadata_checks.py`

Reserved for metadata-level validations. Currently empty — designed as an extension point for checks against table properties, tags, permissions, or catalog metadata.

---

## Execution Flow

The module uses a **single-pass architecture** for efficiency:

```
1. Schema Validation (fail-fast)
   |-- FAILED -> save result, raise ValueError immediately
   +-- PASSED -> continue

2. Build Check Expressions
   |-- check_min_row_count()   -> appends to select_expr_list
   |-- check_null_threshold()  -> appends to select_expr_list
   |-- check_range()           -> appends to select_expr_list
   |-- check_primary_key()     -> appends to select_expr_list
   +-- check_skewness()        -> appends to select_expr_list

3. Execute All Checks
   +-- final_df.selectExpr(*select_expr_list).collect()[0]

4. Evaluate Results
   +-- evaluate_check_results() -> classifies each as PASSED/FAILED

5. Save All Results
   +-- save_all_test_results() -> single batch write to Delta

6. Raise on Hard Failures
   +-- ValueError with all hard failure messages joined
```

This design ensures that **all checks run and all results are saved** even when failures occur — the exception is raised only after persistence.

---

## Hard vs Soft Tests

The `test_type` key controls failure behavior at both table and column levels:

| Test Type | Behavior | Use Case |
| --- | --- | --- |
| `"hard"` (default) | Failure raises `ValueError`, blocking the pipeline | Critical checks that must pass before data reaches production |
| `"soft"` | Failure is recorded in the results table but does **not** raise an exception | Informational checks, known data issues being tracked, or gradual rollout of new rules |

Table-level `test_type` applies to row count and primary key checks. Column-level `test_type` applies to null threshold, range, and skewness checks for that specific column.

---

## Integration with `metadata_management_features`

This module reads the **same DDL file** produced and maintained by `metadata_management_features`. The constraint keys (`null_threshold`, `range_check`, `skewness_threshold`, `primary_key`, `minimum_row_count`) are stored alongside schema keys (`datatype`, `comment`, `allow_nulls`) in a single `.py` file per table.

Typical pipeline flow:

```python
from tinyde.metadata_management_features.main import metadata_create_or_alter
from tinyde.integration_test_features.main import run_integration_tests

# Step 1: Create/evolve the table schema
metadata_create_or_alter(
    catalog_name="my_catalog",
    schema_name="my_schema",
    table_name="customers",
    final_df=transformed_df,
    ddl_path="/path/to/ddl/customers",
    primary_key_list=["customer_id"]
)

# Step 2: Validate data quality before writing
run_integration_tests(
    catalog_name="my_catalog",
    scehma_name="my_schema",
    table_name="customers",
    final_df=transformed_df,
    ddl_path="/path/to/ddl/customers"
)

# Step 3: Write data (only reached if all hard checks pass)
transformed_df.write.mode("overwrite").saveAsTable("my_catalog.my_schema.customers")
```

---

## File Structure

```
tinyde/
|-- integration_test_features/
|   |-- __init__.py                # Exports: run_integration_tests()
|   |-- main.py                    # Entry point: run_integration_tests()
|   |-- basic_checks.py            # Check builders, evaluation, persistence
|   |-- intermediate_checks.py     # (Reserved) Intermediate-level validations
|   |-- metadata_checks.py         # (Reserved) Metadata-level validations
|   +-- README.md                  # This file
|-- metadata_management_features/  # Schema management (produces the DDL file)
+-- common_functions_libs/         # Spark session + shared imports
```

---

## Compatibility

This module is **platform-agnostic** and works with any environment that provides:

* **Apache Spark** with **Delta Lake** support
* A catalog that follows the **`catalog.schema.table`** three-level namespace (e.g., Unity Catalog, Hive Metastore with catalog enabled, or any Delta-compatible catalog)
* **PySpark** and **delta-spark** libraries
* A DDL file generated by `metadata_management_features` (or manually authored in the same format)

Tested platforms include Databricks, open-source Apache Spark with Delta Lake, and other Spark distributions supporting the Delta protocol.
