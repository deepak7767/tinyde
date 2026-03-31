# Metadata Management Features

A platform-agnostic Python module for **declarative schema management** of Delta Lake tables using DDL-as-code. Works with any environment that supports PySpark, Delta Lake, and the `catalog.schema.table` namespace. Define your table schemas in Python dictionary files and let the framework handle creation, evolution, column masking, and reordering — automatically.

> **Adopt once, stop worrying about metadata.** Once integrated, engineers focus purely on data logic — table creation, schema evolution, column masking, and reordering are handled entirely by the framework through a single DDL file per table. No manual ALTER scripts, no drift, no guesswork.

---

## The Problem It Solves

In modern data engineering, managing table schemas across environments is a persistent challenge:

| Challenge | How This Module Addresses It |
| --- | --- |
| **DDL is clumsy and hard to read** — Raw SQL DDL statements are verbose, difficult to review, and error-prone to write manually, especially for tables with many columns and properties | Uses a clean, human-readable Python dictionary format that is auto-generated from your DataFrame schema, easy to review in PRs, and simple to maintain by hand when needed |
| **Fragmented tooling** — Teams end up building or stitching together multiple frameworks to handle table creation, schema migration, column masking, reordering, and partition changes separately | Provides a **single, unified framework** that handles all metadata management concerns in one place — one function call, one DDL file, one consistent workflow |
| **Schema drift** — Tables diverge from intended definitions over time as ad-hoc changes accumulate | Compares a source-of-truth DDL file against the live table and applies only the necessary ALTER statements |
| **Manual ALTER scripts** — Engineers write brittle, one-off migration SQL that's hard to track and review | Generates all ALTER TABLE / ALTER COLUMN statements automatically from a diff between desired and current state |
| **Column masking complexity** — Setting up column-level security with `MASK` functions requires repetitive SQL | Declaratively specify inclusion/exclusion groups in the DDL; the framework creates and applies masking functions |
| **No single source of truth** — Schema definitions live in notebooks, tickets, and tribal knowledge | A single `.py` DDL file per table serves as the auditable, version-controllable contract |
| **Partition/cluster changes** — Changing partitioning requires table recreation; clustering requires specific ALTER syntax | Handles both transparently — recreates tables for partition changes, issues `ALTER TABLE CLUSTER BY` for clustering |
| **Column reordering** — Altering column order requires sequential `ALTER COLUMN ... AFTER` statements | Automatically computes and executes the minimal set of reorder operations |
| **Data quality rules scattered across tools** — Integration test constraints (primary key checks, null thresholds, min/max boundaries, row counts) are defined in separate test scripts, disconnected from the schema they validate | Stores constraint and validation rules **directly in the DDL file** alongside schema definitions — the same file that drives table creation also drives integration tests via the `integration_test_features` module, keeping schema and quality rules in one place |

---

## Architecture Overview

```
metadata_create_or_alter()          ← single entry point
        │
        ├── create_and_read_ddl.py  ← DDL file I/O (create / read)
        ├── create_table.py         ← initial table creation via DeltaTableBuilder
        ├── update_existing_ddl.py  ← schema diff, ALTER orchestration, masking
        └── common_functions.py     ← shared Spark session & imports
```

The DDL file is also consumed by the **`integration_test_features`** module, which reads the same `ddl_dict` to run data quality validations:

```
run_integration_tests()             ← reads the same DDL file
        │
        ├── validate_schema()       ← column existence & datatype match
        ├── pk_check()              ← primary key uniqueness & null check
        ├── min_row_count()         ← minimum row count threshold
        ├── null_threshold()        ← per-column null percentage limits
        └── min_max_threshold()     ← per-column value range boundaries
```

---

## Quick Start

```python
from tinyde.metadata_management_features.main import metadata_create_or_alter

metadata_create_or_alter(
    catalog_name="my_catalog",
    schema_name="my_schema",
    table_name="customers",
    final_df=spark.table("raw.customers"),   # reference DataFrame for schema
    ddl_path="/path/to/ddl/customers",       # .py appended automatically
    primary_key_list=["customer_id"]
)
```

**First run** (no DDL file exists): generates the DDL file from the DataFrame schema and creates the table.  
**Subsequent runs** (DDL file exists): reads the DDL, compares it against the live table, and applies incremental changes.

---

## DDL File Format

The DDL file is a Python file containing a single `ddl_dict` variable. It stores **both** schema definitions **and** integration test constraints in one place:

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
      "constraints": {"null_threshold": 5},
      "masking": {
        "inclusion_group": ["data_admins", "analysts"]
      }
    },
    "age": {
      "datatype": "integer",
      "comment": "Customer age",
      "allow_nulls": True,
      "constraints": {
      "min_max_threshold": {
        "minimum_value": 0,
        "maximum_value": 150
      }}
    },
    "my_catalog.my_schema.customers": {
      "comment": "Main customer dimension table",
      "primary_key": ["customer_id"],
      "minimum_row_count": 1000,
      "cluster_by": ["customer_id"],
      "masking": {
        "exclusion_group": ["external_viewers"],
        "catalog_name": "security_catalog",
        "schema_name": "masking_functions"
      }
    }
  }
}
```

### Supported Column Keys

| Key | Type | Used By | Description |
| --- | --- | --- | --- |
| `datatype` | `str` | Schema | Spark SQL data type (e.g., `"string"`, `"bigint"`, `"decimal(10,2)"`) |
| `comment` | `str` | Schema | Column description |
| `allow_nulls` | `bool` | Schema | Whether the column is nullable (default: `True`) |
| `constraints` | `str` | Schema | Column constraints (validated but cleaned before ALTER) |
| `masking` | `dict` | Schema | Masking config with `inclusion_group` or `exclusion_group` lists |
| `null_threshold` | `number` | Integration Tests | Maximum allowed null percentage for the column (e.g., `5` = max 5% nulls) |
| `min_max_threshold` | `dict` | Integration Tests | Value range boundaries with `minimum_value` and/or `maximum_value` keys |

### Supported Table-Level Keys

| Key | Type | Used By | Description |
| --- | --- | --- | --- |
| `comment` | `str` | Schema | Table description |
| `primary_key` | `list` | Schema + Integration Tests | Primary key column names (also used for uniqueness and null checks) |
| `partition_by` | `list` | Schema | Partition column names |
| `cluster_by` | `list` | Schema | Liquid clustering column names |
| `constraints` | `str` | Schema | Table constraints |
| `masking` | `dict` | Schema | Table-wide masking config applied to all columns |
| `minimum_row_count` | `number` | Integration Tests | Minimum expected row count for the table |

---

## Module Reference

### `main.py`

#### `metadata_create_or_alter(catalog_name, schema_name, table_name, final_df, ddl_path, primary_key_list)`

The **single entry point** for the entire module. Orchestrates the full lifecycle:

1. Loads or creates the DDL file from the DataFrame schema
2. Validates DDL keys against allowed sets
3. Creates the table if it doesn't exist, or computes and applies schema diffs if it does
4. Applies column masking functions

| Parameter | Type | Description |
| --- | --- | --- |
| `catalog_name` | `str` | Catalog name (follows `catalog.schema.table` convention) |
| `schema_name` | `str` | Schema (database) name |
| `table_name` | `str` | Table name |
| `final_df` | `DataFrame` | Spark DataFrame defining the desired schema |
| `ddl_path` | `str` | Path to the DDL `.py` file (auto-appends `.py` if missing) |
| `primary_key_list` | `list` | Column names forming the primary key |

**Returns:** `None`

---

### `create_and_read_ddl.py`

#### `create_ddl_file(full_table_name, df, ddl_path, primary_key_list)`

Generates a DDL dictionary from a Spark DataFrame's schema and writes it as a `.py` file. Each column gets `datatype`, `comment` (empty), and `allow_nulls` (True) entries. Table-level entry includes `comment` and `primary_key`.

**Returns:** `dict` — the generated DDL dictionary.

#### `read_ddl_file(ddl_path)`

Reads a DDL `.py` file and parses the `ddl_dict` variable back into a Python dictionary. Handles both Python (`True`/`False`) and JSON (`true`/`false`) boolean formats.

**Returns:** `dict` — the parsed DDL dictionary.

**Raises:** `ValueError` if the file doesn't contain a valid `ddl_dict = ...` assignment.

---

### `create_table.py`

#### `table_create(table_dict, full_table_name)`

Creates a new Delta table using the **DeltaTableBuilder API**. Supports:

* Standard columns with datatype, comment, and nullability
* Identity columns (`GENERATED ALWAYS AS IDENTITY` / `GENERATED BY DEFAULT AS IDENTITY`)
* Partitioning via `PARTITIONED BY`
* Liquid clustering via `CLUSTER BY`

**Raises:** `ValueError` if both `partition_by` and `cluster_by` are specified (Delta doesn't support both).

**Returns:** `None`

---

### `update_existing_ddl.py`

This is the core schema evolution engine containing the following functions:

#### `get_existing_table_schema(full_table_name)`

Retrieves the current live schema of a table by combining `DESCRIBE TABLE`, `DESCRIBE DETAIL`, and `information_schema.columns`. Returns a dictionary with each column's `datatype`, `comment`, and `allow_nulls`, plus table-level `comment`, `partition_by`, and `cluster_by`.

**Returns:** `dict`

#### `parse_ddl_to_schema_dict(ddl_dict, full_table_name)`

Normalizes a raw DDL dictionary into a clean schema dictionary. Handles:

* Column renames via `previous_name` key
* `drop_and_recreate` flag for datatype changes
* Extraction of masking configuration into a separate dictionary
* Lowercasing datatypes and defaulting `allow_nulls` to `True`
* Removal of non-schema properties (`constraints`, `primary_key`)

**Returns:** `tuple(normalized_schema_dict, column_order_list, masking_dict)`

#### `compute_schema_diff(existing_schema, desired_schema)`

Compares the existing table schema against the desired DDL schema and produces a changes dictionary. Detects:

* **New columns** — columns in DDL but not in the table
* **Modified columns** — changes to datatype, comment, or nullability
* **Dropped columns** — columns in the table but not in the DDL
* **Equivalent datatypes** — recognizes `bigint`/`long` and `int`/`integer` as equivalent

Changes are ordered with renames first, then other operations, with property priority: `drop → rename → datatype → comment → allow_nulls → partition_by → cluster_by`.

**Returns:** `dict`

#### `execute_table_and_column_alterations(full_table_name, changes_dict, desired_column_order)`

Orchestrates all ALTER statements. For each column change it:

1. Determines if it's an ADD or ALTER operation
2. Enables Delta column mapping when needed (rename, drop, datatype change)
3. Delegates datatype changes to `change_column_datatype()`
4. Builds and executes SQL queries for comments, nullability, renames, and drops
5. Reorders columns to match the DDL ordering
6. Applies table-level changes (comment, tags, partition, cluster)

**Returns:** `None`

#### `change_column_datatype(full_table_ref, column_name, new_data_type, is_drop_and_recreate, is_not_nullable)`

Changes a column's datatype using a safe swap pattern:

1. Add a temp column with the new type
2. Copy data from original to temp (or fill defaults if `drop_and_recreate=True`)
3. Preserve the original column's comment
4. Drop the original column and rename the temp column back

**Returns:** `None`

#### `rename_column_case(full_table_name, current_col_name, new_case_name)`

Handles case-only column renames (e.g., `customerid` → `customerId`) using a two-step rename through a temporary name, since direct case-only renames are not supported by Delta.

**Returns:** `None`

#### `enable_delta_column_mapping(full_table_name)`

Enables Delta column mapping mode (`'name'`) on a table, which is required before performing column renames, drops, or certain datatype changes.

**Returns:** `None`

#### `alter_table_partition_and_cluster(full_table_name, desired_partition_cols, desired_cluster_cols)`

Handles partition and cluster changes:

* **Clustering** — applied in-place via `ALTER TABLE ... CLUSTER BY`
* **Partitioning** — requires full table recreation (CTAS with temp table, drop, recreate, restore comment, drop temp)

**Returns:** `None`

#### `build_column_alter_queries(operation_type, alteration_kind, sql_templates, column_details, pending_queries, base_query, column_name)`

Constructs SQL ALTER queries for a single column based on the operation type (ADD/ALTER) and alteration kind (datatype, comment, allow_nulls, rename, drop). Handles nullable toggle logic and accumulates deferred queries.

**Returns:** `tuple(base_query, pending_queries)`

#### `build_table_alter_queries(table_properties, alteration_kind, sql_templates, pending_queries, base_query)`

Constructs SQL queries for table-level property changes (rename, comment, tags).

**Returns:** `list` of pending queries.

#### `execute_column_reorder(desired_order, current_order, full_table_name)`

Reorders columns to match the desired ordering by issuing sequential `ALTER TABLE ... ALTER COLUMN ... FIRST/AFTER` statements.

**Returns:** `None`

#### `create_and_run_masking_functions(full_table_name, masking_dict)`

Creates SQL masking functions and applies them to table columns. Supports two strategies:

* **Inclusion-based** — only listed groups see real data; everyone else sees masked defaults
* **Exclusion-based** — listed groups see masked defaults; everyone else sees real data

Masked default values are type-aware (e.g., `"*****"` for strings, `-99999` for integers, `"1900-01-01"` for dates).

**Returns:** `None`

---

### `common_functions.py`

Shared utility module that initializes or retrieves the active Spark session and re-exports common libraries (`delta`, `pyspark.sql.functions`, `DeltaTable`, `json`, `os`, etc.).

#### `get_or_create_spark()`

Returns the active SparkSession or creates a new one.

**Returns:** `SparkSession`

---

## Supported Schema Evolution Operations

| Operation | Mechanism |
| --- | --- |
| Add column | `ALTER TABLE ... ADD COLUMN` |
| Drop column | Enables column mapping → `ALTER TABLE ... DROP COLUMN` |
| Rename column | Enables column mapping → `ALTER TABLE ... RENAME COLUMN` |
| Case-only rename | Two-step rename through temp name |
| Change datatype | Swap pattern: add temp → copy data → drop original → rename temp |
| Change comment | `ALTER TABLE ... ALTER COLUMN ... COMMENT` |
| Toggle nullability | `ALTER TABLE ... ALTER COLUMN ... SET/DROP NOT NULL` |
| Change table comment | `ALTER TABLE ... SET TBLPROPERTIES ('comment' = ...)` |
| Change partitioning | Full table recreation via CTAS |
| Change clustering | `ALTER TABLE ... CLUSTER BY (...)` |
| Reorder columns | Sequential `ALTER COLUMN ... FIRST/AFTER` |
| Apply column masking | `CREATE OR REPLACE FUNCTION` + `ALTER COLUMN ... SET MASK` |

---

## Integration with `integration_test_features`

The DDL file does double duty — beyond driving schema management, it also stores **data quality constraints** that the `integration_test_features` module reads to run integration tests against your DataFrame:

| Test | DDL Key | Level | What It Checks |
| --- | --- | --- | --- |
| Schema validation | (automatic) | Table | Column existence and datatype match between DataFrame and target table |
| Primary key uniqueness | `primary_key` | Table | No duplicate rows across PK columns; no nulls in PK columns |
| Minimum row count | `minimum_row_count` | Table | DataFrame has at least N rows |
| Null threshold | `null_threshold` | Column | Null/empty percentage does not exceed the specified limit |
| Min/max range | `min_max_threshold` | Column | All values fall within `minimum_value` and/or `maximum_value` bounds |

This means your schema definition and data quality contract live in **one file** — no separate test configs, no drift between what you define and what you validate.

---

## File Structure

```
tinyde/
├── metadata_management_features/
│   ├── __init__.py
│   ├── main.py                   # Entry point: metadata_create_or_alter()
│   ├── create_and_read_ddl.py    # DDL file creation and parsing
│   ├── create_table.py           # Initial table creation via DeltaTableBuilder
│   ├── update_existing_ddl.py    # Schema diff, ALTER orchestration, masking
│   └── README.md                 # This file
├── integration_test_features/
│   ├── __init__.py
│   ├── main.py                   # Entry point: run_integration_tests()
│   ├── basic_checks.py           # PK, row count, null threshold, min/max checks
│   ├── intermediate_checks.py    # Intermediate-level validations
│   └── metadata_checks.py        # Metadata-level validations
└── common_functions_libs/
    ├── __init__.py
    └── common_functions.py       # Spark session + shared imports
```

---

## Compatibility

This module is **platform-agnostic** and works with any environment that provides:

* **Apache Spark** with **Delta Lake** support
* A catalog that follows the **`catalog.schema.table`** three-level namespace (e.g., Unity Catalog, Hive Metastore with catalog enabled, or any Delta-compatible catalog)
* **`information_schema.columns`** for nullability introspection
* **`is_member()`** function for column masking (if masking features are used)
* **PySpark** and **delta-spark** libraries

Tested platforms include Databricks, open-source Apache Spark with Delta Lake, and other Spark distributions supporting the Delta protocol.
