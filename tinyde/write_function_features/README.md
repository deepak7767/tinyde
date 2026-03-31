# Write Function Features

A platform-agnostic Python module for **declarative data writing** to Delta Lake tables using DDL-as-code. Works with any environment that supports PySpark, Delta Lake, and the `catalog.schema.table` namespace. Define your write strategy (`write_type`) in the same DDL file used for schema management and let the framework handle SCD Type 1 merges, overwrites, appends, and SCD Type 2 history tracking — automatically.

> **One function call, five write strategies.** Engineers specify the write pattern in the DDL file and call `write_table()`. The framework handles primary key hashing, content change detection, identity column exclusion, column reordering, and multi-step SCD2 merges — all from a single entry point.

---

## The Problem It Solves

In modern data engineering, writing data to Delta tables with the right change-tracking semantics is a recurring challenge:

| Challenge | How This Module Addresses It |
| --- | --- |
| **Repetitive merge logic** — Engineers rewrite the same MERGE INTO patterns for every table, with subtle variations that introduce bugs | Provides battle-tested, parameterized SCD1 and SCD2 merge functions driven by hash-based change detection |
| **Hash-based change detection is error-prone** — Building correct primary key and content hash expressions across many columns, handling NULLs, and ensuring deterministic comparison is tedious | Generates hash expression dictionaries automatically from column lists, with NULL-safe casting and forward/reverse hashing for collision resistance |
| **Identity column conflicts** — IDENTITY (auto-increment) columns must be excluded from update SET clauses and hash calculations, but forgetting one causes runtime errors | Reads the DDL file to detect identity columns and automatically excludes them from updates and hash generation |
| **SCD2 requires multi-step merges** — Expiring old records and inserting new versions in a single MERGE is not possible; engineers must orchestrate multiple operations correctly | Implements two-step merge patterns for both expiry-only and expiry-and-deleted SCD2 strategies, handling active record filtering and column overrides |
| **Column ordering mismatches** — Source DataFrames may have columns in a different order than the target table, causing silent data corruption on insert | Reorders source DataFrame columns to match the target table schema before writing |
| **Write strategy is disconnected from schema** — The write pattern (merge vs. overwrite vs. append) is defined in notebook code, not alongside the schema definition | Reads `write_type` from the DDL file, keeping write strategy co-located with schema, constraints, and masking configuration |

---

## Architecture Overview

```
write_table()                          <- single entry point
        |
        |-- common_functions.py        <- pre-checks, hash generation
        |     |-- pre_checks()               <- validate schema, detect identity cols
        |     +-- create_hash()              <- PK + content hash expression dicts
        |-- scd_1_overwrite_merge_append.py  <- SCD1 operations
        |     |-- scd_1_merge()              <- update + insert + delete
        |     |-- scd_1_overwrite()           <- full table replace
        |     +-- append()                   <- append-only writes
        +-- scd_2_ops.py               <- SCD2 operations
              |-- scd_2_expiry_only()         <- expire + insert new version + delete
              +-- scd_2_expiry_and_deleted()  <- expire + insert new version + soft-delete
```

The module reads the same DDL file produced by **`metadata_management_features`**, extracting `write_type`, `primary_key`, and `partition_by` to drive the write operation.

---

## Quick Start

```python
from tinyde.write_function_features.main import write_table

write_table(
    catalog_name="my_catalog",
    schema_name="my_schema",
    table_name="customers",
    source_dataframe=spark.table("staging.customers"),  # source DataFrame
    ddl_path="/path/to/ddl/customers"                   # same DDL file used for schema management
)
```

**What happens:**
1. Reads the DDL file and extracts `write_type`, `primary_key`, and `partition_by`
2. Runs pre-checks: validates excluded columns exist, detects identity columns
3. Generates primary key and content hash expression dictionaries
4. Reorders source DataFrame columns to match the target table (excluding identity columns)
5. Dispatches to the appropriate write function based on `write_type`

---

## DDL Write Configuration

The write strategy is configured in the table-level section of the DDL file — the same file used by `metadata_management_features` and `integration_test_features`:

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
      "allow_nulls": True
    },
    "my_catalog.my_schema.customers": {
      "comment": "Main customer dimension table",
      "write_type": "scd_1_merge",
      "partition_by": ["region"],
      "constraints": {
        "primary_key": ["customer_id"]
      }
    }
  }
}
```

### Supported `write_type` Values

| Write Type | Strategy | Deletes Missing PKs? | Tracks History? |
| --- | --- | --- | --- |
| `scd_1_merge` | Updates changed records, inserts new, deletes missing | Yes (hard delete) | No — overwrites in place |
| `scd_1_overwrite` | Replaces all data in the target table | N/A (full replace) | No |
| `append` | Appends new data without modifying existing rows | No | No |
| `scd_2_expiry_only` | Expires changed records, inserts new versions, deletes missing PKs | Yes (hard delete) | Yes — expired records retained |
| `scd_2_expiry_and_deleted` | Expires changed records, inserts new versions, soft-deletes missing PKs | No (soft delete via expiry) | Yes — all versions retained |

### Required DDL Keys for Write Operations

| Key | Type | Required For | Description |
| --- | --- | --- | --- |
| `write_type` | `str` | All | One of the five supported write strategies |
| `primary_key` | `list` | `scd_1_merge`, `scd_2_*` | Column names forming the primary key (inside `constraints`) |
| `partition_by` | `list` | `scd_1_overwrite`, `append` | Optional partition columns for overwrite and append modes |

---

## Write Strategies in Detail

### SCD Type 1 Merge (`scd_1_merge`)

Performs a full Delta MERGE with three clauses:
* **whenMatchedUpdate** — updates records where the primary key matches but content has changed
* **whenNotMatchedInsert** — inserts records with new primary keys
* **whenNotMatchedBySourceDelete** — deletes target records no longer present in the source

Change detection uses a dual-hash approach: `hash(pk_columns)` concatenated with `hash(pk_columns_reversed)` for primary key matching, and a separate content hash for detecting value changes.

Default behavior:
* Excludes `record_effective_date` from updates
* Sets `record_type = 'U'` and `record_expiry_date = current_timestamp()` on updated records

### SCD Type 1 Overwrite (`scd_1_overwrite`)

Replaces all data in the target table using `mode("overwrite")` with `overwriteSchema=true`. Supports optional partitioning.

### Append (`append`)

Appends new rows to the target table using `mode("append")`. Supports optional partitioning. No deduplication or change detection.

### SCD Type 2 Expiry Only (`scd_2_expiry_only`)

Two-step merge that maintains full history:

1. **Step 1:** Expire changed records (set `record_expiry_date`, `record_type='D'`, `record_deletion_status='Yes'`), insert new PKs, hard-delete missing PKs
2. **Step 2:** Insert new live versions of expired records (with `record_type='U'`)

Only merges against active records (`record_deletion_status = 'No'`).

### SCD Type 2 Expiry and Deleted (`scd_2_expiry_and_deleted`)

Two-step merge similar to expiry-only, but soft-deletes missing PKs instead of hard-deleting:

1. **Step 1:** Expire changed records, insert new PKs
2. **Step 2:** Insert new live versions, soft-delete missing PKs (set expiry columns via `whenNotMatchedBySourceUpdate`)

---

## Hash-Based Change Detection

The module uses a dual-hash strategy for robust primary key matching and content change detection:

```
PK Match:      concat(hash(pk_cols), '_', hash(pk_cols_reversed))
Content Match: concat(hash(pk_cols), '_', hash(content_cols))
```

* **Forward + reverse hashing** reduces collision probability for primary key matching
* **Content hashing** covers all columns except identity columns, `record_effective_date`, `record_expiry_date`, `record_type`, and `record_deletion_status`
* All columns are cast to string with `COALESCE(..., '')` for NULL-safe comparison

---

## Module Reference

### `main.py`

#### `write_table(catalog_name, schema_name, table_name, source_dataframe, ddl_path)`

The **single entry point** for the entire module. Reads the DDL to determine write strategy, runs pre-checks, reorders columns, and dispatches to the appropriate write function.

| Parameter | Type | Description |
| --- | --- | --- |
| `catalog_name` | `str` | Unity Catalog name |
| `schema_name` | `str` | Schema (database) name |
| `table_name` | `str` | Target table name |
| `source_dataframe` | `DataFrame` | Source Spark DataFrame to write |
| `ddl_path` | `str` | Path to the DDL `.py` file (same file used by `metadata_management_features`) |

**Returns:** `None`

**Raises:** `ValueError` if `write_type` is missing or unsupported.

---

### `common_functions.py`

#### `create_hash(primary_key, hash_cols)`

Generates primary key and content hash expression dictionaries for use in merge conditions. Produces four PK expressions (target/source x forward/reverse) and two content expressions (target/source).

| Parameter | Type | Description |
| --- | --- | --- |
| `primary_key` | `list` | Column names forming the primary key |
| `hash_cols` | `list` | Column names to include in content hash |

**Returns:** `tuple(pk_hash, content_hash)` — two dictionaries of SQL expression strings.

#### `pre_checks(catalog_name, schema_name, table_name, ddl_path, primary_key, write_type)`

Validates the target table and generates hash dictionaries. Performs:

* Validates that excluded columns (`record_effective_date`, `record_expiry_date`, `record_type`, `record_deletion_status`) exist in the target table (for merge/SCD2 types)
* Validates that a primary key is provided (for merge/SCD2 types)
* Detects IDENTITY columns from the DDL and excludes them from hash generation
* Generates PK and content hash expression dictionaries

| Parameter | Type | Description |
| --- | --- | --- |
| `catalog_name` | `str` | Catalog name |
| `schema_name` | `str` | Schema name |
| `table_name` | `str` | Table name |
| `ddl_path` | `str` | Path to the DDL file |
| `primary_key` | `list` | Primary key column names |
| `write_type` | `str` | The write strategy being used |

**Returns:** `tuple(pk_hash, content_hash, identity_cols, reordered_columns)`

**Raises:** `Exception` if excluded columns are missing, primary key is empty, or an identity column is included in the primary key.

---

### `scd_1_overwrite_merge_append.py`

Contains SCD Type 1 write operations and shared helper functions for building merge conditions.

#### `scd_1_merge(source_df, target_table_name, pk_hash, content_hash, identity_columns, update_col_exclusion=None, update_col_overrides=None)`

Executes a full SCD1 MERGE: update changed, insert new, delete missing.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `source_df` | `DataFrame` | — | Source Spark DataFrame |
| `target_table_name` | `str` | — | Fully qualified target table name |
| `pk_hash` | `dict` | — | Primary key hash dictionary from `create_hash` |
| `content_hash` | `dict` | — | Content hash dictionary from `create_hash` |
| `identity_columns` | `list` | — | IDENTITY columns to exclude from updates |
| `update_col_exclusion` | `list` | `['record_effective_date']` | Additional columns to exclude from update SET |
| `update_col_overrides` | `dict` | `{'record_type': 'U', 'record_expiry_date': current_timestamp()}` | Column values to override during update |

**Returns:** `None`

#### `scd_1_overwrite(source_df, target_table_name, partition_columns=None)`

Replaces all data in the target table with `overwriteSchema=true`.

| Parameter | Type | Description |
| --- | --- | --- |
| `source_df` | `DataFrame` | Source Spark DataFrame |
| `target_table_name` | `str` | Fully qualified target table name |
| `partition_columns` | `list` | Optional partition columns |

**Returns:** `None`

#### `append(source_df, target_table_name, partition_columns=None)`

Appends data to the target table.

| Parameter | Type | Description |
| --- | --- | --- |
| `source_df` | `DataFrame` | Source Spark DataFrame |
| `target_table_name` | `str` | Fully qualified target table name |
| `partition_columns` | `list` | Optional partition columns |

**Returns:** `None`

#### `_build_pk_match_condition(pk_hash)` *(internal)*

Builds the primary key match condition string for merge operations using forward and reverse hash concatenation.

#### `_build_content_change_condition(pk_hash, content_hash)` *(internal)*

Builds the content change detection condition string for merge operations.

---

### `scd_2_ops.py`

Contains SCD Type 2 write operations with full history tracking.

#### `scd_2_expiry_only(source_df, target_table_name, pk_hash, content_hash, identity_columns, expiry_col_overrides=None, record_status_column='record_deletion_status', update_col_overrides=None)`

Two-step SCD2 merge: expire old records, insert new versions, hard-delete missing.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `source_df` | `DataFrame` | — | Source Spark DataFrame |
| `target_table_name` | `str` | — | Fully qualified target table name |
| `pk_hash` | `dict` | — | Primary key hash dictionary |
| `content_hash` | `dict` | — | Content hash dictionary |
| `identity_columns` | `list` | — | IDENTITY columns (unused, kept for interface consistency) |
| `expiry_col_overrides` | `dict` | `{'record_expiry_date': current_timestamp(), 'record_type': 'D', 'record_deletion_status': 'Yes'}` | Columns set when expiring a record |
| `record_status_column` | `str` | `'record_deletion_status'` | Column indicating active/deleted status |
| `update_col_overrides` | `dict` | `{'record_type': 'U'}` | Columns set when inserting updated records |

**Returns:** `None`

#### `scd_2_expiry_and_deleted(source_df, target_table_name, pk_hash, content_hash, identity_columns, expiry_col_overrides=None, delete_col_overrides=None, record_status_column='record_deletion_status', update_col_overrides=None)`

Two-step SCD2 merge: expire old records, insert new versions, soft-delete missing.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `source_df` | `DataFrame` | — | Source Spark DataFrame |
| `target_table_name` | `str` | — | Fully qualified target table name |
| `pk_hash` | `dict` | — | Primary key hash dictionary |
| `content_hash` | `dict` | — | Content hash dictionary |
| `identity_columns` | `list` | — | IDENTITY columns (unused, kept for interface consistency) |
| `expiry_col_overrides` | `dict` | `{'record_expiry_date': current_timestamp(), 'record_type': 'D', 'record_deletion_status': 'Yes'}` | Columns set when expiring a record |
| `delete_col_overrides` | `dict` | `{'record_expiry_date': current_timestamp(), 'record_type': 'D', 'record_deletion_status': 'Yes'}` | Columns set when soft-deleting a record |
| `record_status_column` | `str` | `'record_deletion_status'` | Column indicating active/deleted status |
| `update_col_overrides` | `dict` | `{'record_type': 'U'}` | Columns set when inserting updated records |

**Returns:** `None`

---

## Execution Flow

```
1. Read DDL File
   +-- Extract write_type, primary_key, partition_by

2. Validate Write Type
   |-- Missing -> raise ValueError
   +-- Unsupported -> raise ValueError

3. Pre-Checks
   |-- Validate excluded columns exist in target table
   |-- Validate primary key is provided (for merge/SCD2)
   |-- Detect and exclude IDENTITY columns
   +-- Generate PK + content hash dictionaries

4. Reorder Source DataFrame
   +-- Match target column order (excluding identity columns)

5. Dispatch Write Operation
   |-- scd_1_merge     -> single MERGE (update + insert + delete)
   |-- scd_1_overwrite -> full overwrite with overwriteSchema
   |-- append          -> append mode write
   |-- scd_2_expiry_only      -> two-step MERGE (expire + insert new versions + delete)
   +-- scd_2_expiry_and_deleted -> two-step MERGE (expire + insert new versions + soft-delete)
```

---

## Metadata Columns

The module expects and manages the following metadata columns for merge and SCD2 operations:

| Column | Type | Description |
| --- | --- | --- |
| `record_effective_date` | `timestamp` | When the record became effective (excluded from updates) |
| `record_expiry_date` | `timestamp` | When the record was expired or updated |
| `record_type` | `string` | Record status: `'I'` (insert), `'U'` (update), `'D'` (delete/expire) |
| `record_deletion_status` | `string` | Active status: `'No'` (active), `'Yes'` (deleted/expired) — used by SCD2 |

These columns are automatically excluded from hash generation and managed by the framework's default column overrides.
