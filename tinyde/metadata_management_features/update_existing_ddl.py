from tinyde.common_functions_libs.common_functions import *

COLUMN_ALTER_SQL_TEMPLATES = {
    "datatype": "",
    "comment": "COMMENT '{details}'",
    "rename": "TO {details}",
    "drop": "",
    "allow_nulls": "{details} NOT NULL"
}

TABLE_ALTER_SQL_TEMPLATES = {
    "tags": "SET TAGS {details}",
    "unset_tags": "UNSET TAGS ('{details}')",
    "rename": "RENAME TO {details}",
    "comment": "SET TBLPROPERTIES ('comment' = '{details}')",
    "column_comments": "",
    "partition_by": "",
    "cluster_by": ""
}

EQUIVALENT_DATA_TYPES = {
    "bigint": ["long"],
    "long": ["bigint"],
    "int": ["integer"],
    "integer": ["int"]
}

MASKED_DEFAULTS = {
        "INTEGER": -99999,
        "INT": -99999,
        "LONG": -99999,
        "BIGINT": -99999,
        "DECIMAL": -99999.99,
        "DOUBLE": -99999.99,
        "FLOAT": -99999.99,
        "STRING": "*****",
        "BOOLEAN": False,
        "DATE": "1900-01-01",
        "TIMESTAMP": "1900-01-01 00:00:00",
        "BINARY": b"",
        "ARRAY": [],
        "MAP": {},
        "STRUCT": {}
    }


def rename_column_case(full_table_name, current_col_name, new_case_name):
    """
    Renames a column by changing only its letter casing using a two-step rename
    (original -> temp -> new case) since direct case-only renames are not supported.

    Args:
        full_table_name (str): Fully qualified table name (catalog.schema.table).
        current_col_name (str): Current column name to rename.
        new_case_name (str): New column name with desired casing.

    Returns:
        None
    """
    spark = get_or_create_spark()
    print(f"Changing column name case for {current_col_name} to {new_case_name}")
    spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {current_col_name} DROP MASK")
    spark.sql(f"ALTER TABLE {full_table_name} RENAME COLUMN {current_col_name} to {current_col_name}_temp")
    spark.sql(f"ALTER TABLE {full_table_name} RENAME COLUMN {current_col_name}_temp TO {new_case_name}")
    return None


def change_column_datatype(full_table_ref, column_name, new_data_type, is_drop_and_recreate, is_not_nullable):
    """
    Changes the datatype of a column by creating a temporary column with the new type,
    copying data from the original, dropping the original, and renaming the temp column back.

    Args:
        full_table_ref (str): Fully qualified table name (catalog.schema.table).
        column_name (str): Name of the column whose datatype is being changed.
        new_data_type (str): Target data type for the column.
        is_drop_and_recreate (bool): If True, populates the temp column with default values instead of copying.
        is_not_nullable (bool): If True and is_drop_and_recreate is True, fills with type-appropriate defaults.

    Returns:
        None
    """
    spark = get_or_create_spark()
    print(f"Altering datatype of {column_name} to {new_data_type}")

    temp_column_name = f"{column_name}_temp"

    spark.sql(f"ALTER TABLE {full_table_ref} ADD COLUMN {temp_column_name} {new_data_type}")

    describe_df = spark.sql(f"DESCRIBE {full_table_ref}")

    # Extract the comment for the source column
    source_comment = ""
    for row in describe_df.collect():
        if row['col_name'] == column_name:
            source_comment = row['comment'] if row['comment'] is not None else ""
            break

    spark.sql(f"ALTER TABLE {full_table_ref} ALTER COLUMN {temp_column_name} COMMENT '{source_comment}'")

    if not is_drop_and_recreate:
        try:
            spark.sql(f"UPDATE {full_table_ref} SET {temp_column_name} = {column_name}")
        except Exception as e:
            spark.sql(f"ALTER TABLE {full_table_ref} ALTER COLUMN {temp_column_name} DROP MASK")
            spark.sql(f"ALTER TABLE {full_table_ref} DROP COLUMN {temp_column_name}")
            raise Exception(
                f"Error changing datatype of {column_name} to {new_data_type} "
                f"as current values in the column do not allow the change."
            ) from e

    if is_drop_and_recreate and is_not_nullable:
        data_type_lower = new_data_type.lower()
        default_value = (
            "NA" if data_type_lower == "string"
            else "9999-12-31" if data_type_lower in ("date", "timestamp")
            else True if "bool" in data_type_lower
            else 9999999
        )
        spark.sql(f"UPDATE {full_table_ref} SET {temp_column_name} = '{default_value}'")

    spark.sql(f"ALTER TABLE {full_table_ref} ALTER COLUMN {column_name} DROP MASK")
    try:
        spark.sql(f"ALTER TABLE {full_table_ref} DROP COLUMN {column_name}")
    except Exception as e:
        spark.sql(f"ALTER TABLE {full_table_ref} ALTER COLUMN {temp_column_name} DROP MASK")
        spark.sql(f"ALTER TABLE {full_table_ref} DROP COLUMN {temp_column_name}")
        raise Exception(
            f"Error changing datatype of {column_name} to {new_data_type} "
            f"Given Column might be part of partition columns"
        ) from e
    
    spark.sql(f"ALTER TABLE {full_table_ref} ALTER COLUMN {temp_column_name} DROP MASK")
    spark.sql(f"ALTER TABLE {full_table_ref} RENAME COLUMN {temp_column_name} TO {column_name}")

    return None


def build_column_alter_queries(operation_type, alteration_kind, sql_templates, column_details, pending_queries, base_query, column_name):
    """
    Constructs SQL ALTER queries for a single column based on the operation (ADD/ALTER)
    and the specific alteration kind (datatype, comment, allow_nulls, rename, drop).

    Args:
        operation_type (str): SQL operation fragment, e.g. 'ADD COLUMN col type' or 'ALTER COLUMN col'.
        alteration_kind (str): Type of alteration (e.g. 'datatype', 'comment', 'allow_nulls', 'rename', 'drop').
        sql_templates (dict): Mapping of alteration kinds to SQL template strings.
        column_details (dict): Properties of the column from the DDL diff.
        pending_queries (list): Accumulator list of SQL queries to execute later.
        base_query (str): The base ALTER TABLE ... statement.
        column_name (str): Name of the column being altered.

    Returns:
        tuple: (updated base_query, updated pending_queries list)
    """
    if alteration_kind not in column_details:
        print(f"Alter type not present currently: {alteration_kind}")
        return base_query, pending_queries

    # Handle special cases for drop and rename
    if alteration_kind == "drop":
        base_query = base_query.replace("ALTER COLUMN", "DROP COLUMN")
    elif alteration_kind == "rename":
        base_query = base_query.replace("ALTER COLUMN", "RENAME COLUMN")

    # Handle nullable toggle
    if alteration_kind == "allow_nulls":
        column_details[alteration_kind] = "SET" if column_details[alteration_kind] is False else "DROP"

    # Format the extended query
    extended_query = " " + sql_templates[alteration_kind].format(details=column_details[alteration_kind])

    # Construct final query based on operation type and alteration
    if operation_type.startswith("ADD"):
        if alteration_kind == "allow_nulls":
            if column_details[alteration_kind] == "SET":
                data_type_lower = column_details["datatype"].lower()
                default_value = (
                    "NA" if data_type_lower == "string"
                    else "9999-12-31" if data_type_lower in ("date", "timestamp")
                    else True if "bool" in data_type_lower
                    else 9999999
                )
                update_query = f"UPDATE {base_query.split()[2]} SET {column_name} = '{default_value}'"
                pending_queries.append(update_query)

            nullable_query = " ".join(base_query.split()[:3]) + f" ALTER COLUMN {column_name}" + extended_query
            pending_queries.append(nullable_query)

        elif alteration_kind == "datatype":
            base_query += extended_query

    elif operation_type.startswith("ALTER") and alteration_kind == "rename":
        if column_details[alteration_kind].lower() == column_name.lower():
            rename_column_case(base_query.split()[2], column_name, column_details[alteration_kind])
        else:
            pending_queries.append(base_query + extended_query)

    elif operation_type.startswith("ALTER") and alteration_kind != "datatype":
        pending_queries.append(base_query + extended_query)

    return base_query, pending_queries


def enable_delta_column_mapping(full_table_name):
    """
    Enables Delta column mapping mode ('name') on a table, required before
    performing column renames, drops, or certain datatype changes.

    Args:
        full_table_name (str): Fully qualified table name (catalog.schema.table).

    Returns:
        None
    """
    spark = get_or_create_spark()
    spark.sql(
        f"ALTER TABLE {full_table_name} "
        f"SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    return None


def alter_table_partition_and_cluster(full_table_name, desired_partition_cols=None, desired_cluster_cols=None):
    """
    Alters, adds, or removes partition by and/or cluster by on an existing Delta table.

    - Cluster by (liquid clustering): Uses ALTER TABLE ... CLUSTER BY for in-place modification (DBR 13.3+).
    - Partition by: Requires table recreation via CTAS since Delta does not support in-place partition changes.

    Note: Delta tables do not support both PARTITIONED BY and CLUSTER BY simultaneously.

    Args:
        full_table_name (str): Fully qualified table name (catalog.schema.table).
        desired_partition_cols (list or None): Column names to partition by.
            Pass [] to remove partitioning, None to skip partition changes.
        desired_cluster_cols (list or None): Column names to cluster by.
            Pass [] to remove clustering, None to skip cluster changes.

    Returns:
        None
    """
    spark = get_or_create_spark()
    # Get current partition and cluster info (single call reused for both checks)
    detail_row = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]
    current_partition_cols = list(detail_row.partitionColumns) if detail_row.partitionColumns else []
    current_cluster_cols = (
        list(detail_row.clusteringColumns)
        if hasattr(detail_row, 'clusteringColumns') and detail_row.clusteringColumns
        else []
    )

    # Handle cluster by changes (in-place via ALTER TABLE)
    if desired_cluster_cols is not None:
        if desired_cluster_cols != current_cluster_cols:
            if desired_cluster_cols:
                cols_str = ", ".join(desired_cluster_cols)
                print(f"Setting CLUSTER BY ({cols_str}) on {full_table_name}")
                spark.sql(f"ALTER TABLE {full_table_name} CLUSTER BY ({cols_str})")
            else:
                print(f"Removing CLUSTER BY from {full_table_name}")
                spark.sql(f"ALTER TABLE {full_table_name} CLUSTER BY NONE")
        else:
            print(f"No cluster by changes needed for {full_table_name}")

    # Handle partition by changes (requires table recreation)
    if desired_partition_cols is not None:
        if desired_partition_cols != current_partition_cols:
            temp_table_name = f"{full_table_name}_partition_migration_temp"
            table_comment = detail_row.description or ""

            print(f"Recreating {full_table_name} to change partitioning")

            # Step 1: Save data to temp table
            print(f"Creating temp table {temp_table_name}")
            spark.sql(f"CREATE TABLE {temp_table_name} AS SELECT * FROM {full_table_name}")

            # Step 2: Drop original table
            print(f"Dropping original table {full_table_name}")
            spark.sql(f"DROP TABLE {full_table_name}")

            # Step 3: Recreate with new partitioning
            if desired_partition_cols:
                partition_str = ", ".join(desired_partition_cols)
                print(f"Recreating with PARTITIONED BY ({partition_str})")
                spark.sql(f"CREATE TABLE {full_table_name} USING DELTA PARTITIONED BY ({partition_str}) AS SELECT * FROM {temp_table_name}")
            else:
                print(f"Recreating without partitioning")
                spark.sql(f"CREATE TABLE {full_table_name} USING DELTA AS SELECT * FROM {temp_table_name}")

            # Step 4: Restore table comment if present
            if table_comment:
                spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{table_comment}')")

            # Step 5: Drop temp table
            print(f"Dropping temp table {temp_table_name}")
            spark.sql(f"DROP TABLE {temp_table_name}")
        else:
            print(f"No partition by changes needed for {full_table_name}")

    return None


def build_table_alter_queries(table_properties, alteration_kind, sql_templates, pending_queries, base_query):
    """
    Constructs SQL queries for altering table-level properties (rename, comment, tags, etc.).

    Args:
        table_properties (dict): Dictionary of table-level properties and their values.
        alteration_kind (str): Type of table alteration (e.g. 'rename', 'comment', 'tags').
        sql_templates (dict): Mapping of alteration kinds to SQL template strings.
        pending_queries (list): Accumulator list of SQL queries to execute later.
        base_query (str): The base ALTER TABLE ... statement.

    Returns:
        list: Updated list of pending table-level alter queries.
    """
    # REVISIT RENAME TABLE
    # table_properties[alteration_kind] = (
    #     f"{catalog_name}.{schema_name}." + table_properties[alteration_kind]
    #     if alteration_kind.lower() == "rename"
    #     else table_properties[alteration_kind]
    # )
    if alteration_kind in table_properties:
        base_query += sql_templates[alteration_kind].format(details=table_properties[alteration_kind])

    pending_queries.append(base_query)
    return pending_queries


def execute_column_reorder(desired_order, current_order, full_table_name):
    """
    Reorders columns in a table to match the desired ordering by issuing
    ALTER TABLE ... ALTER COLUMN ... FIRST/AFTER statements.

    Args:
        desired_order (list): Column names in the target order.
        current_order (list): Column names in the current order.
        full_table_name (str): Fully qualified table name (catalog.schema.table).

    Returns:
        None
    """
    spark = get_or_create_spark()
    for col_index in range(len(desired_order)):
        if current_order[col_index] == desired_order[col_index]:
            continue
        reorder_query = f"ALTER TABLE {full_table_name} ALTER COLUMN {desired_order[col_index]} "
        position_clause = "FIRST" if col_index == 0 else f"AFTER {desired_order[col_index - 1]}"
        current_order = list(spark.table(full_table_name).columns)
        spark.sql(reorder_query + position_clause)
    return None


def execute_table_and_column_alterations(full_table_name, changes_dict, desired_column_order):
    """
    Orchestrates all column-level and table-level ALTER statements including adding new
    columns, altering existing columns (rename, datatype, comment, allow_nulls, drop),
    reordering columns, modifying table properties, and partition/cluster changes.

    Args:
        full_table_name (str): Fully qualified table name (catalog.schema.table).
        changes_dict (dict): Mapping of column/table names to their required changes.
        desired_column_order (list): Column names in the desired final order.

    Returns:
        None
    """
    spark = get_or_create_spark()

    if full_table_name in changes_dict:
        table_queries = []
        rename_query = ""

        base_query = f"ALTER TABLE {full_table_name} "

        for alteration_kind in changes_dict[full_table_name]:
            # Skip partition_by and cluster_by — handled separately below
            if alteration_kind in ("partition_by", "cluster_by"):
                continue
            table_queries = build_table_alter_queries(
                changes_dict[full_table_name], alteration_kind,
                TABLE_ALTER_SQL_TEMPLATES, table_queries, base_query
            )

        for queued_query in table_queries:
            if "RENAME TO" in queued_query:
                rename_query = queued_query
                continue

            print("Executing:", queued_query)
            spark.sql(queued_query)

        if rename_query:
            spark.sql(rename_query)

        # Handle partition by and cluster by changes
        table_changes = changes_dict[full_table_name]
        desired_partition_cols = table_changes.get("partition_by")
        desired_cluster_cols = table_changes.get("cluster_by")

        if desired_partition_cols is not None or desired_cluster_cols is not None:
            alter_table_partition_and_cluster(full_table_name, desired_partition_cols, desired_cluster_cols)

    for column_name, column_changes in changes_dict.items():
        if column_name == full_table_name:
            continue

        pending_queries = []
        existing_columns = spark.table(full_table_name).columns
        is_existing = column_name in existing_columns
        operation_type = (
            f"ALTER COLUMN {column_name}" if is_existing
            else f"ADD COLUMN {column_name} {column_changes['datatype']}"
        )

        base_query = f"ALTER TABLE {full_table_name} {operation_type}"

        for alteration_kind in column_changes:

            if alteration_kind in ("rename", "drop", "datatype") and is_existing:
                enable_delta_column_mapping(full_table_name)

            if alteration_kind == "datatype" and is_existing:
                change_column_datatype(
                    full_table_name, column_name, column_changes["datatype"],
                    column_changes.get('drop_and_recreate', False),
                    column_changes.get('allow_nulls', False)
                )
                continue

            base_query, pending_queries = build_column_alter_queries(
                operation_type, alteration_kind, COLUMN_ALTER_SQL_TEMPLATES,
                column_changes, pending_queries, base_query, column_name
            )

        if not is_existing:
            print("Executing:", base_query)
            spark.sql(base_query)

        for queued_query in pending_queries:

            if "rename column" in queued_query.lower() or "drop column" in queued_query.lower():
                print("Executing:", queued_query)
                spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {column_name} DROP MASK")
                spark.sql(queued_query)

            else:
                print("Executing:", queued_query)
                spark.sql(queued_query)

    # Run all reorder queries
    if full_table_name in desired_column_order:
        desired_column_order.remove(full_table_name)
    current_column_order = list(spark.table(full_table_name).columns)
    if desired_column_order != current_column_order:
        print("running reorder queries")
        execute_column_reorder(desired_column_order, current_column_order, full_table_name)   

    if len(changes_dict) == 0 and desired_column_order == current_column_order:
        print(f"No schema changes detected between existing table and DDL for {full_table_name}")
    else:
        print(f"All the schema changes were implemented for the table {full_table_name}")


def get_existing_table_schema(full_table_name):
    """
    Retrieves the current schema of an existing table as a dictionary containing
    each column's datatype, comment, and nullability, plus the table-level comment,
    partition columns, and cluster columns.

    Args:
        full_table_name (str): Fully qualified table name (catalog.schema.table).

    Returns:
        dict: Keys are column names (with values {'datatype', 'comment', 'allow_nulls'})
              and the fully qualified table name (with value {'comment', 'partition_by', 'cluster_by'}).
    """
    spark = get_or_create_spark()
    catalog_name, schema_name, table_name = full_table_name.split('.')

    described_table = spark.sql(f"DESCRIBE TABLE {full_table_name}")
    described_table = described_table.filter(~(F.col("col_name").like("%# %")))
    detail_row = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]
    table_comment = detail_row.description or ''
    partition_cols = list(detail_row.partitionColumns) if detail_row.partitionColumns else []
    cluster_cols = (
        list(detail_row.clusteringColumns)
        if hasattr(detail_row, 'clusteringColumns') and detail_row.clusteringColumns
        else []
    )

    nullability_info = (
        spark.table(f"{catalog_name}.information_schema.columns")
        .select('column_name', 'is_nullable')
        .withColumnRenamed('column_name', 'col_name')
        .filter(
            (F.col('table_catalog') == catalog_name)
            & (F.col('table_schema') == schema_name)
            & (F.col('table_name') == table_name)
        )
    )

    described_table = described_table.join(nullability_info, ['col_name'], 'left')
    schema_dict = {}

    for row in described_table.collect():
        comment = row.comment or ''
        is_nullable = (
            '' if row.is_nullable is None
            else False if row.is_nullable.upper() == "NO"
            else True
        )
        schema_dict[row.col_name] = {"datatype": row.data_type, "comment": comment, "allow_nulls": is_nullable}

    schema_dict[full_table_name] = {
        "comment": table_comment,
        "partition_by": partition_cols,
        "cluster_by": cluster_cols
    }

    return schema_dict


def parse_ddl_to_schema_dict(ddl_dict, full_table_name):
    """
    Parses a raw DDL dictionary into a normalized schema dictionary by processing
    column renames, removing non-schema properties, lowercasing datatypes,
    and defaulting allow_nulls to True where not specified.

    If masking configuration is present (at the top-level 'masking' key or on
    individual columns), it is extracted into a separate masking_dict.
    DDLs without masking config are handled gracefully.

    Args:
        ddl_dict (dict): Raw DDL dictionary keyed by fully qualified table name.
        full_table_name (str): Fully qualified table name (catalog.schema.table).

    Returns:
        tuple: (normalized_schema_dict, column_order_list, masking_dict)
            - normalized_schema_dict (dict): Cleaned column definitions.
            - column_order_list (list): Ordered list of column names for reordering.
            - masking_dict (dict): Column masking configuration (empty if no masking).
    """
    spark = get_or_create_spark()
    table_df = spark.table(full_table_name)
    existing_cols = list(table_df.columns)
    schema = {field.name: field.dataType.simpleString() for field in table_df.schema.fields}
    masking_dict = {}

    table_entry = ddl_dict[full_table_name]
    column_order_list = list(table_entry.keys())

    for col_name in list(table_entry.keys()):
        col_props = table_entry[col_name]

        if 'previous_name' in col_props:
            if col_name not in existing_cols:
                table_entry[col_props['previous_name']] = {'rename': col_name}
            del col_props['previous_name']

        if 'drop_and_recreate' in col_props:
            if col_props['datatype'] in EQUIVALENT_DATA_TYPES.get(schema.get(col_name, ''), [schema.get(col_name, '')]):
                del col_props['drop_and_recreate']

        if (
            'allow_nulls' not in col_props
            and col_name != full_table_name
            and len(col_props) == 1
            and list(col_props.keys())[0] != 'rename'
        ):
            col_props['allow_nulls'] = True

    # Only process masking if masking config exists anywhere in the DDL
    has_masking = False
    for k, v in table_entry.items():
        if 'masking' in list(v.keys()):
            has_masking = True

    if has_masking:
        if 'masking' in list(table_entry[full_table_name].keys()):
            masking_keys = list(table_entry[full_table_name]['masking'].keys())
            if 'inclusion_group' in masking_keys and 'exclusion_group' in masking_keys:
                raise Exception("Both inclusion_group and exclusion_group cannot be specified for the same column")
            for col_name in list(table_entry.keys()):
                if col_name != full_table_name:
                    masking_dict[col_name] = {
                        "inclusion_group": table_entry[full_table_name]['masking'].get('inclusion_group', None),
                        "exclusion_group": table_entry[full_table_name]['masking'].get('exclusion_group', None),
                        "datatype": table_entry[col_name]['datatype']
                    }
            

        for col_name in list(table_entry.keys()):
            col_props = table_entry[col_name]

            if 'masking' in col_props:
                masking_keys = list(col_props['masking'].keys())
                if 'inclusion_group' in masking_keys and 'exclusion_group' in masking_keys:
                    raise Exception("Both inclusion_group and exclusion_group cannot be specified for the same column")

            if col_name != full_table_name and 'masking' in col_props:
                masking_dict[col_name] = {
                    "inclusion_group": col_props['masking'].get('inclusion_group', None),
                    "exclusion_group": col_props['masking'].get('exclusion_group', None),
                    "datatype": col_props['datatype']
                }
                del table_entry[col_name]['masking']
                

        if masking_dict:
            if 'masking' in list(table_entry[full_table_name].keys()):
                masking_dict['function_saving_catalog_name'] = table_entry[full_table_name]['masking'].get('catalog_name', None)
                masking_dict['function_saving_schema_name'] = table_entry[full_table_name]['masking'].get('schema_name', None)
                del table_entry[full_table_name]['masking']
            else:
                masking_dict['function_saving_catalog_name'] = None
                masking_dict['function_saving_schema_name'] = None
           
    normalized_dict = table_entry

    # Cleaning
    for col_name, col_properties in normalized_dict.items():
        for property_name in list(col_properties.keys()):
            if property_name in ['constraints', 'write_type']:
                del normalized_dict[col_name][property_name]

            if property_name == 'datatype':
                normalized_dict[col_name][property_name] = col_properties[property_name].lower()

    return normalized_dict, column_order_list, masking_dict


def reorder_dict_by_keys(original_dict, key_order):
    """Returns a new dictionary with keys reordered according to the specified key_order list."""
    return {key: original_dict[key] for key in key_order if key in original_dict}


def compute_schema_diff(existing_schema, desired_schema):
    """
    Compares an existing table schema against a desired DDL schema and returns a
    dictionary of changes needed (additions, modifications, drops, renames). Changes
    are ordered with renames first, then other operations, with each entry's properties
    sorted by priority: drop, rename, datatype, comment, allow_nulls.

    Args:
        existing_schema (dict): Current table schema from get_existing_table_schema().
        desired_schema (dict): Target schema from parse_ddl_to_schema_dict().

    Returns:
        dict: Mapping of column/table names to their required changes, ordered by priority.
    """
    IGNORE_PROPERTIES = frozenset(['generated_identity'])
    PROPERTY_PRIORITY_ORDER = [ "partition_by", "cluster_by", "drop", "rename", "datatype", "comment", "allow_nulls"]

    diff_dict = {}
    existing_columns = set(existing_schema.keys())
    desired_columns = set(desired_schema.keys())

    # Process entries in desired schema
    for col_name, desired_props in desired_schema.items():
        if col_name not in existing_columns:
            # New column — add all properties
            diff_dict[col_name] = desired_props.copy()
            continue

        changes = {}
        current_props = existing_schema[col_name]
        is_generated = 'generated_identity' in desired_props

        for prop_name, prop_value in desired_props.items():
            if prop_name in IGNORE_PROPERTIES:
                continue

            if is_generated and prop_name == 'allow_nulls':
                continue

            if prop_name not in current_props:
                changes[prop_name] = prop_value
            elif prop_name == 'datatype':
                # Check datatype with alternatives
                equivalent_types = EQUIVALENT_DATA_TYPES.get(prop_value, [])
                if current_props[prop_name] not in equivalent_types and prop_value != current_props[prop_name]:
                    changes[prop_name] = prop_value
            elif prop_value != current_props[prop_name]:
                changes[prop_name] = prop_value

        if changes:
            diff_dict[col_name] = changes

    # Mark columns for dropping
    for col_name in existing_columns - desired_columns:
        diff_dict[col_name] = {"drop": ""}
    

    diff_dict = {col_name: props for col_name, props in diff_dict.items() if props}

    for _col, _val in list(diff_dict.items()):
        if not _val:
            del diff_dict[_col]


    # Reorder: renames first, then others, with consistent property order
    rename_columns = {
        col_name: reorder_dict_by_keys(props, PROPERTY_PRIORITY_ORDER)
        for col_name, props in diff_dict.items() if "rename" in props
    }
    other_columns = {
        col_name: reorder_dict_by_keys(props, PROPERTY_PRIORITY_ORDER)
        for col_name, props in diff_dict.items() if "rename" not in props
    }

    return {**rename_columns, **other_columns}


def create_and_run_masking_functions(full_table_name, masking_dict):
    """
    Creates SQL masking functions for specified columns and applies them to the table.
    Supports inclusion-based (allow listed groups to see real data) and exclusion-based
    (hide data from listed groups) masking strategies.

    If masking_dict is empty (no masking configured), checks whether any masking is
    currently applied to the table and removes it from all masked columns.

    Args:
        full_table_name (str): Fully qualified table name (catalog.schema.table).
        masking_dict (dict): Mapping of column names to their masking config containing
            'inclusion_group' or 'exclusion_group' lists and 'datatype'. Also contains
            'function_saving_catalog_name' and 'function_saving_schema_name' keys.

    Returns:
        None
    """
    spark = get_or_create_spark()

    if not masking_dict:
        # Check if any masking is currently applied and remove it from all masked columns
        catalog_name, schema_name, table_name = full_table_name.split('.')
        masked_columns = (
            spark.table(f"{catalog_name}.information_schema.column_masks")
            .filter(
                (F.col('table_catalog') == catalog_name)
                & (F.col('table_schema') == schema_name)
                & (F.col('table_name') == table_name)
            )
            .select('column_name')
            .collect()
        )
        if masked_columns:
            print(f"Removing masking from all columns in {full_table_name}")
            for row in masked_columns:
                spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN {row.column_name} DROP MASK")
        return None

    SPECIAL_KEYS = ('function_saving_catalog_name', 'function_saving_schema_name')
    table_parts = full_table_name.split('.')
    if masking_dict.get('function_saving_catalog_name') is None:
        masking_dict.pop('function_saving_catalog_name', None)
    if masking_dict.get('function_saving_schema_name') is None:
        masking_dict.pop('function_saving_schema_name', None)
    catalog_name = masking_dict.get('function_saving_catalog_name', table_parts[0])
    schema_name = masking_dict.get('function_saving_schema_name', table_parts[1])

    print("Creating/Altering masking functions")

    queries = []
    alter_queries = []

    for masked, config in masking_dict.items():
        if masked in SPECIAL_KEYS:
            continue

        masked_datatype = "DECIMAL" if config['datatype'].upper().startswith("DECIMAL") else config['datatype'].upper()
        masked_value = MASKED_DEFAULTS.get(masked_datatype, '*******')
        if isinstance(masked_value, str):
            masked_value = f"'{masked_value}'"

        if config.get('inclusion_group') is not None:
            group_str = " OR ".join([f"is_member('{g}')" for g in config['inclusion_group']])
            return_expression = f"CASE WHEN {group_str} THEN val ELSE {masked_value} END"
        elif config.get('exclusion_group') is not None:
            group_str = " OR ".join([f"is_member('{g}')" for g in config['exclusion_group']])
            return_expression = f"CASE WHEN {group_str} THEN {masked_value} ELSE val END"
        else:
            continue

        function_name = f"masking_function_{masked}_{table_parts[1]}_{table_parts[2]}"
        full_function_name = f"{catalog_name}.{schema_name}.{function_name}"

        queries.append(
            f"""CREATE OR REPLACE FUNCTION {full_function_name}(val {config['datatype']}) RETURN {return_expression}"""
        )
        alter_queries.append(
            f"ALTER TABLE {full_table_name} ALTER COLUMN {masked} SET MASK {full_function_name}"
        )

    for q in queries:
        spark.sql(q)
    print(f"Applying masking functions to {full_table_name}")
    for aq in alter_queries:
        spark.sql(aq)

    return None
