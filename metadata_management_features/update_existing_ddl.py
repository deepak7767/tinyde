from common_functions_libs.common_functions import *

alter_column_statements = {"datatype": "",
                           "comment": "COMMENT '{details}'",
                           "rename": "TO {details}",
                           "drop": "",
                           "nullable": "{details} NOT NULL"}

alter_table_statements = {"tags": "SET TAGS {details}",
                          "unset_tags": "UNSET TAGS ('{details}')",
                          "rename": "RENAME TO {details}",
                          "comment": "SET TBLPROPERTIES ('comment' = '{details}')",
                          "column_comments": ""}

common_types = {"bigint": ["long"],
                "long": ["bigint"],
                "int": ["integer"],
                "integer": ["int"]}


def alter_column_case(table_name, col_name, col_case):
    """
    Alters the datatype of a specified column in a given table by creating a temporary column,
    copying data from the original column, dropping the original column, and renaming the temporary column.

    Returns:
    None
    """
    print(f"Changing column name case for {col_name} to {col_case}")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} DROP MASK")
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {col_name} to {col_name}_temp")
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {col_name}_temp TO {col_case}")
    return None

def alter_datatype(catalog_name, schema_name, table_name, col_name, data_type, drop_and_create_flag, nullable_flag):
    """
    Alters the datatype of a specified column in a given table by creating a temporary column,
    copying data from the original column, dropping the original column, and renaming the temporary column.

    Returns:
        None
    """

    print(f"Altering datatype of {col_name} to {data_type}")

    spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} DROP COLUMN IF EXISTS {col_name}_temp")
    spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} ADD COLUMN {col_name}_temp {data_type}")

    comment_df = spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.{table_name}")

    # Extract the comment for the source column
    source_comment = None
    for row in comment_df.collect():
        if row['col_name'] == col_name:
            source_comment = row['comment'] if row['comment'] is not None else ""
            break

    spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} ALTER COLUMN {col_name}_temp COMMENT '{source_comment}'")
    if not drop_and_create_flag:
        spark.sql(f"UPDATE {catalog_name}.{schema_name}.{table_name} SET {col_name}_temp = {col_name}")
    if drop_and_create_flag and nullable_flag:
        col_value = "NA" if column_details["datatype"].lower() == "string" else "9999-12-31" if column_details["datatype"].lower() in ("date", "timestamp") else True if 'bool' in column_details["datatype"].lower() else 9999999
        spark.sql(f"UPDATE {catalog_name}.{schema_name}.{table_name} SET {col_name}_temp = col_value")
    spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} ALTER COLUMN {col_name} DROP MASK")
    spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} DROP COLUMN {col_name}")
    spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} ALTER COLUMN {col_name}_temp DROP MASK")
    spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} RENAME COLUMN {col_name}_temp TO {col_name}")

    return None

def alter_column_queries(add_or_alter, alter_type, alter_statements, column_details, alter_queries, query, col_name):
    """
    Constructs SQL queries for altering columns in a specified table based on the provided parameters.

    Returns:
    - tuple and string: The modified query string and the list of alter queries.
    """
    if alter_type not in column_details.keys():
        print(f"Alter type not present currently: {alter_type}")
        return query, alter_queries

    # Handle special cases for drop and rename
    query = query.replace("ALTER COLUMN", "DROP COLUMN") if alter_type == "drop" else query
    query = query.replace("ALTER COLUMN", "RENAME COLUMN") if alter_type == "rename" else query

    # Handle nullable toggle
    if alter_type == "nullable":
        column_details[alter_type] = "SET" if column_details[alter_type] is False else "DROP"

    # Format the extended query
    extended_query = " " + alter_statements[alter_type].format(details=column_details[alter_type])

    # Construct final query based on operation type and alteration
    if add_or_alter.startswith("ADD"):
        if alter_type == "nullable":
            if column_details[alter_type] == "SET":
                col_value = "NA" if column_details["datatype"].lower() == "string" else "9999-12-31" if column_details["datatype"].lower() in ("date", "timestamp") else True if 'bool' in column_details["datatype"].lower() else 9999999

                update_query = f"UPDATE " +query.split()[2]+ f" SET {col_name} = '{col_value}'"
                alter_queries.append(update_query)

            alter_query = " ".join(query.split()[:3]) + f" ALTER COLUMN {col_name}" + extended_query
            alter_queries.append(alter_query)

        elif alter_type == "datatype":
            query += extended_query

    elif add_or_alter.startswith("ALTER") and alter_type == "rename":
        if column_details[alter_type].lower() == col_name.lower():
            alter_column_case(query.split()[2], col_name, column_details[alter_type])
        else:
            alter_query = query + extended_query
            alter_queries.append(alter_query)

    elif add_or_alter.startswith("ALTER") and alter_type not in ("datatype"):
        alter_query = query + extended_query
        alter_queries.append(alter_query)
    return query, alter_queries

def enable_column_mapping(catalog_name, schema_name, table_name):
    """
    Enables column mapping for a specified table by setting the relevant table property.

    Returns:
    None
    """

    spark.sql(
        f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} "
        f"SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    return None


def alter_table_queries(table_dict, alter_type, alter_table_statements, table_queries, query, catalog_name, schema_name):
    """
    Constructs SQL queries for altering table properties based on the specified alteration type.

    Returns:
    - list: The updated list of alter table queries.
    """

    table_dict[alter_type] = (
        f"{catalog_name}.{schema_name}." + table_dict[alter_type]
        if alter_type.lower() == "rename"
        else table_dict[alter_type]
    )

    if alter_type in table_dict.keys():
        query = query + alter_table_statements[alter_type].format(details=table_dict[alter_type])

    table_queries.append(query)

    return table_queries


def reorder_queries(col_reorder, existing_order, full_table_name):
    """
    Reorders columns in a specified table according to the provided new order.

    Returns:
    None
    """
    for _col in range(len(col_reorder)):
        if existing_order[_col] == col_reorder[_col]:
            continue
        index_preceeding_col = _col - 1
        reorder_query = f"ALTER TABLE {full_table_name} ALTER COLUMN {col_reorder[_col]} "
        query_run = "FIRST" if index_preceeding_col == -1 else f"AFTER {col_reorder[index_preceeding_col]}"
        existing_order = list(spark.table(full_table_name).columns)
        spark.sql(reorder_query + query_run)
    return None

def alter_table_column_commands(catalog_name, schema_name, table_name, table_dict, col_reorder):
    """
    Generates and executes SQL commands to alter columns in a specified table based on the
    provided table dictionary and reordering list. It handles adding, altering, renaming,
    and dropping columns as needed. It also alters table level properties.

    Returns:
    None
    """

    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

    for col_name, col_details in table_dict.items():

        if full_table_name == col_name:
            continue

        alter_queries = []

        table_columns = spark.table(f"{catalog_name}.{schema_name}.{table_name}").columns

        add_or_alter = f"ALTER COLUMN {col_name}" if col_name in table_columns else f"ADD COLUMN {col_name} {col_details['datatype']}"

        query = f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} {add_or_alter}"

        for col_alter_type in col_details:

            if col_alter_type in ("rename", "drop", "datatype") and add_or_alter.startswith("ALTER"):
                enable_column_mapping(catalog_name, schema_name, table_name)

            if col_alter_type == "datatype" and add_or_alter.startswith("ALTER"):
                alter_datatype(catalog_name, schema_name, table_name, col_name, col_details["datatype"], col_details.get('drop_and_create', False), col_details.get('nullable', False))
                continue

            query, alter_queries = alter_column_queries(add_or_alter,col_alter_type,alter_column_statements,col_details,alter_queries,query,col_name)

        if add_or_alter.startswith("ADD"):
            print("Executing:", query)
            spark.sql(query)

        for alter_q in alter_queries:

            if "rename column" in alter_q.lower() or "drop column" in alter_q.lower():
                print("Executing:", alter_q)
                spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} ALTER COLUMN {col_name} DROP MASK")
                spark.sql(alter_q)

            else:
                print("Executing:", alter_q)
                spark.sql(alter_q)

    # Run all reorder queries
    if full_table_name in col_reorder:
        col_reorder.remove(full_table_name)
    existing_order = list(spark.table(full_table_name).columns)
    if col_reorder != existing_order:
        print("running reorder queries")
        reorder_queries(col_reorder, existing_order, full_table_name)

    if full_table_name in table_dict.keys():

        table_queries = []
        rename = ""

        query = f"ALTER TABLE {catalog_name}.{schema_name}.{table_name} "

        for col_alter_type in table_dict[full_table_name]:
            table_queries = alter_table_queries(table_dict[full_table_name], col_alter_type, alter_table_statements, table_queries, query, catalog_name, schema_name)

        for run_query in table_queries:

            if "RENAME TO" in run_query:
                rename = run_query
                continue

            print("Executing:", run_query)
            spark.sql(run_query)

        if rename != '':
            spark.sql(rename)

    if len(table_dict) == 0 and col_reorder == existing_order:
        print(f"No changes detected between existing table and DDL for {catalog_name}.{schema_name}.{table_name}")
    else:
        print(f"All the changes were implemented for the table {catalog_name}.{schema_name}.{table_name}")


def existing_table_to_dict(catalog_name, schema_name, table_name):
    """
    Retrieves the existing table schema and details from a specified catalog and schema
    and converts it into a dictionary format.

    Returns:
        - dict: A dictionary containing column names as keys and their data types and comments as values.
                Also includes the table comment.
    """
    table = spark.sql(f"DESCRIBE TABLE {catalog_name}.{schema_name}.{table_name}")
    tbl_comments = spark.sql(f"DESCRIBE DETAIL {catalog_name}.{schema_name}.{table_name}")
    tbl_comments = '' if tbl_comments.collect()[0].description is None else tbl_comments.collect()[0].description
    null_types = spark.table(f"{catalog_name}.information_schema.columns").select('column_name', 'is_nullable') \
        .withColumnRenamed('column_name', 'col_name') \
        .filter((F.col('table_catalog') == catalog_name) & (F.col('table_schema') == schema_name) & (
                F.col('table_name') == table_name))

    table = table.join(null_types, ['col_name'], 'left')
    existing_table_dict = {}

    for row in table.collect():
        comment = '' if row.comment is None else row.comment
        is_nullable = '' if row.is_nullable is None else False if row.is_nullable.upper() == "NO" else True
        existing_table_dict[row.col_name] = {"datatype": row.data_type, "comment": comment, "nullable": is_nullable}

    existing_table_dict[f"{catalog_name}.{schema_name}.{table_name}"] = {"comment": tbl_comments}
    return existing_table_dict


def ddl_to_dict(ddl_dict, catalog_name, schema_name, table_name_og):
    """
    Converts a Data Definition Language (DDL) dictionary to a modified format
    by renaming columns based on the provided table name.

    Returns:
        - dict: A dictionary containing DDL details for the specified table.
    """
    col_reorder = []
    table_name = f"{catalog_name}.{schema_name}.{table_name_og}"
    col_reorder_List = list(ddl_dict[table_name].keys())
    for _col in list(ddl_dict[table_name].keys()):
        if 'old_column_name' in ddl_dict[table_name][_col].keys():
            ddl_dict[table_name][ddl_dict[table_name][_col]['old_column_name']] = {'rename': _col}
            del ddl_dict[table_name][_col]['old_column_name']

    ddl_dict_fin = ddl_dict[table_name].copy()
    for _col, _col_detail_types in ddl_dict_fin.items():
        for _col_details in list(_col_detail_types.keys()):
            if _col_details in ('min_max_threshold', 'null_threshold', 'minimum_row_count', 'primary_key'):
                del ddl_dict_fin[_col][_col_details]
                continue

            if _col_details not in ('nullable', 'comment', 'rename', 'drop_and_create'):
                ddl_dict_fin[_col][_col_details] = _col_detail_types[_col_details].lower()

        if 'nullable' not in list(_col_detail_types.keys()) and _col != table_name and (len(_col_detail_types)==1 and list(_col_detail_types.keys())[0] != 'rename'):
            ddl_dict_fin[_col]['nullable'] = True

    return ddl_dict_fin, col_reorder_List

def reorder_dict(original_dict, key_order):
    return {key: original_dict[key] for key in key_order if key in original_dict}

def alter_dict_diff(existing_table_dict, ddl_dict):
    """
    Compares the existing table dictionary with a DDL dictionary to determine
    differences and necessary alterations, including additions, modifications,
    and deletions of columns.

    Returns:
        - dict: A dictionary of changes to be made (additions, modifications, drops)
                with keys ordered by priority (drop, rename, datatype, comment, nullable).
    """
    IGNORE_PROPERTIES = frozenset(['generated_identity'])
    KEY_ORDER = ["drop", "rename", "datatype", "comment", "nullable"]

    final_dict = {}
    existing_keys = set(existing_table_dict.keys())
    ddl_keys = set(ddl_dict.keys())

    # Process columns in DDL
    for col, col_props in ddl_dict.items():
        if col not in existing_keys:
            # New column - add all properties
            final_dict[col] = col_props.copy()
            continue

        changes = {}
        existing_props = existing_table_dict[col]
        is_generated = 'generated_identity' in col_props

        for prop_type, prop_value in col_props.items():
            if prop_type in IGNORE_PROPERTIES:
                continue

            if is_generated and prop_type == 'nullable':
                continue

            if prop_type not in existing_props:
                changes[prop_type] = prop_value
            elif prop_type == 'datatype':
                # Check datatype with alternatives
                alt_names = common_types.get(prop_value, [])
                if existing_props[prop_type] not in alt_names and prop_value != existing_props[prop_type]:
                    changes[prop_type] = prop_value
            elif prop_value != existing_props[prop_type]:
                changes[prop_type] = prop_value

        if changes:
            final_dict[col] = changes

    # Mark columns for dropping
    for col in existing_keys - ddl_keys:
        final_dict[col] = {"drop": ""}
    # Process renames and drop&recreate
    cols_to_remove = set()
    for col in list(final_dict.keys()):
        if 'rename' not in final_dict[col]:
            if "drop_and_create" in final_dict[col] and "datatype" not in final_dict[col]:
                cols_to_remove.add(col)
            continue

        rename_target = final_dict[col]['rename']

        # Remove already completed renames
        if rename_target in existing_keys:
            del final_dict[col]['rename']
            if not final_dict[col]:
                cols_to_remove.add(col)
            continue
        

        # Consolidate changes from renamed column
        if rename_target in final_dict and col in existing_keys:
            existing_props = existing_table_dict[col]
            for prop_type in list(final_dict[rename_target].keys()):
                if prop_type in IGNORE_PROPERTIES:
                    continue
                if prop_type in existing_props and existing_props[prop_type] == final_dict[rename_target][prop_type]:
                    del final_dict[rename_target][prop_type]
    
    # Remove empty entries and consolidated renamed columns
    for col in cols_to_remove:
        final_dict.pop(col, None)

    final_dict = {col: props for col, props in final_dict.items() if props}
    # Reorder: renames first, then others, with consistent property order
    rename_cols = {col: reorder_dict(props, KEY_ORDER) for col, props in final_dict.items() if "rename" in props}
    other_cols = {col: reorder_dict(props, KEY_ORDER) for col, props in final_dict.items() if "rename" not in props}

    return {**rename_cols, **other_cols}







