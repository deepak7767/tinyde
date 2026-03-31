from tinyde.common_functions_libs.common_functions import *


def table_create(table_dict, full_table_name):
    """
    Creates a new Delta table using the DeltaTableBuilder API based on the provided
    table dictionary. Handles standard columns, identity columns (ALWAYS / BY DEFAULT),
    partitioning, and liquid clustering.

    Note: Delta tables do not support both PARTITIONED BY and CLUSTER BY simultaneously.
    If both are specified, a ValueError is raised.

    Args:
        table_dict (dict): Top-level dictionary keyed by fully qualified table name,
            containing column definitions and table-level properties.
        full_table_name (str): Fully qualified table name (catalog.schema.table).

    Returns:
        None
    """
    table_dict_fin = table_dict[full_table_name]
    table_props = table_dict_fin.get(full_table_name, {})

    # Build table with DeltaTableBuilder API
    ddl = DeltaTable.create().tableName(full_table_name)

    # Set table-level comment
    table_comment = table_props.get("comment", "")
    if table_comment:
        ddl = ddl.property("comment", table_comment)

    # Add columns
    for col, props in table_dict_fin.items():
        if col == full_table_name:
            continue

        identity_type = props.get("generated_identity", "").upper()
        col_comment = props.get("comment", "")

        if identity_type == "ALWAYS":
            ddl = ddl.addColumn(
                col,
                dataType=props["datatype"],
                comment=col_comment,
                generatedAlwaysAs=IdentityGenerator()
            )
        elif identity_type == "BY DEFAULT":
            ddl = ddl.addColumn(
                col,
                dataType=props["datatype"],
                comment=col_comment,
                generatedByDefaultAs=IdentityGenerator(start=-1, step=1)
            )
        else:
            ddl = ddl.addColumn(
                col,
                dataType=props["datatype"],
                comment=col_comment,
                nullable=props.get("allow_nulls", True)
            )

    # Apply partitioning and/or clustering
    partition_cols = table_props.get("partition_by", [])
    cluster_cols = table_props.get("cluster_by", [])

    if partition_cols and cluster_cols:
        raise ValueError(
            f"Table {full_table_name}: cannot specify both 'partition_by' and 'cluster_by'. "
            f"Delta tables support only one at a time."
        )

    if partition_cols:
        ddl = ddl.partitionedBy(*partition_cols)

    if cluster_cols:
        ddl = ddl.clusterBy(*cluster_cols)

    ddl.execute()
    return None
