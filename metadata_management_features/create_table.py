from common_functions_libs.common_functions import *

def table_create(table_dict, catalog_name, schema_name, table_name):
    """
    Creates a new table in the specified catalog and schema based on the provided table dictionary.
    It handles both identity and non-identity columns using appropriate SQL commands or DeltaTableBuilder API.

    Returns:
        None
    """
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    table_dict_fin = table_dict[full_table_name]

    # Check if any column uses identity (by checking if value is "ALWAYS" or "BY DEFAULT")
    has_identity_column = any(
        table_dict_fin[col].get("generated_identity", "").upper() in {"ALWAYS", "BY DEFAULT"}
        for col in table_dict_fin if col != full_table_name
    )

    # Use DeltaTableBuilder API (if no identity column)
    ddl = DeltaTable.create().tableName(full_table_name)
    table_comment = table_dict_fin.get(full_table_name, {}).get("comment", "")
    ddl = ddl.property("comment", table_comment)
    for col, props in table_dict_fin.items():
        if col == full_table_name:
            continue
        if "generated_identity" in props and props['generated_identity'] == "ALWAYS":
            ddl = ddl.addColumn(col, dataType=props["datatype"], generatedAlwaysAs=IdentityGenerator())
        elif "generated_identity" in props and props['generated_identity'] == "BY DEFAULT":
            ddl = ddl.addColumn(col, dataType=props["datatype"], generatedByDefaultAs=IdentityGenerator(start=-1, step=1))
        else:
            ddl = ddl.addColumn(
                col,
                dataType=props["datatype"],
                comment=props.get("comment", ""),
                nullable=props.get("nullable", True)
            )
    ddl.execute()
    return None