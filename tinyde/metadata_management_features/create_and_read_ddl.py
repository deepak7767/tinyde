from tinyde.common_functions_libs.common_functions import *
import ast
import re


def _json_to_python_booleans(content):
    """Converts JSON boolean literals (true/false) to Python equivalents (True/False)."""
    content = re.sub(r':\s*true', ': True', content)
    content = re.sub(r':\s*false', ': False', content)
    return content


def create_ddl_file(full_table_name, df, ddl_path):
    """
    Generates a DDL dictionary from a Spark DataFrame schema and writes it as a
    Python-formatted file (.py) at the specified path.

    The output file contains a single variable assignment: ``ddl_dict = { ... }``
    with column definitions (datatype, comment, nullable) and table-level
    properties (comment, primary_key).

    Args:
        full_table_name (str): Fully qualified table name (catalog.schema.table).
        df (DataFrame): Spark DataFrame whose schema defines the column structure.
        ddl_path (str): Workspace path where the DDL file will be written.
        primary_key_list (list): List of column names forming the primary key.

    Returns:
        dict: The generated DDL dictionary.
    """
    schema = df.schema
    ddl_dict = {}
    ddl_dict[full_table_name] = {}

    for field in schema.fields:
        ddl_dict[full_table_name][field.name] = {
            "datatype": field.dataType.simpleString(),
            "comment": "",
            "allow_nulls": True
        }

    ddl_dict[full_table_name][full_table_name] = {
        "comment": ""
    }

    # Write the dictionary as a Python file
    abs_path = os.path.abspath(ddl_path)
    with open(abs_path, 'w') as f:
        content = json.dumps(ddl_dict, indent=2)
        content = _json_to_python_booleans(content)
        f.write(f"ddl_dict = {content}")

    return ddl_dict


def read_ddl_file(ddl_path):
    """
    Reads a DDL Python file and parses the ddl_dict variable back into a dictionary.

    Expects the file to contain a single assignment in the form ``ddl_dict = { ... }``.
    Handles both Python (True/False) and JSON (true/false) boolean formats for
    compatibility with manual edits.

    Args:
        ddl_path (str): Workspace path to the DDL file.

    Returns:
        dict: The parsed DDL dictionary.

    Raises:
        ValueError: If the file does not contain a valid 'ddl_dict = ...' assignment.
    """
    with open(ddl_path) as f:
        raw = f.read()

    if '=' not in raw:
        raise ValueError(f"DDL file at {ddl_path} does not contain a valid 'ddl_dict = ...' assignment.")

    content = raw.split('=', 1)[1].strip()
    content = _json_to_python_booleans(content)
    ddl_dict = ast.literal_eval(content)

    return ddl_dict
