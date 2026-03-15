from common_functions_libs.common_functions import *
import ast
import re

def create_ddl_file(catalog_name, schema_name, table_name, df, ddl_path, primary_key_list):
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    schema = df.schema
    columns = []
    ddl_dict = {}
    ddl_dict[f'{full_table_name}'] = {}

    for field in schema.fields:
        ddl_dict[full_table_name][field.name] = {"datatype": field.dataType.simpleString(),
                                                "comment": "",
                                                "nullable": True}
    ddl_dict[full_table_name][full_table_name] = {'comment': "", "primary_key": primary_key_list}
    # Write the dictionary to the specified path
    
    abs_path = os.path.abspath(ddl_path)
    with open(abs_path, 'w') as f:
        content = json.dumps(ddl_dict, indent=2)
        content = re.sub(r'("nullable":\s*)true', r'\1True', content)
        content = re.sub(r'("nullable":\s*)false', r'\1False', content)
        f.write(f"ddl_dict = {content}")
    return None

def read_ddl_file(ddl_path):
    with open(ddl_path) as f:
            content = f.read().split('=', 1)[1]
            content = re.sub(r'("nullable":\s*)true', r'\1True', content)
            content = re.sub(r'("nullable":\s*)false', r'\1False', content)
            ddl_dict = ast.literal_eval(content)
    return ddl_dict