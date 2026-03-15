from metadata_management_features.main import metadata_create_or_alter
from integration_test_features.main import run_integration_tests
from common_functions_libs.common_functions import *


def write_main(final_df, catalog_name, schema_name, table_name, ddl_path, primary_key_list):
    # Create or Alter Metadata for the table
    metadata_create_or_alter(catalog_name, schema_name, table_name, final_df, ddl_path, primary_key_list)
    # Run Intergration Tests
    run_integration_tests(catalog_name, schema_name, table_name, final_df, ddl_path)
    # Write Function
    write_table(catalog_name, schema_name, table_name, final_df, ddl_path)
    return None
    
ddl_path = '/Workspace/Users/deepak.andagunda@gmail.com/frst_ddl.py'
catalog_name = "tinyde"
schema_name = "gold"
table_name1 = "claims_sample_synthetic1"
final_df = spark.table(f"samples.healthverity.claims_sample_synthetic")
final_df = final_df.na.fill(value = 0)
final_df = final_df.na.fill(value = "0").distinct().withColumn('hvid', F.col("hvid").try_cast('int'))
pk_col_list = ['hvid', 'claim_id', 'diagnosis_code', 'prov_rendering_npi', 'prov_referring_npi', 'prov_billing_npi', 'payer_type', 'place_of_service_std_id', 'inst_type_of_bill_std_id', 'inst_admit_type_std_id', 'procedure_code', 'service_line_number', 'line_charge', 'procedure_units_billed', 'line_allowed', 'procedure_modifier_1', 'procedure_modifier_2', 'procedure_modifier_3', 'prov_rendering_std_taxonomy', 'inst_admit_source_std_id', 'patient_zip3', 'inst_drg_std_id']
write_main(final_df, catalog_name, schema_name, table_name1, ddl_path, pk_col_list)
pk_cols = ', '.join(pk_col_list)