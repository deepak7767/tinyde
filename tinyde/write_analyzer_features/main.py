from .executor_details import get_executor_details
from .data_characteristics import get_data_characteristics
from .recommendations import *

def write_analyzer(df, partitions, write_mode):
    write_analyzer_dict = {}
    write_analyzer_dict.update(get_executor_details())
    write_analyzer_dict.update(get_data_characteristics(df, partitions))
    write_analyzer_dict.update(analyze_memory_sufficiency(write_analyzer_dict))
    estimate_write_time_dict = estimate_write_time(write_analyzer_dict, write_mode)
    write_analyzer_dict.update(estimate_write_time_dict)
    return write_analyzer_dict
