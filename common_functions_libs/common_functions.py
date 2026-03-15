
import delta
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql import SparkSession
import json
import os
import ast
import re

def get_or_create_spark():
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
    except Exception:
        spark = SparkSession.builder.getOrCreate()
    return spark

spark = get_or_create_spark()
