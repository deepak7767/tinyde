
import delta
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql import SparkSession
import json
import os
import ast
import re
import multiprocessing
import statistics
import builtins

def get_or_create_spark():
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
    except Exception:
        spark = SparkSession.builder.getOrCreate()
    return spark

# Deferred initialisation — avoids creating a SparkSession at import time
# (e.g. during pip install or documentation builds).  Code that needs the
# session should call get_or_create_spark() directly.
try:
    spark = get_or_create_spark()
except Exception:
    spark = None
