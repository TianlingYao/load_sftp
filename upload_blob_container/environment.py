from pyspark.sql import SparkSession
import sys
from datetime import datetime
import pytz
from functools import partial

# gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder.
spark = SparkSession.builder.getOrCreate()

# checking cluster modes to provide resources to spark applications
is_local = True if "local" in spark.conf.get("spark.master") else False


def get_dbutils(spark):
    # To initialize the databricks utility , where spark is spark session
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils
