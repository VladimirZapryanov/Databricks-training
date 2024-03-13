# Databricks notebook source
import getpass
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import col

# COMMAND ----------

username = getpass.getuser()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders').dtypes

# COMMAND ----------

schema = """
    order_id BIGINT,
    order_date STRING,
    order_customer_id BIGINT,
    order_status STRING
"""

# COMMAND ----------

spark.read.parquet(f'/user/{username}/retail_db_parquet/orders', schema=schema).show()

# COMMAND ----------

spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders').show()

# COMMAND ----------

schema = StructType([
    StructField('order_id', LongType()),
    StructField('order_date', StringType()),
    StructField('order_customer_id', LongType()),
    StructField('order_status', StringType())
])

# COMMAND ----------

orders = spark.read.schema(schema).parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

orders.show(truncate=False)

# COMMAND ----------

orders. \
    withColumn('order_date', col('order_date').cast('timestamp')). \
    dtypes

# COMMAND ----------

orders. \
    withColumn('order_date', col('order_date').cast('timestamp')). \
    show()

# COMMAND ----------


