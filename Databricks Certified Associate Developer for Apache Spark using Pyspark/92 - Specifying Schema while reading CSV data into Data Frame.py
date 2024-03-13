# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType, TimestampType, StringType

# COMMAND ----------

help(spark.read.schema)

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

spark.read.schema(schema).csv('/FileStore/public/retail_db/orders').show()

# COMMAND ----------

spark.read.csv('/FileStore/public/retail_db/orders', schema=schema).show()

# COMMAND ----------

help(StructField)

# COMMAND ----------

schema = StructType([
    StructField('order_id', IntegerType()),
    StructField('order_date', TimestampType()),
    StructField('order_customer_id', IntegerType()),
    StructField('order_status', StringType())
])

# COMMAND ----------

spark.read.schema(schema).csv('/FileStore/public/retail_db/orders').show()

# COMMAND ----------

spark.read.csv('/FileStore/public/retail_db/orders', schema=schema).show()

# COMMAND ----------

help(IntegerType)

# COMMAND ----------

schema = StructType([
    StructField('order_id', IntegerType(), nullable=False),
    StructField('order_date', TimestampType(), nullable=False),
    StructField('order_customer_id', IntegerType(), nullable=False),
    StructField('order_status', StringType(), nullable=False)
])

# COMMAND ----------

spark.read.schema(schema).csv('/FileStore/public/retail_db/orders').show()

# COMMAND ----------

spark.read.schema(schema).csv('/FileStore/public/retail_db/orders').printSchema()

# COMMAND ----------


