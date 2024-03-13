# Databricks notebook source
from pyspark.sql.functions import count

# COMMAND ----------

orders = spark.read.json('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

orders.show()

# COMMAND ----------

help(count)

# COMMAND ----------

orders.select(count('*').alias('count')).show()

# COMMAND ----------

orders.groupBy('order_status').agg(count('*')).show()

# COMMAND ----------


