# Databricks notebook source
from pyspark.sql.functions import count

# COMMAND ----------

order_items = spark.read.json('/FileStore/public/retail_db_json/order_items')

# COMMAND ----------

order_items.count()

# COMMAND ----------

type(order_items.count())

# COMMAND ----------

order_items.select(count('*')).show()

# COMMAND ----------


