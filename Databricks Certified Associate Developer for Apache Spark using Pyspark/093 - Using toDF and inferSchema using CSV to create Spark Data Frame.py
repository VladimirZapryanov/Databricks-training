# Databricks notebook source
columns = ['order_id', 'order_date', 'order_customer_id', 'order_status']

# COMMAND ----------

spark.read.option('inferSchema', True).csv('/FileStore/public/retail_db/orders').dtypes

# COMMAND ----------

spark.read.option('inferSchema', True).csv('/FileStore/public/retail_db/orders').toDF(*columns)

# COMMAND ----------

spark.read.csv('/FileStore/public/retail_db/orders', inferSchema=True).toDF(*columns)

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------


