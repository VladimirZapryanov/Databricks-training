# Databricks notebook source
orders = spark.read.csv('/FileStore/public/retail_db/orders')

# COMMAND ----------

orders.columns

# COMMAND ----------

orders.dtypes

# COMMAND ----------


