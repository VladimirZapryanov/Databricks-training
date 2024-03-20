# Databricks notebook source
df = spark.read.json('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

df.write

# COMMAND ----------

help(df.write.partitionBy)

# COMMAND ----------

help(df.write.json)

# COMMAND ----------

help(df.write.parquet)

# COMMAND ----------


