# Databricks notebook source
help(spark.read.json)

# COMMAND ----------

df = spark.read.json('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.format('json').load('/FileStore/public/retail_db_json/orders')

# COMMAND ----------

display(df)

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------


