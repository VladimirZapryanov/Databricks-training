# Databricks notebook source
import getpass

# COMMAND ----------

help(spark.read.parquet)

# COMMAND ----------

username = getpass.getuser()

# COMMAND ----------

dbutils.fs.ls(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df = spark.read.parquet(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.read.format('parquet').load(f'/user/{username}/retail_db_parquet/orders')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.show()

# COMMAND ----------


