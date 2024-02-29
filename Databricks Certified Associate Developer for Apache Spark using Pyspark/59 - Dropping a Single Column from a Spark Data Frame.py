# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./58 - Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

help(users_df.drop)

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

users_df.drop('last_updated_ts').printSchema()

# COMMAND ----------

users_df.drop(users_df['last_updated_ts']).printSchema()

# COMMAND ----------

users_df.drop(col('last_updated_ts')).printSchema()

# COMMAND ----------


