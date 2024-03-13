# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./58 - Creating Spark Data Frame for Dropping Columns"

# COMMAND ----------

users_df.drop('first_name', 'last_name').printSchema()

# COMMAND ----------

users_df.drop('first_name', 'last_name').show()

# COMMAND ----------

users_df.drop('user_id', 'first_name', 'last_name').printSchema()

# COMMAND ----------

users_df.drop('user_id', 'first_name', 'last_name').show()

# COMMAND ----------


