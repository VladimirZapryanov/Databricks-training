# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df. \
    select('id', 'email', 'current_city', 'is_customer'). \
    filter((col('current_city') == '') | (col('is_customer') == False)). \
    show(truncate=False)

# COMMAND ----------

users_df. \
    select('id', 'email', 'current_city', 'is_customer'). \
    filter('current_city == "" OR is_customer == false'). \
    show(truncate=False)

# COMMAND ----------

users_df. \
    select('id', 'email', 'is_customer', 'last_updated_ts'). \
    filter((col('is_customer') == False) | (col('last_updated_ts') < '2021-03-01')). \
    show(truncate=False)

# COMMAND ----------

users_df. \
    select('id', 'email', 'is_customer', 'last_updated_ts'). \
    filter('is_customer == False OR last_updated_ts < "2021-03-01"'). \
    show(truncate=False)

# COMMAND ----------


