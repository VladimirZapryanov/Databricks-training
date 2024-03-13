# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df. \
    select('id', 'email', 'last_updated_ts'). \
    show(truncate=False)

# COMMAND ----------

users_df. \
    select('id', 'email', 'last_updated_ts'). \
    filter(col('last_updated_ts').between('2021-02-15 00:00:00', '2021-03-15 23:59:59')). \
    show(truncate=False)

# COMMAND ----------

users_df. \
    select('id', 'email', 'last_updated_ts'). \
    filter('last_updated_ts BETWEEN "2021-02-15 00:00:00" AND "2021-03-15 23:59:59"'). \
    show(truncate=False)

# COMMAND ----------

users_df. \
    select('id', 'email', 'amount_paid'). \
    filter(col('amount_paid').between(850, 900)). \
    show(truncate=False)

# COMMAND ----------

users_df. \
    select('id', 'email', 'amount_paid'). \
    filter('amount_paid BETWEEN 850 AND 900'). \
    show(truncate=False)

# COMMAND ----------


