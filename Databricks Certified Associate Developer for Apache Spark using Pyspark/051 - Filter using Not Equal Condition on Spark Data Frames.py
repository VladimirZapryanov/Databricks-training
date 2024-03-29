# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter(col('current_city') != 'Dallas'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') != 'Dallas') | (col('current_city').isNull())). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter('current_city != "Dallas" OR current_city IS NULL'). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') != '')). \
    show()

# COMMAND ----------

users_df. \
    select('id', 'current_city'). \
    filter('current_city != ""'). \
    show()

# COMMAND ----------


