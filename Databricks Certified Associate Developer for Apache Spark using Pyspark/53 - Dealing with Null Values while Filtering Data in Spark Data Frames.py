# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df. \
    select('*').\
    filter(col('current_city').isNotNull()). \
    show()

# COMMAND ----------

users_df. \
    select('*').\
    filter('current_city IS NOT NULL'). \
    show()

# COMMAND ----------

users_df. \
    select('*').\
    filter(col('current_city').isNull()). \
    show()

# COMMAND ----------

users_df. \
    select('*').\
    filter('current_city IS NULL'). \
    show()

# COMMAND ----------

users_df. \
    select('*').\
    filter(col('customer_from').isNull()). \
    show()

# COMMAND ----------

users_df. \
    select('*').\
    filter('customer_from IS NULL'). \
    show()

# COMMAND ----------


