# Databricks notebook source
from pyspark.sql.functions import col, isnan

# COMMAND ----------

# MAGIC %run "./47 - Creating Spark Data Frame for Filtering"

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('amount_paid') > 900) & (isnan(col('amount_paid')) == False)). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('amount_paid > 900 AND isnan(amount_paid) = false'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('amount_paid') < 900) & (isnan(col('amount_paid')) == False)). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('amount_paid < 900 AND isnan(amount_paid) = false'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('amount_paid') >= 900) & (isnan(col('amount_paid')) == False)). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('amount_paid >= 900 AND isnan(amount_paid) = false'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter((col('amount_paid') <= 900) & (isnan(col('amount_paid')) == False)). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('amount_paid <= 900 AND isnan(amount_paid) = false'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter(col('customer_from') > '2021-01-21'). \
    show()

# COMMAND ----------

users_df. \
    select('*'). \
    filter('customer_from > "2021-01-21"'). \
    show()

# COMMAND ----------


